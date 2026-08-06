package nkv_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
)

func TestWatchInitialReplayAndLiveUpdates(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_REPLAY"})
	is.NoErr(err) // bucket creation should succeed

	for key, value := range map[string]string{"alpha": "one", "beta": "two"} {
		_, err := kv.Put(t.Context(), key, []byte(value))
		is.NoErr(err) // replay fixture put should succeed
	}

	watcher, err := kv.WatchAll(t.Context(), nkv.WithPullBatch(1))
	is.NoErr(err) // watch creation should succeed
	t.Cleanup(watcher.Stop)
	is.True(!watcher.InitialDone()) // initial replay should start incomplete

	replayed := make(map[string]string)
	deltas := make([]uint64, 0, 2)
	for !watcher.InitialDone() {
		entry, err := nextWatch(t, watcher)
		is.NoErr(err)         // initial replay should not fail
		is.True(entry != nil) // initial replay should return an entry before its sentinel
		replayed[entry.Key] = string(entry.Value)
		deltas = append(deltas, entry.Delta)
	}
	is.Equal(replayed, map[string]string{"alpha": "one", "beta": "two"}) // replay should contain each current key
	is.Equal(deltas, []uint64{1, 0})                                     // replay delta should count entries still pending

	entry, err := nextWatch(t, watcher)
	is.NoErr(err)         // initial replay sentinel should not return an error
	is.True(entry == nil) // initial replay should end with a nil sentinel

	revision, err := kv.Put(t.Context(), "gamma", []byte("three"))
	is.NoErr(err) // live update put should succeed
	entry, err = nextWatch(t, watcher)
	is.NoErr(err)                          // live update delivery should not fail
	is.Equal(entry.Key, "gamma")           // watcher should deliver the live key
	is.Equal(string(entry.Value), "three") // watcher should deliver the live value
	is.Equal(entry.Revision, revision)     // watcher should deliver the live revision
	is.Equal(entry.Delta, uint64(0))       // live update should have no pending entries
}

func TestWatchEmptyBucketReplay(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_EMPTY"})
	is.NoErr(err) // bucket creation should succeed

	watcher, err := kv.WatchAll(t.Context())
	is.NoErr(err) // watch creation should succeed
	t.Cleanup(watcher.Stop)

	entry, err := nextWatch(t, watcher)
	is.NoErr(err)                  // empty replay sentinel should not return an error
	is.True(entry == nil)          // empty bucket should immediately return a nil sentinel
	is.True(watcher.InitialDone()) // empty bucket replay should be complete
}

func TestWatchUpdatesOnly(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_UPDATES"})
	is.NoErr(err) // bucket creation should succeed
	_, err = kv.Put(t.Context(), "existing", []byte("before"))
	is.NoErr(err) // existing fixture put should succeed

	watcher, err := kv.WatchAll(t.Context(), nkv.WithUpdatesOnly())
	is.NoErr(err) // updates-only watch creation should succeed
	t.Cleanup(watcher.Stop)
	is.True(watcher.InitialDone()) // updates-only watch should skip initial replay

	revision, err := kv.Put(t.Context(), "existing", []byte("after"))
	is.NoErr(err) // live update put should succeed
	entry, err := nextWatch(t, watcher)
	is.NoErr(err)                          // updates-only delivery should not fail
	is.Equal(entry.Key, "existing")        // updates-only watch should deliver the updated key
	is.Equal(string(entry.Value), "after") // updates-only watch should skip the old value
	is.Equal(entry.Revision, revision)     // updates-only watch should deliver the new revision
}

func TestWatchFromRevision(t *testing.T) {
	type wantEntry struct {
		key      string
		revision uint64
	}

	tests := []struct {
		name            string
		bucket          string
		pattern         string
		options         []nkv.WatchOption
		want            []wantEntry
		wantInitialDone bool
		wantSentinel    bool
	}{
		{
			name:    "inclusive revision",
			bucket:  "WATCH_REVISION",
			pattern: ">",
			options: []nkv.WatchOption{nkv.WithRevision(2)},
			want: []wantEntry{
				{key: "users.alice", revision: 2},
				{key: "ignored.middle", revision: 3},
				{key: "config.theme", revision: 4},
			},
			wantSentinel: true,
		},
		{
			name:    "filtered global revision",
			bucket:  "WATCH_REVISION_FILTERED",
			pattern: "users.>",
			options: []nkv.WatchOption{nkv.WithRevision(2), nkv.WithAdditionalKeys("config.>")},
			want: []wantEntry{
				{key: "users.alice", revision: 2},
				{key: "config.theme", revision: 4},
			},
			wantSentinel: true,
		},
		{
			name:    "zero keeps default replay",
			bucket:  "WATCH_REVISION_ZERO",
			pattern: ">",
			options: []nkv.WatchOption{nkv.WithRevision(0)},
			want: []wantEntry{
				{key: "ignored.before", revision: 1},
				{key: "users.alice", revision: 2},
				{key: "ignored.middle", revision: 3},
				{key: "config.theme", revision: 4},
			},
			wantSentinel: true,
		},
		{
			name:    "updates only resumes without sentinel",
			bucket:  "WATCH_REVISION_UPDATES",
			pattern: ">",
			options: []nkv.WatchOption{nkv.WithUpdatesOnly(), nkv.WithRevision(2)},
			want: []wantEntry{
				{key: "users.alice", revision: 2},
				{key: "ignored.middle", revision: 3},
				{key: "config.theme", revision: 4},
			},
			wantInitialDone: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed

			fixtures := []struct {
				key   string
				value string
			}{
				{key: "ignored.before", value: "one"},
				{key: "users.alice", value: "two"},
				{key: "ignored.middle", value: "three"},
				{key: "config.theme", value: "four"},
			}
			for _, fixture := range fixtures {
				_, err := kv.Put(t.Context(), fixture.key, []byte(fixture.value))
				is.NoErr(err) // revision fixture put should succeed
			}

			watcher, err := kv.Watch(t.Context(), test.pattern, test.options...)
			is.NoErr(err) // revision watch creation should succeed
			t.Cleanup(watcher.Stop)
			is.Equal(watcher.InitialDone(), test.wantInitialDone) // initial state should match the selected watch mode

			for _, want := range test.want {
				entry, err := nextWatch(t, watcher)
				is.NoErr(err)                           // revision replay should not fail
				is.Equal(entry.Key, want.key)           // revision replay should respect stream order and filters
				is.Equal(entry.Revision, want.revision) // revision replay should start inclusively
			}

			if test.wantSentinel {
				entry, err := nextWatch(t, watcher)
				is.NoErr(err)                  // revision replay sentinel should not fail
				is.True(entry == nil)          // revision replay should end with a nil sentinel
				is.True(watcher.InitialDone()) // revision replay should be complete after the sentinel
			}
		})
	}
}

func TestWatchFilters(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_FILTERS"})
	is.NoErr(err) // bucket creation should succeed

	values := []struct {
		key   string
		value string
	}{
		{key: "config.theme", value: "dark"},
		{key: "users.alice", value: "alice"},
		{key: "ignored.key", value: "ignored"},
	}
	for _, value := range values {
		_, err := kv.Put(t.Context(), value.key, []byte(value.value))
		is.NoErr(err) // filter fixture put should succeed
	}

	watcher, err := kv.Watch(t.Context(), "users.>", nkv.WithAdditionalKeys("config.>"), nkv.WithPullBatch(1))
	is.NoErr(err) // multi-filter watch creation should succeed
	t.Cleanup(watcher.Stop)

	status, err := kv.Status(t.Context())
	is.NoErr(err)                       // bucket status should be available
	is.Equal(status.State.Consumers, 1) // multiple filters should share one consumer

	replayed := make(map[string]string)
	for !watcher.InitialDone() {
		entry, err := nextWatch(t, watcher)
		is.NoErr(err) // filtered replay should not fail
		replayed[entry.Key] = string(entry.Value)
	}
	is.Equal(replayed, map[string]string{"config.theme": "dark", "users.alice": "alice"}) // replay should include only matching filters
	entry, err := nextWatch(t, watcher)
	is.NoErr(err)         // filtered replay sentinel should not fail
	is.True(entry == nil) // filtered replay should end with a nil sentinel

	_, err = kv.Put(t.Context(), "ignored.live", []byte("ignored"))
	is.NoErr(err) // ignored live put should succeed
	revision, err := kv.Put(t.Context(), "config.live", []byte("matched"))
	is.NoErr(err) // matched live put should succeed
	entry, err = nextWatch(t, watcher)
	is.NoErr(err)                      // filtered live delivery should not fail
	is.Equal(entry.Key, "config.live") // watcher should skip nonmatching live updates
	is.Equal(entry.Revision, revision) // watcher should preserve the matched revision
}

func TestWatchOperations(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_OPERATIONS"})
	is.NoErr(err) // bucket creation should succeed

	watcher, err := kv.WatchAll(t.Context(), nkv.WithUpdatesOnly(), nkv.WithPullBatch(1))
	is.NoErr(err) // updates-only watch creation should succeed
	t.Cleanup(watcher.Stop)

	tests := []struct {
		name          string
		run           func(context.Context) (uint64, error)
		wantOperation nkv.Operation
		wantValue     string
	}{
		{name: "put", run: func(ctx context.Context) (uint64, error) {
			return kv.Put(ctx, "key", []byte("put"))
		}, wantOperation: nkv.OpPut, wantValue: "put"},
		{name: "delete", run: func(ctx context.Context) (uint64, error) {
			if err := kv.Delete(ctx, "key"); err != nil {
				return 0, err
			}
			return 2, nil
		}, wantOperation: nkv.OpDelete},
		{name: "create", run: func(ctx context.Context) (uint64, error) {
			return kv.Create(ctx, "key", []byte("create"))
		}, wantOperation: nkv.OpPut, wantValue: "create"},
		{name: "purge", run: func(ctx context.Context) (uint64, error) {
			if err := kv.Purge(ctx, "key"); err != nil {
				return 0, err
			}
			return 4, nil
		}, wantOperation: nkv.OpPurge},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			revision, err := test.run(t.Context())
			is.NoErr(err) // watched operation should succeed
			entry, err := nextWatch(t, watcher)
			is.NoErr(err)                                 // watched operation delivery should not fail
			is.Equal(entry.Key, "key")                    // watched operation should retain the key
			is.Equal(entry.Operation, test.wantOperation) // watcher should decode the KV operation
			is.Equal(string(entry.Value), test.wantValue) // watcher should deliver the expected operation value
			is.Equal(entry.Revision, revision)            // watcher should deliver the operation revision
		})
	}
}

func TestWatchMetaOnly(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_META"})
	is.NoErr(err) // bucket creation should succeed

	watcher, err := kv.WatchAll(t.Context(),
		nkv.WithUpdatesOnly(),
		nkv.WithMetaOnly(),
		nkv.WithPullBatch(1),
		nkv.WithInactiveThreshold(time.Minute),
	)
	is.NoErr(err) // metadata-only watch creation should succeed
	t.Cleanup(watcher.Stop)

	revision, err := kv.Put(t.Context(), "key", []byte("hidden"), nkv.WithHeaders(nats.Header{"Content-Type": []string{"text/plain"}}))
	is.NoErr(err) // metadata-only fixture put should succeed
	entry, err := nextWatch(t, watcher)
	is.NoErr(err)                             // metadata-only delivery should not fail
	is.Equal(entry.Key, "key")                // metadata-only delivery should retain the key
	is.Equal(len(entry.Value), 0)             // metadata-only delivery should omit the value
	is.Equal(entry.Revision, revision)        // metadata-only delivery should retain the revision
	is.Equal(entry.ContentType, "text/plain") // metadata-only delivery should retain headers
}

func TestWatchStopAndCancellation(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		run    func(context.Context, context.CancelFunc, *nkv.Watcher)
	}{
		{name: "stop", bucket: "WATCH_STOP", run: func(_ context.Context, _ context.CancelFunc, watcher *nkv.Watcher) {
			watcher.Stop()
		}},
		{name: "cancel context", bucket: "WATCH_CANCEL", run: func(_ context.Context, cancel context.CancelFunc, _ *nkv.Watcher) {
			cancel()
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			watcher, err := kv.WatchAll(ctx, nkv.WithUpdatesOnly())
			is.NoErr(err) // updates-only watch creation should succeed
			test.run(ctx, cancel, watcher)
			_, err = nextWatch(t, watcher)
			is.True(err != nil) // stopped or canceled watcher should unblock with an error
		})
	}
}

func TestWatchUpdatesChannel(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_CHANNEL"})
	is.NoErr(err) // bucket creation should succeed

	watcher, err := kv.WatchAll(t.Context())
	is.NoErr(err) // watch creation should succeed
	updates := watcher.Updates()

	select {
	case entry, open := <-updates:
		is.True(open)         // updates channel should remain open for live updates
		is.True(entry == nil) // updates channel should deliver the initial replay sentinel
	case <-time.After(2 * time.Second):
		is.True(false) // updates channel should promptly deliver the replay sentinel
	}

	watcher.Stop()
	select {
	case _, open := <-updates:
		is.True(!open) // updates channel should close after the watcher stops
	case <-time.After(2 * time.Second):
		is.True(false) // updates channel should promptly close after stop
	}
}

func TestWatchUpdatesChannelStopsWhenFull(t *testing.T) {
	tests := []struct {
		name     string
		bucket   string
		shutdown func(context.CancelFunc, *nkv.Watcher)
	}{
		{name: "stop", bucket: "WATCH_CHANNEL_FULL_STOP", shutdown: func(_ context.CancelFunc, watcher *nkv.Watcher) {
			watcher.Stop()
		}},
		{name: "cancel context", bucket: "WATCH_CHANNEL_FULL_CANCEL", shutdown: func(cancel context.CancelFunc, _ *nkv.Watcher) {
			cancel()
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed

			for index := range 64 {
				_, err := kv.Put(t.Context(), fmt.Sprintf("key.%d", index), []byte("value"))
				is.NoErr(err) // replay fixture put should succeed
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			watcher, err := kv.WatchAll(ctx)
			is.NoErr(err) // watch creation should succeed
			updates := watcher.Updates()

			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for len(updates) < 64 {
				select {
				case <-ticker.C:
				case <-deadline.C:
					is.True(false) // updates channel should fill before the deadline
					return
				}
			}

			test.shutdown(cancel, watcher)
			for {
				select {
				case entry, open := <-updates:
					if !open {
						return
					}
					is.True(entry != nil) // shutdown should discard the blocked replay sentinel
				case <-time.After(2 * time.Second):
					is.True(false) // a full updates channel should close promptly after shutdown
					return
				}
			}
		})
	}
}

func TestWatchConcurrentAccess(t *testing.T) {
	const (
		entryCount  = 64
		workerCount = 8
	)

	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_CONCURRENT"})
	is.NoErr(err) // bucket creation should succeed

	for index := range entryCount {
		_, err := kv.Put(t.Context(), fmt.Sprintf("key.%d", index), []byte("value"))
		is.NoErr(err) // replay fixture put should succeed
	}

	watcher, err := kv.WatchAll(t.Context(), nkv.WithPullBatch(1))
	is.NoErr(err) // watch creation should succeed
	t.Cleanup(watcher.Stop)

	type result struct {
		entry *nkv.Entry
		err   error
	}

	jobs := make(chan struct{}, entryCount+1)
	results := make(chan result, entryCount+1)
	start := make(chan struct{})
	stateDone := make(chan struct{})
	stateStopped := make(chan struct{})

	for range entryCount + 1 {
		jobs <- struct{}{}
	}
	close(jobs)

	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			<-start
			for range jobs {
				entry, err := watcher.Next()
				results <- result{entry: entry, err: err}
			}
		}()
	}

	go func() {
		defer close(stateStopped)
		<-start
		for {
			select {
			case <-stateDone:
				return
			default:
				_ = watcher.InitialDone()
			}
		}
	}()

	close(start)
	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
	case <-time.After(2 * time.Second):
		watcher.Stop()
		t.Fatal("timed out waiting for concurrent watcher reads")
	}
	close(stateDone)
	<-stateStopped
	close(results)

	keys := make(map[string]struct{}, entryCount)
	sentinels := 0
	for result := range results {
		is.NoErr(result.err) // concurrent watcher read should succeed
		if result.entry == nil {
			sentinels++
			continue
		}
		keys[result.entry.Key] = struct{}{}
	}

	is.Equal(len(keys), entryCount) // every replay entry should be delivered once
	is.Equal(sentinels, 1)          // concurrent readers should share one replay sentinel
	is.True(watcher.InitialDone())  // replay state should be safe to read concurrently
}

func TestWatchRejectsInvalidTuning(t *testing.T) {
	tests := []struct {
		name   string
		option nkv.WatchOption
	}{
		{name: "zero pull batch", option: nkv.WithPullBatch(0)},
		{name: "negative pull batch", option: nkv.WithPullBatch(-1)},
		{name: "zero inactive threshold", option: nkv.WithInactiveThreshold(0)},
		{name: "negative inactive threshold", option: nkv.WithInactiveThreshold(-time.Second)},
	}

	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_INVALID_TUNING"})
	is.New(t).NoErr(err) // bucket creation should succeed

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := kv.WatchAll(t.Context(), test.option)
			is.New(t).True(errors.Is(err, nkv.ErrInvalidOption)) // invalid tuning should be rejected before consumer creation
		})
	}

	status, err := kv.Status(t.Context())
	is.New(t).NoErr(err)                       // bucket status should remain available
	is.New(t).Equal(status.State.Consumers, 0) // invalid tuning should not create consumers
}

func TestWatchCanceledContext(t *testing.T) {
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_CANCELED_CREATE"})
	is.New(t).NoErr(err) // bucket creation should succeed

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = kv.WatchAll(ctx)
	is.New(t).True(errors.Is(err, context.Canceled)) // watch creation should respect an already canceled context
}
