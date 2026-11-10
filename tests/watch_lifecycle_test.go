package nkv_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestWatchReconnectBetweenReads(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	bucket, err := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: "WATCH_RECONNECT"})
	is.NoErr(err) // bucket creation should succeed

	watcher, err := bucket.WatchAll(ctx, nkv.WithUpdatesOnly(), nkv.WithPullBatch(4))
	is.NoErr(err) // live watcher should start
	t.Cleanup(watcher.Stop)

	_, err = bucket.Put(ctx, "before", []byte("before"))
	is.NoErr(err) // pre-reconnect update should be written

	entry, err := nextWatch(t, watcher)
	is.NoErr(err)                 // first read should establish the watch's delivered sequence
	is.Equal(entry.Key, "before") // first read should consume the pre-reconnect entry

	connected := nc.StatusChanged(nats.CONNECTED)
	defer nc.RemoveStatusListener(connected)

	// Do not call Next again until the connection has fully reconnected. The
	// iterator must recover even when no read observes the disconnected state.
	is.NoErr(nc.ForceReconnect()) // the in-process connection should enter reconnect

	select {
	case <-connected:
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconnect")
	}

	revisions := make([]uint64, 2)
	for i := range revisions {
		key := fmt.Sprintf("after.%d", i)
		revisions[i], err = bucket.Put(ctx, key, []byte(key))
		is.NoErr(err) // publishing should resume on the healthy connection
	}

	for i, revision := range revisions {
		entry, err := nextWatch(t, watcher)
		is.NoErr(err)                                   // watch must resume without waiting for heartbeat recovery
		is.True(entry != nil)                           // reconnect must not introduce another replay sentinel
		is.Equal(entry.Key, fmt.Sprintf("after.%d", i)) // prior entries must not be replayed or new entries skipped
		is.Equal(entry.Revision, revision)              // resumed entries must preserve stream order
	}
}

func TestWatchNextUnblocksOnConnectionClose(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)

	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_CONNECTION_CLOSE"})
	is.NoErr(err) // bucket creation should succeed

	watcher, err := bucket.WatchAll(t.Context(), nkv.WithUpdatesOnly())
	is.NoErr(err) // live watcher should start
	t.Cleanup(watcher.Stop)

	started := make(chan struct{})
	readErrors := make(chan error, 1)

	go func() {
		close(started)

		_, err := watcher.Next()
		readErrors <- err
	}()

	<-started
	nc.Close()

	select {
	case err := <-readErrors:
		is.True(errors.Is(err, nkv.ErrWatcherStopped)) // connection closure must terminate the active reader
	case <-time.After(2 * time.Second):
		t.Fatal("watcher.Next did not return after connection close")
	}
}

func TestWatchUpdatesCloseOnConnectionClose(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		is := is.New(t)
		nc := testConnection(t)

		bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_UPDATES_CLOSE"})
		is.NoErr(err) // bucket creation should succeed

		watcher, err := bucket.WatchAll(t.Context(), nkv.WithUpdatesOnly())
		is.NoErr(err) // live watcher should start
		t.Cleanup(watcher.Stop)

		updates := watcher.Updates()
		nc.Close()

		select {
		case _, open := <-updates:
			is.True(!open) // connection closure must close the byte channel without an explicit Stop
		case <-time.After(2 * time.Second):
			t.Fatal("byte Updates channel did not close with the connection")
		}
	})

	t.Run("Store", func(t *testing.T) {
		is := is.New(t)
		nc := testConnection(t)

		bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_WATCH_UPDATES_CLOSE"})
		is.NoErr(err) // bucket creation should succeed

		store := nkv.NewStore(bucket)
		watcher, err := store.Watch[int](t.Context(), ">", nkv.WithUpdatesOnly())
		is.NoErr(err) // typed live watcher should start
		t.Cleanup(watcher.Stop)

		updates := watcher.Updates()
		nc.Close()

		select {
		case _, open := <-updates:
			is.True(!open) // connection closure must also close the Store channel adapter
		case <-time.After(2 * time.Second):
			t.Fatal("Store Updates channel did not close with the connection")
		}
	})
}

func TestWatchStopDuringBufferedReplay(t *testing.T) {
	const batch = 8

	is := is.New(t)
	nc := testConnection(t)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	bucket, err := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: "WATCH_BUFFERED_STOP"})
	is.NoErr(err) // bucket creation should succeed

	for i := range 64 {
		_, err := bucket.Put(ctx, fmt.Sprintf("key.%d", i), []byte("value"))
		is.NoErr(err) // replay should contain more messages than one pull batch
	}

	watcher, err := bucket.WatchAll(ctx, nkv.WithPullBatch(batch))
	is.NoErr(err) // replay watcher should start

	entry, err := nextWatch(t, watcher)
	is.NoErr(err)         // first read should request and begin consuming a batch
	is.True(entry != nil) // buffered replay must begin with a data entry

	// Observe delivery through the public consumer API rather than sleeping
	// and assuming the remaining pull messages have been sent.
	js, err := jetstream.New(nc)
	is.NoErr(err) // consumer inspection should use the existing connection

	stream, err := js.Stream(ctx, bucket.Stream())
	is.NoErr(err) // backing stream should be available

	list := stream.ListConsumers(ctx)
	var infos []*jetstream.ConsumerInfo
	for info := range list.Info() {
		infos = append(infos, info)
	}
	is.NoErr(list.Err())    // consumer enumeration should succeed
	is.Equal(len(infos), 1) // the watcher should own the only consumer

	consumer, err := stream.Consumer(ctx, infos[0].Name)
	is.NoErr(err) // existing pull consumer should be inspectable

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		info, err := consumer.Info(ctx)
		is.NoErr(err) // delivery state should remain available before stop

		if info.Delivered.Consumer >= batch {
			break
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("pull batch was not delivered before the deadline")
		}
	}

	start := make(chan struct{})
	stopped := make(chan struct{})
	readErrors := make(chan error, 1)

	// Race Stop against consumption of buffered pull messages, not against
	// a full Updates adapter. Refilling the pull window must not deadlock.
	go func() {
		<-start

		for {
			if _, err := watcher.Next(); err != nil {
				readErrors <- err

				return
			}
		}
	}()

	go func() {
		<-start

		watcher.Stop()
		close(stopped)
	}()

	close(start)

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop deadlocked during buffered replay")
	}

	select {
	case err := <-readErrors:
		is.True(errors.Is(err, nkv.ErrWatcherStopped)) // replay reader should terminate after Stop
	case <-time.After(2 * time.Second):
		t.Fatal("buffered replay reader did not return after Stop")
	}
}
