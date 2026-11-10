package nkv_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
)

func TestStoreWatchTypesAndFilters(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: "STORE_WATCH"})
	is.NoErr(err) // bucket creation should succeed
	store := nkv.NewStore(bucket, nkv.WithPrefix("app"))
	_, err = store.Put(ctx, "names.tom", "Tom")
	is.NoErr(err) // replay fixture should be written
	_, err = store.Put(ctx, "labels.region", "west")
	is.NoErr(err) // additional-filter fixture should be written
	_, err = store.Put(ctx, "counts.users", 1)
	is.NoErr(err) // a different value type should share the Store
	_, err = bucket.Put(ctx, "names.outside", []byte(`"outside"`))
	is.NoErr(err) // unprefixed fixture should be written

	options := []nkv.WatchOption{nkv.WithAdditionalKeys("labels.*"), nkv.WithPullBatch(1)}
	names, err := store.Watch[string](ctx, "names.*", options...)
	is.NoErr(err) // string watcher should start
	t.Cleanup(names.Stop)
	counts, err := store.Watch[int](ctx, "counts.*")
	is.NoErr(err) // another watcher should select a different decode type
	t.Cleanup(counts.Stop)

	got := make(map[string]string)
	for range 2 {
		entry, err := names.Next()
		is.NoErr(err)                                                  // replay delivery should succeed
		is.Equal(entry.Bucket, bucket.Name())                          // replay should retain the bucket
		is.Equal(entry.ContentType, "application/json")                // replay should retain the codec header
		is.Equal(entry.Headers.Get("Content-Type"), entry.ContentType) // replay should retain headers
		is.True(len(entry.Entry.Value) > 0)                            // replay should retain encoded bytes
		is.True(!entry.Created.IsZero())                               // replay should retain timestamps
		got[entry.Key] = entry.Value
	}
	is.Equal(got, map[string]string{"names.tom": "Tom", "labels.region": "west"}) // all watch filters should be scoped and stripped
	entry, err := names.Next()
	is.NoErr(err)                // replay sentinel should not fail
	is.True(entry == nil)        // string watcher should preserve the nil sentinel
	is.True(names.InitialDone()) // replay should now be complete
	count, err := counts.Next()
	is.NoErr(err)            // numeric watcher should decode its own pattern
	is.Equal(count.Value, 1) // numeric replay should preserve the value
	count, err = counts.Next()
	is.NoErr(err)         // numeric replay sentinel should not fail
	is.True(count == nil) // numeric watcher should also preserve the sentinel

	// Options must be reusable without accumulating the Store's prefix.
	again, err := store.Watch[string](ctx, "names.*", options...)
	is.NoErr(err) // reused additional filters should remain valid
	t.Cleanup(again.Stop)
	for range 2 {
		_, err := again.Next()
		is.NoErr(err) // reused filters should replay both values
	}

	revision, err := store.Put(ctx, "names.tom", "Thomas")
	is.NoErr(err) // live update should succeed
	entry, err = names.Next()
	is.NoErr(err)                      // live update should be delivered
	is.Equal(entry.Value, "Thomas")    // live update should decode as the watcher's T
	is.Equal(entry.Revision, revision) // live update should preserve its revision
	is.Equal(entry.Key, "names.tom")   // live update should remove the Store prefix
}

func TestStoreWatchMetadataAndTombstones(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: "STORE_WATCH_META"})
	is.NoErr(err) // bucket creation should succeed
	store := nkv.NewStore(bucket, nkv.WithPrefix("scope"))
	meta, err := store.Watch[int](ctx, "*", nkv.WithMetaOnly(), nkv.WithUpdatesOnly())
	is.NoErr(err) // metadata-only watcher should start
	t.Cleanup(meta.Stop)
	values, err := store.Watch[int](ctx, "*", nkv.WithUpdatesOnly())
	is.NoErr(err) // value watcher should start
	t.Cleanup(values.Stop)
	_, err = store.Put(ctx, "key", 7)
	is.NoErr(err) // live fixture should be written
	entry, err := meta.Next()
	is.NoErr(err)                       // metadata-only delivery should not decode empty bytes
	is.Equal(entry.Value, 0)            // metadata-only entries should have the zero value
	is.Equal(len(entry.Entry.Value), 0) // metadata-only entries should omit wire bytes
	is.Equal(entry.Key, "key")          // metadata-only entries should strip the prefix
	_, err = values.Next()
	is.NoErr(err)                                      // value watcher should consume the initial put
	is.NoErr(store.Delete(ctx, "key", nkv.WithTTL(0))) // delete should publish a tombstone
	entry, err = values.Next()
	is.NoErr(err)                           // tombstones must not attempt to decode an empty payload
	is.Equal(entry.Value, 0)                // tombstones should carry the zero value
	is.Equal(entry.Operation, nkv.OpDelete) // tombstones should retain their operation
	is.True(entry.IsTombstone())            // embedded metadata methods should remain available
}

func TestStoreWatchDecodeErrors(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: "STORE_WATCH_ERRORS"})
	is.NoErr(err) // bucket creation should succeed

	store := nkv.NewStore(bucket)
	_, err = bucket.Put(ctx, "bad", []byte(`"not an integer"`))
	is.NoErr(err) // incompatible replay fixture should be written

	_, err = store.Put(ctx, "good", 42)
	is.NoErr(err) // compatible replay fixture should be written

	watcher, err := store.Watch[int](ctx, "*")
	is.NoErr(err) // watcher should start before decoding entries
	t.Cleanup(watcher.Stop)

	entry, err := watcher.Next()
	is.True(err != nil)     // incompatible data should fail at delivery time
	is.True(entry == nil)   // decode failures should not return partially decoded entries
	is.NoErr(watcher.Err()) // recoverable direct errors should not terminate the watcher

	entry, err = watcher.Next()
	is.NoErr(err)             // Next should continue after a decode failure
	is.Equal(entry.Value, 42) // the next compatible entry should be decoded
	is.NoErr(watcher.Err())   // successful direct iteration should leave Err nil

	entry, err = watcher.Next()
	is.NoErr(err)                  // initial replay should still terminate normally
	is.True(entry == nil)          // the replay sentinel should follow consumed entries
	is.True(watcher.InitialDone()) // replay state should survive decode errors

	watcher.Stop()
	_, err = watcher.Next()
	is.True(errors.Is(err, nkv.ErrWatcherStopped)) // stopped watcher errors should pass through

	_, err = store.Watch[int](ctx, "*", nkv.WithPullBatch(0))
	is.True(errors.Is(err, nkv.ErrInvalidOption)) // watch option validation should pass through
}

func TestStoreWatchUpdates(t *testing.T) {
	for _, test := range []struct {
		name string
		bad  bool
	}{
		{name: "replay and live"},
		{name: "decode failure", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			nc := testConnection(t)
			bucket, err := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: "STORE_WATCH_UPDATES"})
			is.NoErr(err) // bucket creation should succeed

			store := nkv.NewStore(bucket)
			watcher, err := store.Watch[int](ctx, "*")
			is.NoErr(err) // empty watcher should start
			t.Cleanup(watcher.Stop)

			updates := watcher.Updates()
			is.Equal(watcher.Updates(), updates) // Updates should reuse one channel

			select {
			case entry, open := <-updates:
				is.True(open && entry == nil) // empty replay should deliver its sentinel

			case <-ctx.Done():
				t.Fatal("timed out waiting for replay sentinel")
			}

			if test.bad {
				_, err = store.Put(ctx, "key", "not an integer")
			} else {
				_, err = store.Put(ctx, "key", 42)
			}

			is.NoErr(err) // live fixture should be written

			select {
			case entry, open := <-updates:
				if test.bad {
					is.True(!open) // decode errors should close the channel

					var decodeErr *json.UnmarshalTypeError
					is.True(errors.As(watcher.Err(), &decodeErr)) // the original decoding error should remain inspectable
				} else {
					is.True(open && entry != nil) // compatible live values should be delivered
					is.Equal(entry.Value, 42)     // live channel values should be decoded
				}

			case <-ctx.Done():
				t.Fatal("timed out waiting for live update")
			}

			streamErr := watcher.Err()
			watcher.Stop()
			is.Equal(watcher.Err(), streamErr) // stopping must not overwrite an adapter failure

			select {
			case _, open := <-updates:
				is.True(!open) // stopping should close the adapter

			case <-ctx.Done():
				t.Fatal("timed out waiting for channel close")
			}

			cancel()
			is.Equal(watcher.Err(), streamErr) // context cleanup must preserve the adapter result
		})
	}
}

func TestStoreWatchUpdatesCancellation(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: "STORE_WATCH_CANCEL"})
	is.NoErr(err) // bucket creation should succeed
	store := nkv.NewStore(bucket)
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	watcher, err := store.Watch[int](watchCtx, "*", nkv.WithUpdatesOnly())
	is.NoErr(err) // watcher should start
	t.Cleanup(watcher.Stop)
	updates := watcher.Updates()
	for i := range 100 {
		_, err := store.Put(ctx, fmt.Sprintf("key%d", i), i)
		is.NoErr(err) // writes should not depend on the caller draining Updates
	}
	stop()
	for {
		select {
		case _, open := <-updates:
			if !open {
				return // cancellation must unblock even a full channel adapter
			}

		case <-ctx.Done():
			t.Fatal("timed out closing canceled Updates adapter")
		}
	}
}
