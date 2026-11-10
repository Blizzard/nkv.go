package nkv_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
)

type errorWatcher[T any] interface {
	Err() error
	Stop()
	Updates() <-chan T
}

func TestWatchErrors(t *testing.T) {
	testWatchErrors(t, func(ctx context.Context, bucket *nkv.Bucket) (errorWatcher[*nkv.Entry], error) {
		return bucket.WatchAll(ctx)
	})
}

func TestStoreWatchErrors(t *testing.T) {
	testWatchErrors(t, func(ctx context.Context, bucket *nkv.Bucket) (errorWatcher[*nkv.StoreEntry[int]], error) {
		return nkv.NewStore(bucket).Watch[int](ctx, "*")
	})
}

func testWatchErrors[T any](t *testing.T, create func(context.Context, *nkv.Bucket) (errorWatcher[T], error)) {
	t.Helper()

	for _, full := range []bool{false, true} {
		for _, cause := range []string{"stop", "cancel", "deadline", "connection"} {
			t.Run(fmt.Sprintf("%s/full=%t", cause, full), func(t *testing.T) {
				testWatchTermination(t, create, cause, full)
			})
		}
	}
}

func testWatchTermination[T any](t *testing.T, create func(context.Context, *nkv.Bucket) (errorWatcher[T], error), cause string, full bool) {
	t.Helper()
	is := is.New(t)

	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "WATCH_ERRORS"})
	is.NoErr(err) // bucket creation should succeed

	if full {
		for index := range 100 {
			_, err = bucket.Put(t.Context(), fmt.Sprintf("key%d", index), []byte("42"))
			is.NoErr(err) // replay should contain enough entries to fill the adapter
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	if cause == "deadline" {
		var cancelDeadline context.CancelFunc
		ctx, cancelDeadline = context.WithTimeout(ctx, time.Second)
		defer cancelDeadline()
	}

	watcher, err := create(ctx, bucket)
	is.NoErr(err) // watcher creation should succeed
	t.Cleanup(watcher.Stop)

	is.NoErr(watcher.Err()) // a new watcher should have no termination error

	updates := watcher.Updates()
	if full {
		awaitFullWatchChannel(t, updates)
	} else {
		select {
		case <-updates:
			is.NoErr(watcher.Err()) // the replay sentinel should not record a failure

		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for initial replay")
		}
	}

	// Exercise concurrent error access while shutdown records its cause.
	readerStop := make(chan struct{})
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)

		for {
			select {
			case <-readerStop:
				return

			default:
				_ = watcher.Err()
			}
		}
	}()

	defer func() {
		close(readerStop)
		<-readerDone
	}()

	var want error
	switch cause {
	case "stop":
		watcher.Stop()

	case "cancel":
		want = context.Canceled
		cancel()

	case "deadline":
		want = context.DeadlineExceeded
		<-ctx.Done()

	case "connection":
		want = nkv.ErrWatcherStopped
		nc.Close()
	}

	awaitWatchChannelClosed(t, updates)
	is.True(errors.Is(watcher.Err(), want)) // channel closure must publish the termination cause

	watcher.Stop()
	cancel()
	is.True(errors.Is(watcher.Err(), want)) // cleanup must preserve the recorded cause
}

func awaitFullWatchChannel[T any](t *testing.T, updates <-chan T) {
	t.Helper()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for len(updates) < cap(updates) {
		select {
		case <-ticker.C:

		case <-deadline.C:
			t.Fatal("timed out filling Updates channel")
		}
	}
}

func awaitWatchChannelClosed[T any](t *testing.T, updates <-chan T) {
	t.Helper()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()

	for {
		select {
		case _, open := <-updates:
			if !open {
				return
			}

		case <-deadline.C:
			t.Fatal("timed out closing Updates channel")
		}
	}
}
