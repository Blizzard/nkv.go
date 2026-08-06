package nkv_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type entryWant struct {
	bucket    string
	key       string
	value     string
	revision  uint64
	delta     uint64
	operation nkv.Operation
}

func assertPanicContains(t *testing.T, match string, fn func()) {
	t.Helper()
	defer func() {
		value := recover()
		is := is.New(t)
		is.True(value != nil) // invalid configuration should panic
		if value != nil {
			is.True(strings.Contains(fmt.Sprint(value), match)) // panic should describe the invalid configuration
		}
	}()
	fn()
}

func assertEntry(t *testing.T, got *nkv.Entry, want entryWant) {
	t.Helper()
	is := is.New(t)
	is.True(got != nil)                     // entry should not be nil
	is.Equal(got.Bucket, want.bucket)       // entry should report the expected bucket
	is.Equal(got.Key, want.key)             // entry should report the expected key
	is.Equal(string(got.Value), want.value) // entry should contain the expected value
	is.Equal(got.Revision, want.revision)   // entry should report the expected revision
	is.Equal(got.Delta, want.delta)         // entry should report the expected delivery delta
	is.Equal(got.Operation, want.operation) // entry should report the expected operation
	is.True(!got.Created.IsZero())          // entry should include its creation time
}

func testConnection(t *testing.T) *nats.Conn {
	t.Helper()
	is := is.New(t)

	ns, err := server.NewServer(&server.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		DontListen: true,
		NoLog:      true,
		NoSigs:     true,
	})
	is.NoErr(err) // test server creation should succeed
	ns.Start()
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	is.True(ns.ReadyForConnections(5 * time.Second)) // test server should become ready

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	is.NoErr(err) // test client should connect to the server
	t.Cleanup(nc.Close)

	return nc
}

func nextWatch(t *testing.T, watcher *nkv.Watcher) (*nkv.Entry, error) {
	t.Helper()
	type result struct {
		entry *nkv.Entry
		err   error
	}
	done := make(chan result, 1)
	go func() {
		entry, err := watcher.Next()
		done <- result{entry: entry, err: err}
	}()

	select {
	case result := <-done:
		return result.entry, result.err
	case <-time.After(2 * time.Second):
		watcher.Stop()
		t.Fatal("timed out waiting for watcher.Next")
		return nil, context.DeadlineExceeded
	}
}

func lastRawMessage(t *testing.T, nc *nats.Conn, bucket *nkv.Bucket, key string) *jetstream.RawStreamMsg {
	t.Helper()
	is := is.New(t)
	js, err := jetstream.New(nc)
	is.NoErr(err) // JetStream client creation should succeed
	stream, err := js.Stream(t.Context(), bucket.Stream())
	is.NoErr(err) // backing stream should be available
	message, err := stream.GetLastMsgForSubject(t.Context(), "$KV."+bucket.Name()+"."+key)
	is.NoErr(err) // raw KV message should be available
	return message
}
