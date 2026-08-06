package nkv_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go/jetstream"
)

func TestKeyValueBasics(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	nc := testConnection(t)

	kv, err := nkv.CreateBucket(ctx, nc, nkv.Config{
		Bucket: "TEST",
		StreamConfig: jetstream.StreamConfig{
			MaxAge:   time.Hour,
			Metadata: map[string]string{"foo": "bar"},
		},
	})
	is.NoErr(err)                    // bucket creation should succeed
	is.Equal(kv.Name(), "TEST")      // bucket should retain its configured name
	is.Equal(kv.Stream(), "KV_TEST") // bucket should expose its backing stream name

	revision, err := kv.Put(ctx, "name", []byte("derek"))
	is.NoErr(err)                 // initial put should succeed
	is.Equal(revision, uint64(1)) // initial put should create the first stream revision

	entry, err := kv.Get(ctx, "name")
	is.NoErr(err) // stored value should be retrievable
	assertEntry(t, entry, entryWant{
		bucket:    "TEST",
		key:       "name",
		value:     "derek",
		revision:  1,
		delta:     0,
		operation: nkv.OpPut,
	})

	is.NoErr(kv.Delete(ctx, "name")) // deleting an existing key should succeed
	_, err = kv.Get(ctx, "name")
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // deleted key should not be found

	revision, err = kv.Create(ctx, "name", []byte("derek"))
	is.NoErr(err)                 // create should restore a tombstoned key
	is.Equal(revision, uint64(3)) // create should follow the put and delete revisions

	err = kv.Delete(ctx, "name", nkv.WithRevision(4))
	is.True(errors.Is(err, nkv.ErrRevisionMismatch))      // delete should reject an incorrect revision
	is.NoErr(kv.Delete(ctx, "name", nkv.WithRevision(3))) // delete should accept the current revision

	revision, err = kv.Update(ctx, "name", []byte("rip"), 4)
	is.NoErr(err)                 // update should accept the delete revision
	is.Equal(revision, uint64(5)) // successful update should advance the stream revision

	_, err = kv.Update(ctx, "name", []byte("ik"), 3)
	is.True(errors.Is(err, nkv.ErrRevisionMismatch)) // update should reject a stale revision

	revision, err = kv.Update(ctx, "name", []byte("ik"), revision)
	is.NoErr(err)                 // update should accept the current revision
	is.Equal(revision, uint64(6)) // second successful update should advance the revision

	ageRevision, err := kv.Create(ctx, "age", []byte("22"))
	is.NoErr(err)                    // create should add a new key
	is.Equal(ageRevision, uint64(7)) // new key should use the next stream revision

	ageRevision, err = kv.Update(ctx, "age", []byte("33"), ageRevision)
	is.NoErr(err)                    // age update should accept its current revision
	is.Equal(ageRevision, uint64(8)) // age update should advance the stream revision

	status, err := kv.Status(ctx)
	is.NoErr(err)                                  // bucket status should be available
	is.Equal(status.Config.Name, "KV_TEST")        // status should report the backing stream
	is.Equal(status.Config.MaxAge, time.Hour)      // status should retain the configured maximum age
	is.Equal(status.Config.Metadata["foo"], "bar") // status should retain configured metadata
	is.Equal(status.State.Msgs, uint64(2))         // history one should retain one message per live key
}

func TestKeyValueInvalidKeys(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *nkv.Bucket) error
	}{
		{name: "put", run: func(ctx context.Context, kv *nkv.Bucket) error {
			_, err := kv.Put(ctx, ".invalid", []byte("value"))
			return err
		}},
		{name: "get", run: func(ctx context.Context, kv *nkv.Bucket) error {
			_, err := kv.Get(ctx, ".invalid")
			return err
		}},
		{name: "create", run: func(ctx context.Context, kv *nkv.Bucket) error {
			_, err := kv.Create(ctx, ".invalid", []byte("value"))
			return err
		}},
		{name: "update", run: func(ctx context.Context, kv *nkv.Bucket) error {
			_, err := kv.Update(ctx, ".invalid", []byte("value"), 1)
			return err
		}},
		{name: "delete", run: func(ctx context.Context, kv *nkv.Bucket) error {
			return kv.Delete(ctx, ".invalid")
		}},
		{name: "purge", run: func(ctx context.Context, kv *nkv.Bucket) error {
			return kv.Purge(ctx, ".invalid")
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: "INVALID_KEYS"})
	is.New(t).NoErr(err) // bucket creation should succeed

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			is.True(errors.Is(test.run(ctx, kv), nkv.ErrInvalidKey)) // operation should reject an invalid key
		})
	}
}
