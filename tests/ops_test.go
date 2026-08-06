package nkv_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
)

func TestGetRevisions(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "GET_REVISIONS"})
	is.NoErr(err) // bucket creation should succeed

	alphaRevision, err := kv.Put(t.Context(), "alpha", []byte("one"))
	is.NoErr(err)                      // alpha put should succeed
	is.Equal(alphaRevision, uint64(1)) // alpha should use the first stream revision
	betaRevision, err := kv.Put(t.Context(), "beta", []byte("two"))
	is.NoErr(err)                     // beta put should succeed
	is.Equal(betaRevision, uint64(2)) // beta should use the second stream revision

	tests := []struct {
		name         string
		key          string
		options      []nkv.GetOption
		wantErr      error
		wantValue    string
		wantRevision uint64
	}{
		{name: "latest", key: "alpha", wantValue: "one", wantRevision: alphaRevision},
		{name: "exact revision", key: "alpha", options: []nkv.GetOption{nkv.WithRevision(alphaRevision)}, wantValue: "one", wantRevision: alphaRevision},
		{name: "revision belongs to another key", key: "alpha", options: []nkv.GetOption{nkv.WithRevision(betaRevision)}, wantErr: nkv.ErrKeyNotFound},
		{name: "revision does not exist", key: "alpha", options: []nkv.GetOption{nkv.WithRevision(999)}, wantErr: nkv.ErrKeyNotFound},
		{name: "key does not exist", key: "missing", wantErr: nkv.ErrKeyNotFound},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			entry, err := kv.Get(t.Context(), test.key, test.options...)
			if test.wantErr != nil {
				is.True(errors.Is(err, test.wantErr)) // get should return the expected sentinel error
				return
			}
			is.NoErr(err)                                 // get should find the requested key revision
			is.Equal(string(entry.Value), test.wantValue) // get should return the expected value
			is.Equal(entry.Revision, test.wantRevision)   // get should return the expected revision
		})
	}

	is.NoErr(kv.Delete(t.Context(), "alpha")) // deleting alpha should succeed
	for _, test := range []struct {
		name    string
		options []nkv.GetOption
	}{
		{name: "latest tombstone"},
		{name: "exact tombstone revision", options: []nkv.GetOption{nkv.WithRevision(3)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			_, err := kv.Get(t.Context(), "alpha", test.options...)
			is.True(errors.Is(err, nkv.ErrKeyNotFound)) // tombstone should be exposed as a missing key
		})
	}
}

func TestCreateEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		bucket    string
		tombstone func(context.Context, *nkv.Bucket, string) error
	}{
		{
			name:   "after delete",
			bucket: "CREATE_AFTER_DELETE",
			tombstone: func(ctx context.Context, kv *nkv.Bucket, key string) error {
				return kv.Delete(ctx, key)
			},
		},
		{
			name:   "after purge",
			bucket: "CREATE_AFTER_PURGE",
			tombstone: func(ctx context.Context, kv *nkv.Bucket, key string) error {
				return kv.Purge(ctx, key)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed

			revision, err := kv.Create(t.Context(), "key", []byte("first"))
			is.NoErr(err)                 // first create should succeed
			is.Equal(revision, uint64(1)) // first create should use revision one
			_, err = kv.Create(t.Context(), "key", []byte("duplicate"))
			is.True(errors.Is(err, nkv.ErrKeyExists)) // create should reject an existing live key

			is.NoErr(test.tombstone(t.Context(), kv, "key")) // tombstone operation should succeed
			revision, err = kv.Create(t.Context(), "key", []byte("restored"))
			is.NoErr(err)                 // create should restore a tombstoned key
			is.Equal(revision, uint64(3)) // restored key should follow the tombstone revision

			entry, err := kv.Get(t.Context(), "key")
			is.NoErr(err)                             // restored key should be retrievable
			is.Equal(string(entry.Value), "restored") // restored key should contain the new value
		})
	}
}

func TestCreateConcurrent(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "CREATE_CONCURRENT"})
	is.NoErr(err) // bucket creation should succeed

	const attempts = 16
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for range attempts {
		wait.Go(func() {
			_, err := kv.Create(t.Context(), "key", []byte("value"))
			results <- err
		})
	}
	wait.Wait()
	close(results)

	created := 0
	alreadyExists := 0
	unexpected := 0
	for err := range results {
		switch {
		case err == nil:
			created++
		case errors.Is(err, nkv.ErrKeyExists):
			alreadyExists++
		default:
			unexpected++
		}
	}
	is.Equal(created, 1)                // exactly one concurrent create should succeed
	is.Equal(alreadyExists, attempts-1) // all losing creates should report an existing key
	is.Equal(unexpected, 0)             // concurrent creates should not return unexpected errors
}

func TestUpdateEdgeCases(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "UPDATE_EDGES"})
	is.NoErr(err) // bucket creation should succeed

	revision, err := kv.Put(t.Context(), "existing", []byte("one"))
	is.NoErr(err)                 // initial put should succeed
	is.Equal(revision, uint64(1)) // initial put should use revision one

	tests := []struct {
		name         string
		key          string
		value        string
		revision     uint64
		wantErr      error
		wantRevision uint64
		wantValue    string
	}{
		{name: "stale existing revision", key: "existing", value: "stale", revision: 2, wantErr: nkv.ErrRevisionMismatch, wantValue: "one"},
		{name: "current existing revision", key: "existing", value: "two", revision: 1, wantRevision: 2, wantValue: "two"},
		{name: "missing nonzero revision", key: "missing", value: "nope", revision: 1, wantErr: nkv.ErrRevisionMismatch},
		{name: "missing zero revision", key: "missing", value: "created", revision: 0, wantRevision: 3, wantValue: "created"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			revision, err := kv.Update(t.Context(), test.key, []byte(test.value), test.revision)
			if test.wantErr != nil {
				is.True(errors.Is(err, test.wantErr)) // update should return the expected CAS error
			} else {
				is.NoErr(err)                         // update with a matching revision should succeed
				is.Equal(revision, test.wantRevision) // successful update should return the next stream revision
			}

			entry, getErr := kv.Get(t.Context(), test.key)
			if test.wantValue == "" {
				is.True(errors.Is(getErr, nkv.ErrKeyNotFound)) // failed update should not create a missing key
				return
			}
			is.NoErr(getErr)                              // expected key should remain retrievable
			is.Equal(string(entry.Value), test.wantValue) // update should leave the expected value
		})
	}
}

func TestDeleteEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		put         bool
		options     []nkv.DeleteOption
		wantErr     error
		wantMissing bool
	}{
		{name: "current revision", bucket: "DELETE_CURRENT", put: true, options: []nkv.DeleteOption{nkv.WithRevision(1)}, wantMissing: true},
		{name: "stale revision", bucket: "DELETE_STALE", put: true, options: []nkv.DeleteOption{nkv.WithRevision(2)}, wantErr: nkv.ErrRevisionMismatch},
		{name: "missing with revision", bucket: "DELETE_MISSING_CAS", options: []nkv.DeleteOption{nkv.WithRevision(1)}, wantErr: nkv.ErrRevisionMismatch, wantMissing: true},
		{name: "missing unconditional", bucket: "DELETE_MISSING", wantMissing: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed
			if test.put {
				_, err = kv.Put(t.Context(), "key", []byte("value"))
				is.NoErr(err) // test value setup should succeed
			}

			err = kv.Delete(t.Context(), "key", test.options...)
			if test.wantErr != nil {
				is.True(errors.Is(err, test.wantErr)) // delete should return the expected CAS error
			} else {
				is.NoErr(err) // valid delete should succeed
			}

			entry, getErr := kv.Get(t.Context(), "key")
			if test.wantMissing {
				is.True(errors.Is(getErr, nkv.ErrKeyNotFound)) // deleted or absent key should not be found
				return
			}
			is.NoErr(getErr)                       // failed CAS delete should preserve the key
			is.Equal(string(entry.Value), "value") // failed CAS delete should preserve the value
		})
	}
}

func TestPurgeEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		put         bool
		options     []nkv.PurgeOption
		wantErr     error
		wantMissing bool
	}{
		{name: "current revision", bucket: "PURGE_CURRENT", put: true, options: []nkv.PurgeOption{nkv.WithRevision(1)}, wantMissing: true},
		{name: "stale revision", bucket: "PURGE_STALE", put: true, options: []nkv.PurgeOption{nkv.WithRevision(2)}, wantErr: nkv.ErrRevisionMismatch},
		{name: "missing with revision", bucket: "PURGE_MISSING_CAS", options: []nkv.PurgeOption{nkv.WithRevision(1)}, wantErr: nkv.ErrRevisionMismatch, wantMissing: true},
		{name: "missing unconditional", bucket: "PURGE_MISSING", wantMissing: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed
			if test.put {
				_, err = kv.Put(t.Context(), "key", []byte("value"))
				is.NoErr(err) // test value setup should succeed
			}

			err = kv.Purge(t.Context(), "key", test.options...)
			if test.wantErr != nil {
				is.True(errors.Is(err, test.wantErr)) // purge should return the expected CAS error
			} else {
				is.NoErr(err) // valid purge should succeed
			}

			entry, getErr := kv.Get(t.Context(), "key")
			if test.wantMissing {
				is.True(errors.Is(getErr, nkv.ErrKeyNotFound)) // purged or absent key should not be found
				return
			}
			is.NoErr(getErr)                       // failed CAS purge should preserve the key
			is.Equal(string(entry.Value), "value") // failed CAS purge should preserve the value
		})
	}
}

func TestOperationsRespectCanceledContext(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *nkv.Bucket) error
	}{
		{name: "get", run: func(ctx context.Context, kv *nkv.Bucket) error {
			_, err := kv.Get(ctx, "key")
			return err
		}},
		{name: "put", run: func(ctx context.Context, kv *nkv.Bucket) error {
			_, err := kv.Put(ctx, "put", []byte("value"))
			return err
		}},
		{name: "create", run: func(ctx context.Context, kv *nkv.Bucket) error {
			_, err := kv.Create(ctx, "create", []byte("value"))
			return err
		}},
		{name: "update", run: func(ctx context.Context, kv *nkv.Bucket) error {
			_, err := kv.Update(ctx, "update", []byte("value"), 0)
			return err
		}},
		{name: "delete", run: func(ctx context.Context, kv *nkv.Bucket) error {
			return kv.Delete(ctx, "delete")
		}},
		{name: "purge", run: func(ctx context.Context, kv *nkv.Bucket) error {
			return kv.Purge(ctx, "purge")
		}},
	}

	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "CANCELED_CONTEXT"})
	is.New(t).NoErr(err) // bucket creation should succeed

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			is.True(errors.Is(test.run(ctx, kv), context.Canceled)) // operation should return the context cancellation
		})
	}
}
