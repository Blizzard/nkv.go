package nkv_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
)

func TestListEmptyBucket(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "LIST_EMPTY"})
	is.NoErr(err) // bucket creation should succeed

	entries := 0
	for _, err := range kv.List(t.Context(), ">") {
		is.NoErr(err) // empty list iteration should not fail
		entries++
	}
	is.Equal(entries, 0) // empty bucket should return no entries

	keys := 0
	for _, err := range kv.Keys(t.Context(), ">") {
		is.NoErr(err) // empty keys iteration should not fail
		keys++
	}
	is.Equal(keys, 0) // empty bucket should return no keys
}

func TestListAndKeysPatterns(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "LIST_PATTERNS"})
	is.NoErr(err) // bucket creation should succeed

	values := map[string]string{
		"config.theme":     "dark",
		"literal":          "top-level",
		"users.admin.root": "root",
		"users.alice":      "alice",
		"users.bob":        "bob",
	}
	for key, value := range values {
		_, err := kv.Put(t.Context(), key, []byte(value))
		is.NoErr(err) // list fixture put should succeed
	}

	tests := []struct {
		name     string
		pattern  string
		wantKeys []string
	}{
		{name: "all keys", pattern: ">", wantKeys: []string{"config.theme", "literal", "users.admin.root", "users.alice", "users.bob"}},
		{name: "single token wildcard", pattern: "users.*", wantKeys: []string{"users.alice", "users.bob"}},
		{name: "multi token wildcard", pattern: "users.>", wantKeys: []string{"users.admin.root", "users.alice", "users.bob"}},
		{name: "exact key", pattern: "users.alice", wantKeys: []string{"users.alice"}},
		{name: "top level only", pattern: "*", wantKeys: []string{"literal"}},
		{name: "no matches", pattern: "missing.*", wantKeys: []string{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			listedKeys := make([]string, 0)
			for entry, err := range kv.List(t.Context(), test.pattern) {
				is.NoErr(err)                                    // list iteration should not fail
				is.Equal(string(entry.Value), values[entry.Key]) // list entry should contain the stored value
				listedKeys = append(listedKeys, entry.Key)
			}
			sort.Strings(listedKeys)
			is.Equal(listedKeys, test.wantKeys) // list should match the expected keys

			keys := make([]string, 0)
			for key, err := range kv.Keys(t.Context(), test.pattern) {
				is.NoErr(err) // keys iteration should not fail
				keys = append(keys, key)
			}
			sort.Strings(keys)
			is.Equal(keys, test.wantKeys) // keys should match the list projection
		})
	}
}

func TestListTombstones(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "LIST_TOMBSTONES"})
	is.NoErr(err) // bucket creation should succeed

	for _, key := range []string{"live", "deleted", "purged"} {
		_, err := kv.Put(t.Context(), key, []byte(key+"-value"))
		is.NoErr(err) // tombstone fixture put should succeed
	}
	is.NoErr(kv.Delete(t.Context(), "deleted")) // delete fixture should succeed
	is.NoErr(kv.Purge(t.Context(), "purged"))   // purge fixture should succeed

	keys := make([]string, 0)
	for key, err := range kv.Keys(t.Context(), ">") {
		is.NoErr(err) // default keys iteration should not fail
		keys = append(keys, key)
	}
	is.Equal(keys, []string{"live"}) // default keys should omit delete and purge tombstones

	operations := make(map[string]nkv.Operation)
	for entry, err := range kv.List(t.Context(), ">", nkv.WithDeletes()) {
		is.NoErr(err) // list with deletes should not fail
		operations[entry.Key] = entry.Operation
		if entry.IsTombstone() {
			is.Equal(len(entry.Value), 0) // tombstone entries should have no value
		}
	}
	is.Equal(operations, map[string]nkv.Operation{
		"live":    nkv.OpPut,
		"deleted": nkv.OpDelete,
		"purged":  nkv.OpPurge,
	}) // list with deletes should expose both tombstone operations
}

func TestListPaginationDoesNotCreateConsumers(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "LIST_PAGES"})
	is.NoErr(err) // bucket creation should succeed

	wantKeys := make([]string, 20)
	for index := range wantKeys {
		key := fmt.Sprintf("key.%02d", index)
		wantKeys[index] = key
		_, err := kv.Put(t.Context(), key, []byte(key))
		is.NoErr(err) // pagination fixture put should succeed
	}

	before, err := kv.Status(t.Context())
	is.NoErr(err)                       // status before listing should be available
	is.Equal(before.State.Consumers, 0) // bucket should start without consumers

	for _, batch := range []int{1, 2, 3, 7, 256} {
		t.Run(fmt.Sprintf("batch %d", batch), func(t *testing.T) {
			is := is.New(t)
			seen := make(map[string]bool)
			keys := make([]string, 0, len(wantKeys))
			for key, err := range kv.Keys(t.Context(), ">", nkv.WithListBatch(batch)) {
				is.NoErr(err)       // paged keys iteration should not fail
				is.True(!seen[key]) // pagination should not return duplicate keys
				seen[key] = true
				keys = append(keys, key)
			}
			sort.Strings(keys)
			is.Equal(keys, wantKeys) // every page size should return all keys
		})
	}

	after, err := kv.Status(t.Context())
	is.NoErr(err)                      // status after listing should be available
	is.Equal(after.State.Consumers, 0) // direct get listing should not create consumers
}

func TestListPinsSnapshotAtCallTime(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "LIST_SNAPSHOT"})
	is.NoErr(err) // bucket creation should succeed

	_, err = kv.Put(t.Context(), "before", []byte("before"))
	is.NoErr(err) // initial snapshot value should be stored

	entries := kv.List(t.Context(), ">", nkv.WithListBatch(1))
	keys := kv.Keys(t.Context(), ">", nkv.WithListBatch(1))

	_, err = kv.Put(t.Context(), "after", []byte("after"))
	is.NoErr(err) // value after snapshot creation should be stored

	listed := make([]string, 0)
	for entry, err := range entries {
		is.NoErr(err) // snapshot list iteration should not fail
		listed = append(listed, entry.Key)
	}
	is.Equal(listed, []string{"before"}) // list should exclude writes after the call

	projected := make([]string, 0)
	for key, err := range keys {
		is.NoErr(err) // snapshot keys iteration should not fail
		projected = append(projected, key)
	}
	is.Equal(projected, []string{"before"}) // keys should exclude writes after the call
}

func TestListRespectsCanceledContext(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *nkv.Bucket) error
	}{
		{name: "list", run: func(ctx context.Context, kv *nkv.Bucket) error {
			for _, err := range kv.List(ctx, ">") {
				return err
			}
			return nil
		}},
		{name: "keys", run: func(ctx context.Context, kv *nkv.Bucket) error {
			for _, err := range kv.Keys(ctx, ">") {
				return err
			}
			return nil
		}},
	}

	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "LIST_CANCELED"})
	is.New(t).NoErr(err) // bucket creation should succeed
	_, err = kv.Put(t.Context(), "key", []byte("value"))
	is.New(t).NoErr(err) // cancellation fixture put should succeed

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			is.True(errors.Is(test.run(ctx, kv), context.Canceled)) // iteration should return the context cancellation
		})
	}
}

func TestListRejectsInvalidBatch(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *nkv.Bucket, int) error
	}{
		{name: "list", run: func(ctx context.Context, kv *nkv.Bucket, batch int) error {
			for _, err := range kv.List(ctx, ">", nkv.WithListBatch(batch)) {
				return err
			}
			return nil
		}},
		{name: "keys", run: func(ctx context.Context, kv *nkv.Bucket, batch int) error {
			for _, err := range kv.Keys(ctx, ">", nkv.WithListBatch(batch)) {
				return err
			}
			return nil
		}},
	}

	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "LIST_INVALID_BATCH"})
	is.New(t).NoErr(err) // bucket creation should succeed

	for _, batch := range []int{0, -1} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s/%d", test.name, batch), func(t *testing.T) {
				err := test.run(t.Context(), kv, batch)
				is.New(t).True(errors.Is(err, nkv.ErrInvalidOption)) // nonpositive batch should be rejected
			})
		}
	}
}
