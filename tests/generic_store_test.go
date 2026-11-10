package nkv_test

import (
	"errors"
	"testing"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
)

func TestGenericStoreCompatibility(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "GENERIC_STORE"})
	is.NoErr(err) // bucket creation should succeed
	options := []nkv.GenericOption{nkv.WithPrefix("app")}
	store := nkv.NewStore(bucket, options...)
	legacy := nkv.NewGeneric[string](bucket, options...)
	_, err = store.Put(t.Context(), "store", "new")
	is.NoErr(err) // Store fixture should be written
	value, _, err := legacy.Get(t.Context(), "store")
	is.NoErr(err)          // compatibility wrapper should read Store data
	is.Equal(value, "new") // compatibility wrapper should preserve codec semantics
	_, err = legacy.Put(t.Context(), "legacy", "old")
	is.NoErr(err) // compatibility wrapper should still write
	value, _, err = store.Get[string](t.Context(), "legacy")
	is.NoErr(err)          // Store should read compatibility-wrapper data
	is.Equal(value, "old") // Store should preserve codec semantics

	watcher, err := legacy.Watch(t.Context(), "*")
	is.NoErr(err) // compatibility watcher should start
	t.Cleanup(watcher.Stop)
	entry, err := watcher.Next()
	is.NoErr(err)                // watcher aliases should require no adapter
	is.Equal(entry.Value, "new") // entry aliases should retain typed data
}

func TestGenericTxStoreCompatibility(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "GENERIC_STORE_TX"})
	is.NoErr(err) // bucket creation should succeed
	legacy := nkv.NewGeneric[int](bucket, nkv.WithPrefix("app"))
	revision, err := legacy.Put(t.Context(), "update", 1)
	is.NoErr(err) // compatibility CAS fixture should be written
	_, err = legacy.Put(t.Context(), "delete", 2)
	is.NoErr(err) // compatibility delete fixture should be written
	tx := legacy.Tx()
	defer tx.Abort()
	is.NoErr(tx.Update("update", 3, revision))    // compatibility wrapper should forward CAS staging
	is.NoErr(tx.Delete("delete", nkv.WithTTL(0))) // compatibility wrapper should forward delete options
	_, err = tx.Commit(t.Context())
	is.NoErr(err) // compatibility transaction should commit through StoreTx
	store := nkv.NewStore(bucket, nkv.WithPrefix("app"))
	value, _, err := store.Get[int](t.Context(), "update")
	is.NoErr(err)      // Store should read compatibility transaction updates
	is.Equal(value, 3) // compatibility update should preserve the value
	_, _, err = store.Get[int](t.Context(), "delete")
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // compatibility delete should preserve the prefix
	aborted := legacy.Tx()
	aborted.Abort()
	is.True(errors.Is(aborted.Put("closed", 4), nkv.ErrTxClosed)) // compatibility abort should close StoreTx

	byteTx := bucket.Tx()
	defer byteTx.Abort()
	storeTx := nkv.NewStoreTx(byteTx, nkv.WithPrefix("store"))
	legacyTx := nkv.NewGenericTx[string](byteTx, nkv.WithPrefix("legacy"))
	is.NoErr(storeTx.Put("number", 42))     // StoreTx should share the underlying transaction
	is.NoErr(legacyTx.Put("name", "Tom"))   // compatibility wrapper should share the underlying transaction
	is.Equal(legacyTx.Len(), storeTx.Len()) // wrappers should report the same staged length
	_, err = legacyTx.Commit(t.Context())
	is.NoErr(err) // compatibility commit should publish all wrappers' operations
	value, _, err = nkv.NewStore(bucket).Get[int](t.Context(), "store.number")
	is.NoErr(err)       // Store data should be visible after compatibility commit
	is.Equal(value, 42) // Store data should survive compatibility commit
}
