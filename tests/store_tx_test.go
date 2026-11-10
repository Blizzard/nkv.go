package nkv_test

import (
	"errors"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
)

func TestStoreTxMixedTypes(t *testing.T) {
	type user struct{ Name string }
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_TX"})
	is.NoErr(err) // bucket creation should succeed
	codec := storeTestCodec("application/store-tx", "tx:")
	store := nkv.NewStore(bucket, nkv.WithPrefix("app"), nkv.WithWriteCodec(codec))
	revision, err := store.Put(t.Context(), "update", "before")
	is.NoErr(err) // CAS fixture should be written
	deletedRevision, err := store.Put(t.Context(), "delete", 99)
	is.NoErr(err) // delete fixture should be written
	tx := store.Tx(nkv.WithTxTTL(time.Hour))
	defer tx.Abort()
	is.NoErr(tx.Put("user", user{Name: "Tom"}))                                      // write type should be inferred per operation
	is.NoErr(tx.Create[int]("count", 42))                                            // explicit type arguments should work in transactions
	is.NoErr(tx.Update("update", "after", revision))                                 // CAS update should stage
	is.NoErr(tx.Delete("delete", nkv.WithRevision(deletedRevision), nkv.WithTTL(0))) // CAS delete should stage
	is.Equal(tx.Len(), 4)                                                            // transaction should include every value type
	_, _, err = store.Get[user](t.Context(), "user")
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // writes should not be visible before commit
	result, err := tx.Commit(t.Context())
	is.NoErr(err)            // mixed-type transaction should commit atomically
	is.Equal(result.Size, 4) // result should report all operations
	value, _, err := store.Get[user](t.Context(), "user")
	is.NoErr(err)                      // custom transaction codec should be readable
	is.Equal(value, user{Name: "Tom"}) // transaction should inherit the Store's codec and prefix
	count, _, err := store.Get[int](t.Context(), "count")
	is.NoErr(err)       // a different transaction type should be readable
	is.Equal(count, 42) // transaction should preserve primitive values
	updated, _, err := store.Get[string](t.Context(), "update")
	is.NoErr(err)              // CAS update should be readable
	is.Equal(updated, "after") // CAS update should replace the value
	_, _, err = store.Get[int](t.Context(), "delete")
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // committed tombstone should hide the deleted key
	message := lastRawMessage(t, nc, bucket, "app.user")
	is.Equal(message.Header.Get("Content-Type"), codec.ContentType) // transaction should stamp its codec header
	is.Equal(message.Header.Get("Nats-TTL"), "1h0m0s")              // transaction TTL should reach the wire
	message = lastRawMessage(t, nc, bucket, "app.delete")
	is.Equal(message.Header.Get("Nats-TTL"), "")             // explicit delete TTL should override transaction TTL
	is.True(errors.Is(tx.Put("closed", 1), nkv.ErrTxClosed)) // committed transaction should reject writes
	_, err = tx.Commit(t.Context())
	is.True(errors.Is(err, nkv.ErrTxClosed)) // committed transaction should reject another commit
}

func TestStoreTxSharedByteTransaction(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_TX_SHARED"})
	is.NoErr(err) // bucket creation should succeed
	byteTx := bucket.Tx()
	defer byteTx.Abort()
	options := []nkv.StoreOption{nkv.WithPrefix("store")}
	storeTx := nkv.NewStoreTx(byteTx, options...)
	otherTx := nkv.NewStoreTx(byteTx, nkv.WithPrefix("custom"), nkv.WithWriteCodec(storeTestCodec("application/custom", "custom:")))
	is.NoErr(byteTx.Put("raw", []byte("raw"))) // byte staging should share the same transaction
	is.NoErr(storeTx.Put("number", 123))       // Store staging should use its own prefix
	is.NoErr(otherTx.Create("flag", true))     // another wrapper should use its own codec
	is.Equal(storeTx.Len(), 3)                 // wrapper lengths should include all staged operations
	is.Equal(otherTx.Len(), storeTx.Len())     // Store wrappers should see the same transaction
	result, err := storeTx.Commit(t.Context())
	is.NoErr(err)            // shared transaction should commit
	is.Equal(result.Size, 3) // all wrappers should participate in one atomic commit
	store := nkv.NewStore(bucket)
	value, _, err := store.Get[int](t.Context(), "store.number")
	is.NoErr(err)        // StoreTx's default codec should be JSON
	is.Equal(value, 123) // StoreTx should use its configured prefix
	message := lastRawMessage(t, nc, bucket, "custom.flag")
	is.Equal(string(message.Data), "custom:true") // shared wrappers should encode independently
}

func TestStoreTxValidationAndAbort(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_TX_VALIDATION"})
	is.NoErr(err) // bucket creation should succeed
	tx := nkv.NewStore(bucket).Tx()
	_, err = tx.Commit(t.Context())
	is.True(errors.Is(err, nkv.ErrEmptyTx))                               // empty transactions should report the byte error
	is.NoErr(tx.Put("key", 1))                                            // empty commit should leave the transaction open
	is.True(errors.Is(tx.Create("key", 2), nkv.ErrDuplicateKey))          // duplicate keys should be rejected across operation kinds
	is.True(errors.Is(tx.Update("invalid key", 3, 1), nkv.ErrInvalidKey)) // invalid keys should be rejected before staging
	is.True(errors.Is(tx.Delete("invalid key"), nkv.ErrInvalidKey))       // deletes should preserve key validation
	is.Equal(tx.Len(), 1)                                                 // rejected operations must not change the transaction length
	tx.Abort()
	tx.Abort()
	is.True(errors.Is(tx.Put("new", 2), nkv.ErrTxClosed)) // abort should be idempotent and reject further writes
	is.True(errors.Is(tx.Delete("key"), nkv.ErrTxClosed)) // abort should reject deletes
	_, err = tx.Commit(t.Context())
	is.True(errors.Is(err, nkv.ErrTxClosed)) // abort should reject commit
	_, _, err = nkv.NewStore(bucket).Get[int](t.Context(), "key")
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // aborted staging should not publish anything
}

func TestStoreTxMarshalErrorsDoNotReserveKeys(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_TX_MARSHAL"})
	is.NoErr(err) // bucket creation should succeed
	codecErr := errors.New("cannot marshal")
	codec := nkv.JSONCodec()
	codec.Marshal = func(any) ([]byte, error) { return nil, codecErr }
	byteTx := bucket.Tx()
	defer byteTx.Abort()
	broken := nkv.NewStoreTx(byteTx, nkv.WithWriteCodec(codec))
	is.True(errors.Is(broken.Put("put", 1), codecErr))          // put should preserve marshal failures
	is.True(errors.Is(broken.Create("create", 2), codecErr))    // create should preserve marshal failures
	is.True(errors.Is(broken.Update("update", 3, 1), codecErr)) // update should preserve marshal failures
	is.Equal(broken.Len(), 0)                                   // failed encoding should not stage operations
	working := nkv.NewStoreTx(byteTx)
	for _, key := range []string{"put", "create", "update"} {
		is.NoErr(working.Put(key, 1)) // failed encoding must not reserve a key
	}
	_, err = working.Commit(t.Context())
	is.NoErr(err) // transaction should remain usable after marshal failures
}

func TestStoreTxConflictIsAtomic(t *testing.T) {
	for _, conflictFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "final conflict", true: "staging conflict"}[conflictFirst], func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_TX_CONFLICT"})
			is.NoErr(err) // bucket creation should succeed
			store := nkv.NewStore(bucket)
			_, err = store.Put(t.Context(), "existing", 1)
			is.NoErr(err) // conflict fixture should be written
			tx := store.Tx()
			defer tx.Abort()
			if conflictFirst {
				is.NoErr(tx.Create("existing", "wrong type")) // staging should defer existence checks to commit
			}
			is.NoErr(tx.Put("new", true)) // another value type should stage in the same transaction
			if !conflictFirst {
				is.NoErr(tx.Update("existing", "wrong type", 999)) // stale revision should fail at commit
			}
			_, err = tx.Commit(t.Context())
			is.True(errors.Is(err, nkv.ErrTxConflict)) // commit should preserve the conflict error
			_, _, err = store.Get[bool](t.Context(), "new")
			is.True(errors.Is(err, nkv.ErrKeyNotFound)) // failed atomic commit must not publish the other operation
			value, _, err := store.Get[int](t.Context(), "existing")
			is.NoErr(err)      // original value should remain readable
			is.Equal(value, 1) // failed commit must not alter existing values
		})
	}
}

func TestStoreTxTTLConfiguration(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_TX_TTL"})
	is.NoErr(err) // bucket creation should succeed
	store := nkv.NewStore(bucket, nkv.WithDefaultTTL(5*time.Minute))
	tx := store.Tx()
	defer tx.Abort()
	is.NoErr(tx.Put("value", 1))   // transaction should inherit codec and prefix, not default value TTL
	is.NoErr(tx.Delete("deleted")) // delete should use the bucket's marker TTL
	_, err = tx.Commit(t.Context())
	is.NoErr(err)                                                                     // transaction should commit
	is.Equal(lastRawMessage(t, nc, bucket, "value").Header.Get("Nats-TTL"), "")       // default TTL must not silently change existing transaction behavior
	is.Equal(lastRawMessage(t, nc, bucket, "deleted").Header.Get("Nats-TTL"), "1m0s") // delete should retain marker fallback

	byteTx := bucket.Tx(nkv.WithTxTTL(time.Hour))
	defer byteTx.Abort()
	configured := nkv.NewStoreTx(byteTx,
		nkv.WithDefaultTTL(time.Minute),
		nkv.WithReadCodec(nkv.Codec{}),
		nkv.WithFallbackCodec(nkv.Codec{}),
	)
	is.NoErr(configured.Put("explicit", true)) // read-only options should not affect transaction configuration
	_, err = configured.Commit(t.Context())
	is.NoErr(err)                                                                        // explicit transaction TTL should commit
	is.Equal(lastRawMessage(t, nc, bucket, "explicit").Header.Get("Nats-TTL"), "1h0m0s") // underlying transaction should control TTL
}
