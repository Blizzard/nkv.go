package nkv_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
)

func TestTxStagingValidationAndAbort(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "TX_VALIDATION"})
	is.NoErr(err) // bucket creation should succeed

	invalid := []struct {
		name string
		run  func(*nkv.Tx) error
	}{
		{name: "put", run: func(tx *nkv.Tx) error { return tx.Put(".invalid", nil) }},
		{name: "create", run: func(tx *nkv.Tx) error { return tx.Create(".invalid", nil) }},
		{name: "update", run: func(tx *nkv.Tx) error { return tx.Update(".invalid", nil, 1) }},
		{name: "delete", run: func(tx *nkv.Tx) error { return tx.Delete(".invalid") }},
		{name: "purge", run: func(tx *nkv.Tx) error { return tx.Purge(".invalid") }},
	}
	for _, test := range invalid {
		t.Run("invalid "+test.name, func(t *testing.T) {
			is := is.New(t)
			tx := bucket.Tx()
			is.True(errors.Is(test.run(tx), nkv.ErrInvalidKey)) // staging should reject an invalid key
			is.Equal(tx.Len(), 0)                               // rejected operation should not change transaction length
		})
	}

	tx := bucket.Tx()
	is.NoErr(tx.Put("key", []byte("value")))                  // first staged key should succeed
	is.True(errors.Is(tx.Delete("key"), nkv.ErrDuplicateKey)) // duplicate staged key should be rejected across operations
	is.Equal(tx.Len(), 1)                                     // duplicate operation should not change transaction length
	tx.Abort()
	tx.Abort()
	is.True(errors.Is(tx.Put("after", nil), nkv.ErrTxClosed)) // aborted transaction should reject more operations
	_, err = tx.Commit(t.Context())
	is.True(errors.Is(err, nkv.ErrTxClosed)) // aborted transaction should reject commit
	_, err = bucket.Get(t.Context(), "key")
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // aborted transaction should not write staged values

	empty := bucket.Tx()
	_, err = empty.Commit(t.Context())
	is.True(errors.Is(err, nkv.ErrEmptyTx)) // empty transaction should reject commit
}

func TestTxCommitAllOperations(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "TX_COMMIT"})
	is.NoErr(err) // bucket creation should succeed

	updateRevision, err := bucket.Put(t.Context(), "update", []byte("before-update"))
	is.NoErr(err) // update fixture put should succeed
	deleteRevision, err := bucket.Put(t.Context(), "delete", []byte("before-delete"))
	is.NoErr(err) // delete fixture put should succeed
	_, err = bucket.Put(t.Context(), "purge", []byte("before-purge"))
	is.NoErr(err) // purge fixture put should succeed

	tx := bucket.Tx()
	is.NoErr(tx.Put("put", []byte("put-value")))                          // transaction put should stage
	is.NoErr(tx.Create("create", []byte("create-value")))                 // transaction create should stage
	is.NoErr(tx.Update("update", []byte("after-update"), updateRevision)) // transaction update should stage
	is.NoErr(tx.Delete("delete", nkv.WithRevision(deleteRevision)))       // transaction delete should stage
	is.NoErr(tx.Purge("purge"))                                           // transaction purge should stage
	is.Equal(tx.Len(), 5)                                                 // transaction should report all staged operations

	result, err := tx.Commit(t.Context())
	is.NoErr(err)                        // transaction commit should succeed
	is.True(result.ID != "")             // transaction result should include an ID
	is.Equal(result.Size, 5)             // transaction result should include its size
	is.Equal(result.Sequence, uint64(8)) // transaction result should include the final stream sequence

	values := map[string]string{
		"put":    "put-value",
		"create": "create-value",
		"update": "after-update",
	}
	for key, want := range values {
		entry, err := bucket.Get(t.Context(), key)
		is.NoErr(err)                       // committed live key should be retrievable
		is.Equal(string(entry.Value), want) // committed live key should contain its staged value
	}
	for _, key := range []string{"delete", "purge"} {
		_, err := bucket.Get(t.Context(), key)
		is.True(errors.Is(err, nkv.ErrKeyNotFound)) // committed tombstone key should not be found
	}

	is.True(errors.Is(tx.Put("closed", nil), nkv.ErrTxClosed)) // committed transaction should reject more operations
	_, err = tx.Commit(t.Context())
	is.True(errors.Is(err, nkv.ErrTxClosed)) // committed transaction should reject another commit
}

func TestTxDeleteOptions(t *testing.T) {
	tests := []struct {
		name       string
		bucket     string
		txTTL      time.Duration
		options    []nkv.DeleteOption
		wantTTL    string
		wantCustom string
	}{
		{
			name:   "headers and TTL",
			bucket: "TX_DELETE_OPTIONS",
			txTTL:  10 * time.Minute,
			options: []nkv.DeleteOption{
				nkv.WithTTL(5 * time.Minute),
				nkv.WithHeaders(nats.Header{
					"X-Custom":                            []string{"preserved"},
					"KV-Operation":                        []string{"PURGE"},
					"Nats-Expected-Last-Subject-Sequence": []string{"999"},
					"Nats-TTL":                            []string{"99h"},
					"Nats-Batch-Id":                       []string{"invalid"},
				}),
			},
			wantTTL:    "5m0s",
			wantCustom: "preserved",
		},
		{
			name:    "transaction TTL default",
			bucket:  "TX_DELETE_DEFAULT_TTL",
			txTTL:   10 * time.Minute,
			wantTTL: "10m0s",
		},
		{
			name:    "zero disables transaction TTL",
			bucket:  "TX_DELETE_ZERO_TTL",
			txTTL:   10 * time.Minute,
			options: []nkv.DeleteOption{nkv.WithTTL(0)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed
			revision, err := bucket.Put(t.Context(), "key", []byte("value"))
			is.NoErr(err) // delete fixture put should succeed

			tx := bucket.Tx(nkv.WithTxTTL(test.txTTL))
			options := append([]nkv.DeleteOption{nkv.WithRevision(revision)}, test.options...)
			is.NoErr(tx.Delete("key", options...)) // transaction delete with options should stage
			_, err = tx.Commit(t.Context())
			is.NoErr(err) // transaction delete should commit

			message := lastRawMessage(t, nc, bucket, "key")
			is.Equal(message.Header.Get("X-Custom"), test.wantCustom)                                             // transaction delete should preserve custom headers
			is.Equal(message.Header.Get("Nats-TTL"), test.wantTTL)                                                // per-delete TTL should override the transaction default
			is.Equal(message.Header.Get("KV-Operation"), "DEL")                                                   // reserved operation header should retain delete semantics
			is.Equal(message.Header.Get("Nats-Expected-Last-Subject-Sequence"), strconv.FormatUint(revision, 10)) // reserved revision header should retain CAS semantics
			is.True(message.Header.Get("Nats-Batch-Id") != "invalid")                                             // reserved batch ID should not be user-controlled
		})
	}
}

func TestTxConflictRollsBack(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		stage  func(*nkv.Tx) error
	}{
		{name: "intermediate conflict", bucket: "TX_CONFLICT_INTERMEDIATE", stage: func(tx *nkv.Tx) error {
			if err := tx.Update("guard", []byte("changed"), 99); err != nil {
				return err
			}
			return tx.Put("new", []byte("new"))
		}},
		{name: "final conflict", bucket: "TX_CONFLICT_FINAL", stage: func(tx *nkv.Tx) error {
			if err := tx.Put("new", []byte("new")); err != nil {
				return err
			}
			return tx.Update("guard", []byte("changed"), 99)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed
			_, err = bucket.Put(t.Context(), "guard", []byte("original"))
			is.NoErr(err) // conflict fixture put should succeed

			tx := bucket.Tx()
			is.NoErr(test.stage(tx)) // conflicting transaction should stage locally
			_, err = tx.Commit(t.Context())
			is.True(errors.Is(err, nkv.ErrTxConflict)) // CAS conflict should fail the transaction

			guard, err := bucket.Get(t.Context(), "guard")
			is.NoErr(err)                             // conflicted guard key should remain available
			is.Equal(string(guard.Value), "original") // conflict should preserve the original guard value
			_, err = bucket.Get(t.Context(), "new")
			is.True(errors.Is(err, nkv.ErrKeyNotFound)) // conflict should roll back every staged key
		})
	}
}

func TestTxCanceledContextRollsBack(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "TX_CANCELED"})
	is.NoErr(err) // bucket creation should succeed
	tx := bucket.Tx()
	is.NoErr(tx.Put("one", []byte("one"))) // first transaction put should stage
	is.NoErr(tx.Put("two", []byte("two"))) // second transaction put should stage

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = tx.Commit(ctx)
	is.True(errors.Is(err, context.Canceled)) // transaction should return context cancellation
	for _, key := range []string{"one", "two"} {
		_, err := bucket.Get(t.Context(), key)
		is.True(errors.Is(err, nkv.ErrKeyNotFound)) // canceled transaction should not write staged values
	}
	is.True(errors.Is(tx.Put("closed", nil), nkv.ErrTxClosed)) // failed commit should close the transaction
}

func TestTxTTL(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "TX_TTL"})
	is.NoErr(err) // bucket creation should succeed
	tx := bucket.Tx(nkv.WithTxTTL(100 * time.Millisecond))
	is.NoErr(tx.Put("one", []byte("one"))) // first TTL transaction put should stage
	is.NoErr(tx.Put("two", []byte("two"))) // second TTL transaction put should stage
	_, err = tx.Commit(t.Context())
	is.NoErr(err) // TTL transaction should commit

	for _, key := range []string{"one", "two"} {
		message := lastRawMessage(t, nc, bucket, key)
		is.Equal(message.Header.Get("Nats-TTL"), "1s") // transaction TTL should clamp to the server minimum
	}
}

func TestGenericTx(t *testing.T) {
	type user struct {
		Name string `json:"name"`
	}
	type audit struct {
		Action string `json:"action"`
	}

	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "TX_GENERIC"})
	is.NoErr(err) // bucket creation should succeed
	tx := bucket.Tx()
	users := nkv.NewGenericTx[user](tx, nkv.WithPrefix("users"))
	audits := nkv.NewGenericTx[audit](tx, nkv.WithPrefix("audit"))
	is.NoErr(users.Put("alice", user{Name: "Alice"}))        // typed user put should stage
	is.NoErr(audits.Create("login", audit{Action: "login"})) // typed audit create should stage
	is.Equal(users.Len(), 2)                                 // typed wrappers should share transaction length
	result, err := users.Commit(t.Context())
	is.NoErr(err)            // shared typed transaction should commit
	is.Equal(result.Size, 2) // typed transaction should report both operations

	typedUsers := nkv.NewGeneric[user](bucket, nkv.WithPrefix("users"))
	typedAudits := nkv.NewGeneric[audit](bucket, nkv.WithPrefix("audit"))
	alice, _, err := typedUsers.Get(t.Context(), "alice")
	is.NoErr(err)                        // typed user should be readable after commit
	is.Equal(alice, user{Name: "Alice"}) // typed user should preserve its value
	login, _, err := typedAudits.Get(t.Context(), "login")
	is.NoErr(err)                           // typed audit should be readable after commit
	is.Equal(login, audit{Action: "login"}) // typed audit should preserve its value

	message := lastRawMessage(t, nc, bucket, "users.alice")
	is.Equal(message.Header.Get("Content-Type"), "application/json") // typed transaction should stamp its codec content type

	inherited := typedUsers.Tx()
	is.NoErr(inherited.Put("bob", user{Name: "Bob"})) // Generic.Tx put should stage with inherited configuration
	_, err = inherited.Commit(t.Context())
	is.NoErr(err) // Generic.Tx should commit
	bob, _, err := typedUsers.Get(t.Context(), "bob")
	is.NoErr(err)                    // inherited-prefix value should be readable
	is.Equal(bob, user{Name: "Bob"}) // Generic.Tx should inherit codec and prefix
}

func TestGenericTxCodecValidation(t *testing.T) {
	jsonCodec := nkv.JSONCodec()
	tests := []struct {
		name       string
		codec      nkv.Codec
		errorMatch string
	}{
		{name: "empty content type", codec: nkv.Codec{}, errorMatch: "codec ContentType must not be empty"},
		{name: "nil marshal", codec: nkv.Codec{ContentType: "application/x-test", Unmarshal: jsonCodec.Unmarshal}, errorMatch: "codec Marshal must not be nil"},
		{name: "nil unmarshal", codec: nkv.Codec{ContentType: "application/x-test", Marshal: jsonCodec.Marshal}, errorMatch: "codec Unmarshal must not be nil"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertPanicContains(t, test.errorMatch, func() {
				_ = nkv.NewGenericTx[struct{}](nil, nkv.WithWriteCodec(test.codec))
			})
		})
	}
}
