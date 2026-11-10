package nkv_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTxRejectsStreamExpectationHeaders(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "TX_HEADER_VALIDATION"})
	is.NoErr(err) // bucket creation should succeed

	headers := []struct {
		name  string
		key   string
		value string
	}{
		{name: "sequence", key: jetstream.ExpectedLastSeqHeader, value: "0"},
		{name: "lowercase sequence", key: strings.ToLower(jetstream.ExpectedLastSeqHeader), value: "0"},
		{name: "uppercase sequence", key: strings.ToUpper(jetstream.ExpectedLastSeqHeader), value: "0"},
		{name: "empty sequence", key: jetstream.ExpectedLastSeqHeader},
		{name: "message ID", key: jetstream.ExpectedLastMsgIDHeader, value: "previous"},
		{name: "lowercase message ID", key: strings.ToLower(jetstream.ExpectedLastMsgIDHeader), value: "previous"},
		{name: "uppercase message ID", key: strings.ToUpper(jetstream.ExpectedLastMsgIDHeader), value: "previous"},
		{name: "empty message ID", key: jetstream.ExpectedLastMsgIDHeader},
	}

	positions := []struct {
		name     string
		priorPut bool
	}{
		{name: "first operation"},
		{name: "after put", priorPut: true},
	}

	for _, header := range headers {
		for _, position := range positions {
			t.Run(header.name+"/"+position.name, func(t *testing.T) {
				is := is.New(t)

				revision, err := bucket.Put(t.Context(), "key", []byte("before"))
				is.NoErr(err) // revision-guarded delete fixture should be written

				tx := bucket.Tx()
				defer tx.Abort()

				if position.priorPut {
					is.NoErr(tx.Put("other", []byte("value"))) // preceding operation should stage normally
				}

				staged := tx.Len()
				err = tx.Delete("key", nkv.WithHeaders(nats.Header{header.key: {header.value}}))
				is.True(errors.Is(err, nkv.ErrInvalidOption)) // stream-wide expectations must fail during staging
				is.Equal(tx.Len(), staged)                    // rejected options must not add an operation

				// Retrying the same key proves rejection did not reserve it.
				// A valueless header is not transmitted; similarly named metadata
				// must not be mistaken for a protocol expectation.
				is.NoErr(tx.Delete("key",
					nkv.WithRevision(revision),
					nkv.WithTTL(0),
					nkv.WithHeaders(nats.Header{
						jetstream.ExpectedLastSeqHeader:   nil,
						jetstream.ExpectedLastMsgIDHeader: nil,
						"X-Nats-Expected-Last-Sequence":   {"metadata"},
					}),
				)) // key-level CAS and unrelated headers should still stage

				result, err := tx.Commit(t.Context())
				is.NoErr(err)                   // rejected options must leave the transaction usable
				is.Equal(result.Size, staged+1) // only accepted operations should be committed

				_, err = bucket.Get(t.Context(), "key")
				is.True(errors.Is(err, nkv.ErrKeyNotFound)) // the accepted CAS delete should be committed

				message := lastRawMessage(t, nc, bucket, "key")
				is.Equal(message.Header.Get("X-Nats-Expected-Last-Sequence"), "metadata") // similarly named metadata should be preserved
			})
		}
	}
}

func TestPutPreservesStreamSequenceExpectations(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "PUT_STREAM_EXPECTATION"})
	is.NoErr(err) // bucket creation should succeed

	_, err = bucket.Put(t.Context(), "existing", []byte("value"))
	is.NoErr(err) // stream must have a nonzero sequence before the conditional put

	_, err = bucket.Put(t.Context(), "blocked", []byte("value"), nkv.WithHeaders(nats.Header{
		jetstream.ExpectedLastSeqHeader: {"0"},
	}))

	var apiErr *jetstream.APIError
	is.True(errors.As(err, &apiErr))                                       // ordinary writes should still pass stream expectations to the server
	is.Equal(apiErr.ErrorCode, jetstream.JSErrCodeStreamWrongLastSequence) // the server should reject the stale expectation

	_, err = bucket.Get(t.Context(), "blocked")
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // the stream expectation must not be silently discarded
}
