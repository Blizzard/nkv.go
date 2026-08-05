package nkv_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamKVInteroperability(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
	}{
		{name: "bucket created by nkv", bucket: "INTEROP_NKV"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			js, err := jetstream.New(nc)
			is.NoErr(err) // JetStream client creation should succeed

			modern, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // nkv should create the bucket
			standard, err := js.KeyValue(t.Context(), test.bucket)
			is.NoErr(err) // standard client should open an nkv bucket

			modernRevision, err := modern.Put(t.Context(), "modern", []byte("nkv-one"))
			is.NoErr(err)                       // nkv put should succeed
			is.Equal(modernRevision, uint64(1)) // nkv put should use the first stream revision
			standardEntry, err := standard.Get(t.Context(), "modern")
			is.NoErr(err)                                      // standard client should read an nkv value
			is.Equal(string(standardEntry.Value()), "nkv-one") // standard client should receive the nkv value
			is.Equal(standardEntry.Revision(), modernRevision) // standard client should receive the nkv revision

			standardRevision, err := standard.Put(t.Context(), "standard", []byte("js-one"))
			is.NoErr(err)                         // standard client put should succeed
			is.Equal(standardRevision, uint64(2)) // standard put should use the next stream revision
			modernEntry, err := modern.Get(t.Context(), "standard")
			is.NoErr(err)                                    // nkv should read a standard client value
			is.Equal(string(modernEntry.Value), "js-one")    // nkv should receive the standard client value
			is.Equal(modernEntry.Revision, standardRevision) // nkv should receive the standard client revision

			modernRevision, err = standard.Update(t.Context(), "modern", []byte("js-two"), modernRevision)
			is.NoErr(err)                       // standard client should update an nkv value
			is.Equal(modernRevision, uint64(3)) // standard update should advance the stream revision
			modernEntry, err = modern.Get(t.Context(), "modern")
			is.NoErr(err)                                 // nkv should read the standard client update
			is.Equal(string(modernEntry.Value), "js-two") // nkv should receive the updated standard value

			standardRevision, err = modern.Update(t.Context(), "standard", []byte("nkv-two"), standardRevision)
			is.NoErr(err)                         // nkv should update a standard client value
			is.Equal(standardRevision, uint64(4)) // nkv update should advance the stream revision
			standardEntry, err = standard.Get(t.Context(), "standard")
			is.NoErr(err)                                      // standard client should read the nkv update
			is.Equal(string(standardEntry.Value()), "nkv-two") // standard client should receive the updated nkv value

			is.NoErr(modern.Delete(t.Context(), "modern")) // nkv delete should succeed
			_, err = standard.Get(t.Context(), "modern")
			is.True(errors.Is(err, jetstream.ErrKeyNotFound)) // standard client should treat an nkv tombstone as missing

			is.NoErr(standard.Delete(t.Context(), "standard")) // standard client delete should succeed
			_, err = modern.Get(t.Context(), "standard")
			is.True(errors.Is(err, nkv.ErrKeyNotFound)) // nkv should treat a standard tombstone as missing
		})
	}
}

func TestNKVWriteWireFormat(t *testing.T) {
	tests := []struct {
		name            string
		bucket          string
		key             string
		run             func(context.Context, *nkv.Bucket, string) error
		wantData        string
		wantOperation   string
		wantRollup      string
		wantExpectedSeq string
		wantSequence    uint64
		wantTraceID     string
	}{
		{
			name:   "put",
			bucket: "WIRE_PUT",
			key:    "put.key",
			run: func(ctx context.Context, kv *nkv.Bucket, key string) error {
				_, err := kv.Put(ctx, key, []byte("put-value"), nkv.WithHeaders(nats.Header{"X-Trace-ID": []string{"trace-1"}}))
				return err
			},
			wantData:     "put-value",
			wantSequence: 1,
			wantTraceID:  "trace-1",
		},
		{
			name:   "create",
			bucket: "WIRE_CREATE",
			key:    "create.key",
			run: func(ctx context.Context, kv *nkv.Bucket, key string) error {
				_, err := kv.Create(ctx, key, []byte("create-value"))
				return err
			},
			wantData:        "create-value",
			wantExpectedSeq: "0",
			wantSequence:    1,
		},
		{
			name:   "update",
			bucket: "WIRE_UPDATE",
			key:    "update.key",
			run: func(ctx context.Context, kv *nkv.Bucket, key string) error {
				revision, err := kv.Put(ctx, key, []byte("before"))
				if err != nil {
					return err
				}
				_, err = kv.Update(ctx, key, []byte("after"), revision)
				return err
			},
			wantData:        "after",
			wantExpectedSeq: "1",
			wantSequence:    2,
		},
		{
			name:   "delete",
			bucket: "WIRE_DELETE",
			key:    "delete.key",
			run: func(ctx context.Context, kv *nkv.Bucket, key string) error {
				revision, err := kv.Put(ctx, key, []byte("before"))
				if err != nil {
					return err
				}
				return kv.Delete(ctx, key, nkv.WithRevision(revision))
			},
			wantOperation:   "DEL",
			wantExpectedSeq: "1",
			wantSequence:    2,
		},
		{
			name:   "purge",
			bucket: "WIRE_PURGE",
			key:    "purge.key",
			run: func(ctx context.Context, kv *nkv.Bucket, key string) error {
				if _, err := kv.Put(ctx, key, []byte("before")); err != nil {
					return err
				}
				return kv.Purge(ctx, key)
			},
			wantOperation: "PURGE",
			wantRollup:    "sub",
			wantSequence:  2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err)                                 // bucket creation should succeed
			is.NoErr(test.run(t.Context(), kv, test.key)) // nkv operation should succeed

			js, err := jetstream.New(nc)
			is.NoErr(err) // JetStream client creation should succeed
			stream, err := js.Stream(t.Context(), kv.Stream())
			is.NoErr(err) // backing stream should be available
			message, err := stream.GetLastMsgForSubject(t.Context(), "$KV."+test.bucket+"."+test.key)
			is.NoErr(err) // raw KV message should be retrievable by its wire subject

			is.Equal(message.Subject, "$KV."+test.bucket+"."+test.key)                                // message should use the standard KV subject layout
			is.Equal(message.Sequence, test.wantSequence)                                             // message should have the expected stream sequence
			is.Equal(string(message.Data), test.wantData)                                             // message should contain the expected payload
			is.Equal(message.Header.Get("KV-Operation"), test.wantOperation)                          // message should use the standard KV operation header
			is.Equal(message.Header.Get("Nats-Rollup"), test.wantRollup)                              // message should use the standard rollup header
			is.Equal(message.Header.Get("Nats-Expected-Last-Subject-Sequence"), test.wantExpectedSeq) // message should use the standard CAS header
			is.Equal(message.Header.Get("X-Trace-ID"), test.wantTraceID)                              // message should preserve custom headers
		})
	}
}
