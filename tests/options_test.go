package nkv_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
)

func TestWriteOptions(t *testing.T) {
	customHeaders := func() nats.Header {
		return nats.Header{
			"Content-Type":                        []string{"application/custom"},
			"KV-Operation":                        []string{"INVALID"},
			"Nats-Expected-Last-Subject-Sequence": []string{"999"},
			"Nats-TTL":                            []string{"99h"},
			"Nats-Rollup":                         []string{"all"},
			"X-Custom":                            []string{"one", "two"},
		}
	}

	tests := []struct {
		name            string
		bucket          string
		key             string
		run             func(context.Context, *nkv.Bucket, string, nats.Header) error
		wantOperation   string
		wantRollup      string
		wantExpectedSeq string
	}{
		{name: "put", bucket: "OPTIONS_PUT", key: "put", run: func(ctx context.Context, kv *nkv.Bucket, key string, headers nats.Header) error {
			_, err := kv.Put(ctx, key, []byte("value"), nkv.WithTTL(5*time.Minute), nkv.WithHeaders(headers))
			return err
		}},
		{name: "create", bucket: "OPTIONS_CREATE", key: "create", run: func(ctx context.Context, kv *nkv.Bucket, key string, headers nats.Header) error {
			_, err := kv.Create(ctx, key, []byte("value"), nkv.WithTTL(5*time.Minute), nkv.WithHeaders(headers))
			return err
		}, wantExpectedSeq: "0"},
		{name: "update", bucket: "OPTIONS_UPDATE", key: "update", run: func(ctx context.Context, kv *nkv.Bucket, key string, headers nats.Header) error {
			revision, err := kv.Put(ctx, key, []byte("before"))
			if err != nil {
				return err
			}
			_, err = kv.Update(ctx, key, []byte("value"), revision, nkv.WithTTL(5*time.Minute), nkv.WithHeaders(headers))
			return err
		}, wantExpectedSeq: "1"},
		{name: "delete", bucket: "OPTIONS_DELETE", key: "delete", run: func(ctx context.Context, kv *nkv.Bucket, key string, headers nats.Header) error {
			revision, err := kv.Put(ctx, key, []byte("before"))
			if err != nil {
				return err
			}
			return kv.Delete(ctx, key, nkv.WithRevision(revision), nkv.WithTTL(5*time.Minute), nkv.WithHeaders(headers))
		}, wantOperation: "DEL", wantExpectedSeq: "1"},
		{name: "purge", bucket: "OPTIONS_PURGE", key: "purge", run: func(ctx context.Context, kv *nkv.Bucket, key string, headers nats.Header) error {
			revision, err := kv.Put(ctx, key, []byte("before"))
			if err != nil {
				return err
			}
			return kv.Purge(ctx, key, nkv.WithRevision(revision), nkv.WithTTL(5*time.Minute), nkv.WithHeaders(headers))
		}, wantOperation: "PURGE", wantRollup: "sub", wantExpectedSeq: "1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err)                                                  // bucket creation should succeed
			is.NoErr(test.run(t.Context(), kv, test.key, customHeaders())) // write with options should succeed

			message := lastRawMessage(t, nc, kv, test.key)
			is.Equal(message.Header.Get("Content-Type"), "application/custom")                        // non-reserved custom header should be preserved
			is.Equal(message.Header.Values("X-Custom"), []string{"one", "two"})                       // repeated custom header values should be preserved
			is.Equal(message.Header.Get("Nats-TTL"), "5m0s")                                          // TTL option should override a custom TTL header
			is.Equal(message.Header.Get("KV-Operation"), test.wantOperation)                          // operation header should preserve KV semantics
			is.Equal(message.Header.Get("Nats-Rollup"), test.wantRollup)                              // rollup header should preserve KV semantics
			is.Equal(message.Header.Get("Nats-Expected-Last-Subject-Sequence"), test.wantExpectedSeq) // expected sequence should preserve KV semantics
		})
	}
}

func TestInvalidWriteTTL(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *nkv.Bucket, time.Duration) error
	}{
		{name: "put", run: func(ctx context.Context, kv *nkv.Bucket, ttl time.Duration) error {
			_, err := kv.Put(ctx, "put", nil, nkv.WithTTL(ttl))
			return err
		}},
		{name: "create", run: func(ctx context.Context, kv *nkv.Bucket, ttl time.Duration) error {
			_, err := kv.Create(ctx, "create", nil, nkv.WithTTL(ttl))
			return err
		}},
		{name: "update", run: func(ctx context.Context, kv *nkv.Bucket, ttl time.Duration) error {
			_, err := kv.Update(ctx, "update", nil, 1, nkv.WithTTL(ttl))
			return err
		}},
		{name: "delete", run: func(ctx context.Context, kv *nkv.Bucket, ttl time.Duration) error {
			return kv.Delete(ctx, "delete", nkv.WithTTL(ttl))
		}},
		{name: "purge", run: func(ctx context.Context, kv *nkv.Bucket, ttl time.Duration) error {
			return kv.Purge(ctx, "purge", nkv.WithTTL(ttl))
		}},
	}

	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "OPTIONS_INVALID_TTL"})
	is.New(t).NoErr(err) // bucket creation should succeed

	for _, ttl := range []time.Duration{-time.Second, 500 * time.Millisecond} {
		for _, test := range tests {
			t.Run(test.name+"/"+ttl.String(), func(t *testing.T) {
				err := test.run(t.Context(), kv, ttl)
				is.New(t).True(errors.Is(err, nkv.ErrInvalidOption)) // unsupported TTL should be rejected locally
			})
		}
	}

	typed := nkv.NewGeneric[struct{}](kv, nkv.WithDefaultTTL(500*time.Millisecond))
	_, err = typed.Put(t.Context(), "generic", struct{}{})
	is.New(t).True(errors.Is(err, nkv.ErrInvalidOption)) // invalid default TTL should use the same validation

	tx := kv.Tx()
	err = tx.Delete("tx", nkv.WithTTL(500*time.Millisecond))
	is.New(t).True(errors.Is(err, nkv.ErrInvalidOption)) // invalid transaction delete TTL should be rejected
	is.New(t).Equal(tx.Len(), 0)                         // rejected transaction option should not stage an operation
	is.New(t).NoErr(tx.Delete("tx"))                     // rejected option should not reserve the key
}

func TestPerKeyTTLExpiresValue(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "OPTIONS_TTL_EXPIRY"})
	is.NoErr(err) // bucket creation should succeed

	_, err = kv.Put(t.Context(), "key", []byte("value"), nkv.WithTTL(time.Second))
	is.NoErr(err) // put with per-key TTL should succeed
	_, err = kv.Get(t.Context(), "key")
	is.NoErr(err) // value should be available before its TTL expires

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	expired := false
	for !expired {
		select {
		case <-deadline.C:
			expired = true
		case <-ticker.C:
			_, err = kv.Get(t.Context(), "key")
			if errors.Is(err, nkv.ErrKeyNotFound) {
				expired = true
				deadline.Stop()
			}
		}
	}
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // value should disappear after its per-key TTL

	var marker *nkv.Entry
	markerDeadline := time.NewTimer(2 * time.Second)
	defer markerDeadline.Stop()
	markerTicker := time.NewTicker(20 * time.Millisecond)
	defer markerTicker.Stop()
	for marker == nil {
		for entry, listErr := range kv.List(t.Context(), "key", nkv.WithDeletes()) {
			is.NoErr(listErr) // marker lookup should succeed
			if listErr == nil && entry.Operation == nkv.OpPurge {
				marker = entry
			}
		}
		if marker != nil {
			break
		}

		select {
		case <-markerDeadline.C:
			t.Fatal("timed out waiting for TTL expiry marker")
		case <-markerTicker.C:
		}
	}
	is.Equal(marker.Headers.Get("Nats-Marker-Reason"), "MaxAge") // expiry should leave a server-generated marker
}
