package nkv_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
)

func TestGenericCRUDAndPrefix(t *testing.T) {
	type record struct {
		Name string `json:"name"`
		Rank int    `json:"rank"`
	}

	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "GENERIC_CRUD"})
	is.NoErr(err) // bucket creation should succeed
	typed := nkv.NewGeneric[record](bucket, nkv.WithPrefix("users."))
	is.Equal(typed.Bucket(), bucket) // generic wrapper should expose its bucket

	aliceRevision, err := typed.Put(t.Context(), "alice", record{Name: "Alice", Rank: 1})
	is.NoErr(err)                      // typed put should succeed
	is.Equal(aliceRevision, uint64(1)) // typed put should use the first revision
	alice, revision, err := typed.Get(t.Context(), "alice")
	is.NoErr(err)                                   // typed get should succeed
	is.Equal(alice, record{Name: "Alice", Rank: 1}) // typed get should decode JSON
	is.Equal(revision, aliceRevision)               // typed get should return the stored revision

	bobRevision, err := typed.Create(t.Context(), "bob", record{Name: "Bob", Rank: 2})
	is.NoErr(err)                    // typed create should succeed
	is.Equal(bobRevision, uint64(2)) // typed create should advance the revision
	_, err = typed.Create(t.Context(), "bob", record{Name: "Duplicate"})
	is.True(errors.Is(err, nkv.ErrKeyExists)) // typed create should preserve create conflicts

	aliceRevision, err = typed.Update(t.Context(), "alice", record{Name: "Alice", Rank: 3}, aliceRevision)
	is.NoErr(err)                      // typed update should succeed
	is.Equal(aliceRevision, uint64(3)) // typed update should advance the revision

	_, err = bucket.Get(t.Context(), "alice")
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // prefix should hide the unqualified key
	raw, err := bucket.Get(t.Context(), "users.alice")
	is.NoErr(err)                                 // prefixed raw key should be retrievable
	is.Equal(raw.ContentType, "application/json") // default codec should stamp JSON content type

	names := make([]string, 0)
	for value, err := range typed.List(t.Context(), "*") {
		is.NoErr(err) // typed list should decode each value
		names = append(names, value.Name)
	}
	sort.Strings(names)
	is.Equal(names, []string{"Alice", "Bob"}) // typed list should stay within its prefix
}

func TestGenericCodecDispatchAndFallback(t *testing.T) {
	type record struct {
		Name string
	}

	codec := func(contentType, prefix string) nkv.Codec {
		return nkv.Codec{
			ContentType: contentType,
			Marshal: func(value any) ([]byte, error) {
				record, ok := value.(record)
				if !ok {
					return nil, fmt.Errorf("unexpected type %T", value)
				}
				return []byte(prefix + record.Name), nil
			},
			Unmarshal: func(data []byte, value any) error {
				record, ok := value.(*record)
				if !ok {
					return fmt.Errorf("unexpected target %T", value)
				}
				text := string(data)
				if !strings.HasPrefix(text, prefix) {
					return fmt.Errorf("missing prefix %q", prefix)
				}
				record.Name = strings.TrimPrefix(text, prefix)
				return nil
			},
		}
	}

	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "GENERIC_CODECS"})
	is.NoErr(err) // bucket creation should succeed
	writeCodec := codec("application/x-write", "write:")
	legacyCodec := codec("application/x-legacy", "legacy:")
	fallbackCodec := codec("application/x-fallback", "fallback:")
	typed := nkv.NewGeneric[record](bucket,
		nkv.WithWriteCodec(writeCodec),
		nkv.WithReadCodec(legacyCodec),
		nkv.WithFallbackCodec(fallbackCodec),
	)

	_, err = typed.Put(t.Context(), "write", record{Name: "written"}, nkv.WithHeaders(nats.Header{"Content-Type": []string{"application/x-wrong"}}))
	is.NoErr(err) // typed write should succeed with caller headers
	_, err = bucket.Put(t.Context(), "legacy", []byte("legacy:old"), nkv.WithHeaders(nats.Header{"Content-Type": []string{"application/x-legacy"}}))
	is.NoErr(err) // legacy fixture put should succeed
	_, err = bucket.Put(t.Context(), "headerless", []byte("fallback:none"))
	is.NoErr(err) // headerless fixture put should succeed
	_, err = bucket.Put(t.Context(), "unknown", []byte("fallback:unknown"), nkv.WithHeaders(nats.Header{"Content-Type": []string{"application/x-unknown"}}))
	is.NoErr(err) // unknown codec fixture put should succeed

	tests := []struct {
		name string
		key  string
		want record
	}{
		{name: "registered write codec", key: "write", want: record{Name: "written"}},
		{name: "registered read codec", key: "legacy", want: record{Name: "old"}},
		{name: "headerless fallback", key: "headerless", want: record{Name: "none"}},
		{name: "unknown content type fallback", key: "unknown", want: record{Name: "unknown"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			value, _, err := typed.Get(t.Context(), test.key)
			is.NoErr(err)              // typed get should select a compatible codec
			is.Equal(value, test.want) // selected codec should decode the expected value
		})
	}

	message := lastRawMessage(t, nc, bucket, "write")
	is.Equal(message.Header.Values("Content-Type"), []string{"application/x-write"}) // write codec content type should override caller metadata
}

func TestGenericCodecValidationAndErrors(t *testing.T) {
	type record struct{ Name string }
	jsonCodec := nkv.JSONCodec()
	tests := []struct {
		name       string
		options    []nkv.GenericOption
		errorMatch string
	}{
		{name: "empty write content type", options: []nkv.GenericOption{nkv.WithWriteCodec(nkv.Codec{})}, errorMatch: "codec ContentType must not be empty"},
		{name: "nil write marshal", options: []nkv.GenericOption{nkv.WithWriteCodec(nkv.Codec{ContentType: "application/x-test", Unmarshal: jsonCodec.Unmarshal})}, errorMatch: "codec Marshal must not be nil"},
		{name: "nil write unmarshal", options: []nkv.GenericOption{nkv.WithWriteCodec(nkv.Codec{ContentType: "application/x-test", Marshal: jsonCodec.Marshal})}, errorMatch: "codec Unmarshal must not be nil"},
		{name: "empty read content type", options: []nkv.GenericOption{nkv.WithReadCodec(nkv.Codec{})}, errorMatch: "codec ContentType must not be empty"},
		{name: "nil read marshal", options: []nkv.GenericOption{nkv.WithReadCodec(nkv.Codec{ContentType: "application/x-test", Unmarshal: jsonCodec.Unmarshal})}, errorMatch: "codec Marshal must not be nil"},
		{name: "nil read unmarshal", options: []nkv.GenericOption{nkv.WithReadCodec(nkv.Codec{ContentType: "application/x-test", Marshal: jsonCodec.Marshal})}, errorMatch: "codec Unmarshal must not be nil"},
		{name: "nil fallback marshal", options: []nkv.GenericOption{nkv.WithFallbackCodec(nkv.Codec{ContentType: "application/x-test", Unmarshal: jsonCodec.Unmarshal})}, errorMatch: "codec Marshal must not be nil"},
		{name: "nil fallback unmarshal", options: []nkv.GenericOption{nkv.WithFallbackCodec(nkv.Codec{ContentType: "application/x-test", Marshal: jsonCodec.Marshal})}, errorMatch: "codec Unmarshal must not be nil"},
		{name: "duplicate read content type", options: []nkv.GenericOption{nkv.WithReadCodec(nkv.JSONCodec())}, errorMatch: "duplicate codec"},
	}

	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "GENERIC_VALIDATION"})
	is.New(t).NoErr(err) // bucket creation should succeed
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertPanicContains(t, test.errorMatch, func() {
				_ = nkv.NewGeneric[record](bucket, test.options...)
			})
		})
	}

	codecError := errors.New("codec failure")
	failing := nkv.Codec{
		ContentType: "application/x-failing",
		Marshal: func(any) ([]byte, error) {
			return nil, codecError
		},
		Unmarshal: func([]byte, any) error {
			return codecError
		},
	}
	typed := nkv.NewGeneric[record](bucket, nkv.WithWriteCodec(failing))
	_, err = typed.Put(t.Context(), "marshal", record{Name: "value"})
	is.New(t).True(errors.Is(err, codecError)) // typed put should propagate marshal errors
	_, err = bucket.Put(t.Context(), "unmarshal", []byte("value"), nkv.WithHeaders(nats.Header{"Content-Type": []string{failing.ContentType}}))
	is.New(t).NoErr(err) // unmarshal fixture put should succeed
	_, _, err = typed.Get(t.Context(), "unmarshal")
	is.New(t).True(errors.Is(err, codecError)) // typed get should propagate unmarshal errors
}

func TestGenericPrefixValidation(t *testing.T) {
	type record struct{ Name string }

	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "GENERIC_PREFIX"})
	is.NoErr(err) // bucket creation should succeed

	for _, prefix := range []string{"", ".", ".."} {
		t.Run("empty/"+prefix, func(t *testing.T) {
			typed := nkv.NewGeneric[record](bucket, nkv.WithPrefix(prefix))
			_, err := typed.Put(t.Context(), "key", record{Name: "value"})
			is.New(t).NoErr(err) // empty normalized prefix should leave the key unqualified
		})
	}

	typed := nkv.NewGeneric[record](bucket, nkv.WithPrefix("users..."))
	_, err = typed.Put(t.Context(), "alice", record{Name: "Alice"})
	is.NoErr(err) // repeated trailing dots should be normalized
	_, err = bucket.Get(t.Context(), "users.alice")
	is.NoErr(err) // normalized prefix should use one separator

	for _, prefix := range []string{".users", "users..active", "users.*", "users.>", "users active"} {
		t.Run("invalid/"+prefix, func(t *testing.T) {
			assertPanicContains(t, "invalid prefix", func() {
				_ = nkv.NewGeneric[record](bucket, nkv.WithPrefix(prefix))
			})
			assertPanicContains(t, "invalid prefix", func() {
				_ = nkv.NewGenericTx[record](bucket.Tx(), nkv.WithPrefix(prefix))
			})
		})
	}
}

func TestGenericDefaultTTL(t *testing.T) {
	type record struct{ Name string }
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "GENERIC_TTL"})
	is.NoErr(err) // bucket creation should succeed
	typed := nkv.NewGeneric[record](bucket, nkv.WithDefaultTTL(5*time.Minute))

	tests := []struct {
		name    string
		key     string
		run     func() error
		wantTTL string
	}{
		{name: "put default", key: "put", run: func() error {
			_, err := typed.Put(t.Context(), "put", record{Name: "put"})
			return err
		}, wantTTL: "5m0s"},
		{name: "put override", key: "override", run: func() error {
			_, err := typed.Put(t.Context(), "override", record{Name: "override"}, nkv.WithTTL(time.Hour))
			return err
		}, wantTTL: "1h0m0s"},
		{name: "create default", key: "create", run: func() error {
			_, err := typed.Create(t.Context(), "create", record{Name: "create"})
			return err
		}, wantTTL: "5m0s"},
		{name: "update default", key: "update", run: func() error {
			revision, err := typed.Put(t.Context(), "update", record{Name: "before"})
			if err != nil {
				return err
			}
			_, err = typed.Update(t.Context(), "update", record{Name: "after"}, revision)
			return err
		}, wantTTL: "5m0s"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			is.NoErr(test.run()) // typed write should succeed
			message := lastRawMessage(t, nc, bucket, test.key)
			is.Equal(message.Header.Get("Nats-TTL"), test.wantTTL) // typed write should apply the expected TTL
		})
	}
}

func TestGenericListPinsSnapshotAtCallTime(t *testing.T) {
	type record struct{ Name string }
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "GENERIC_SNAPSHOT"})
	is.NoErr(err) // bucket creation should succeed
	typed := nkv.NewGeneric[record](bucket, nkv.WithPrefix("records"))
	_, err = typed.Put(t.Context(), "before", record{Name: "before"})
	is.NoErr(err) // initial typed value should be stored

	values := typed.List(t.Context(), "*")
	_, err = typed.Put(t.Context(), "after", record{Name: "after"})
	is.NoErr(err) // value after list creation should be stored

	names := make([]string, 0)
	for value, err := range values {
		is.NoErr(err) // typed snapshot iteration should not fail
		names = append(names, value.Name)
	}
	is.Equal(names, []string{"before"}) // typed list should exclude writes after the call
}

func TestGenericKeys(t *testing.T) {
	tests := []struct {
		name    string
		bucket  string
		prefix  string
		pattern string
		keys    []string
		want    []string
	}{
		{
			name:    "prefixed",
			bucket:  "GENERIC_KEYS_PREFIXED",
			prefix:  "users",
			pattern: "*",
			keys:    []string{"alice", "bob"},
			want:    []string{"alice", "bob"},
		},
		{
			name:    "pattern",
			bucket:  "GENERIC_KEYS_PATTERN",
			pattern: "users.*",
			keys:    []string{"users.alice", "config.region"},
			want:    []string{"users.alice"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed
			typed := nkv.NewGeneric[string](bucket, nkv.WithPrefix(test.prefix))

			for _, key := range test.keys {
				_, err := typed.Put(t.Context(), key, key)
				is.NoErr(err) // typed fixture put should succeed
			}

			keys := make([]string, 0)
			for key, err := range typed.Keys(t.Context(), test.pattern, nkv.WithListBatch(1)) {
				is.NoErr(err) // typed key iteration should succeed
				keys = append(keys, key)
			}
			sort.Strings(keys)
			is.Equal(keys, test.want) // keys should match the pattern without the configured prefix
		})
	}
}

func TestGenericDeleteAndPurge(t *testing.T) {
	type record struct{ Name string }
	tests := []struct {
		name          string
		bucket        string
		run           func(context.Context, *nkv.Generic[record], uint64) error
		wantOperation nkv.Operation
		wantTTL       string
	}{
		{
			name:   "delete",
			bucket: "GENERIC_DELETE",
			run: func(ctx context.Context, typed *nkv.Generic[record], revision uint64) error {
				return typed.Delete(ctx, "alice", nkv.WithRevision(revision), nkv.WithTTL(time.Minute))
			},
			wantOperation: nkv.OpDelete,
			wantTTL:       "1m0s",
		},
		{
			name:   "purge",
			bucket: "GENERIC_PURGE",
			run: func(ctx context.Context, typed *nkv.Generic[record], _ uint64) error {
				return typed.Purge(ctx, "alice", nkv.WithTTL(2*time.Minute))
			},
			wantOperation: nkv.OpPurge,
			wantTTL:       "2m0s",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed
			typed := nkv.NewGeneric[record](bucket, nkv.WithPrefix("users"))
			revision, err := typed.Put(t.Context(), "alice", record{Name: "Alice"})
			is.NoErr(err) // typed fixture put should succeed
			watcher, err := typed.Watch(t.Context(), "*", nkv.WithUpdatesOnly())
			is.NoErr(err) // typed watch creation should succeed
			t.Cleanup(watcher.Stop)

			is.NoErr(test.run(t.Context(), typed, revision)) // typed removal should succeed
			entry, err := watcher.Next()
			is.NoErr(err)                                 // typed watch should deliver the tombstone
			is.Equal(entry.Key, "alice")                  // typed watch should remove the configured prefix
			is.Equal(entry.Operation, test.wantOperation) // typed watch should preserve operation metadata
			is.Equal(entry.Value, record{})               // tombstones should have the typed zero value
			message := lastRawMessage(t, nc, bucket, "users.alice")
			is.Equal(message.Header.Get("Nats-TTL"), test.wantTTL) // removal options should reach the bucket
			_, err = bucket.Get(t.Context(), "alice")
			is.True(errors.Is(err, nkv.ErrKeyNotFound)) // typed removal should not affect an unprefixed key
		})
	}
}

func TestGenericWatch(t *testing.T) {
	type record struct{ Name string }
	tests := []struct {
		name         string
		bucket       string
		options      []nkv.WatchOption
		before       map[string]record
		after        map[string]record
		want         map[string]record
		wantSentinel bool
		wantMetaOnly bool
	}{
		{
			name:         "decoded replay and additional filters",
			bucket:       "GENERIC_WATCH_REPLAY",
			options:      []nkv.WatchOption{nkv.WithAdditionalKeys("config.*"), nkv.WithPullBatch(1)},
			before:       map[string]record{"users.alice": {Name: "Alice"}, "config.theme": {Name: "Dark"}, "ignored.key": {Name: "Ignored"}},
			want:         map[string]record{"users.alice": {Name: "Alice"}, "config.theme": {Name: "Dark"}},
			wantSentinel: true,
		},
		{
			name:         "metadata-only live update",
			bucket:       "GENERIC_WATCH_META",
			options:      []nkv.WatchOption{nkv.WithUpdatesOnly(), nkv.WithMetaOnly()},
			after:        map[string]record{"users.alice": {Name: "Alice"}},
			want:         map[string]record{"users.alice": {}},
			wantMetaOnly: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			nc := testConnection(t)
			bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: test.bucket})
			is.NoErr(err) // bucket creation should succeed
			typed := nkv.NewGeneric[record](bucket, nkv.WithPrefix("scope"))
			for key, value := range test.before {
				_, err := typed.Put(t.Context(), key, value)
				is.NoErr(err) // replay fixture put should succeed
			}

			watcher, err := typed.Watch(t.Context(), "users.*", test.options...)
			is.NoErr(err) // typed watch creation should succeed
			t.Cleanup(watcher.Stop)
			for key, value := range test.after {
				_, err := typed.Put(t.Context(), key, value)
				is.NoErr(err) // live fixture put should succeed
			}

			got := make(map[string]record, len(test.want))
			for range test.want {
				entry, err := watcher.Next()
				is.NoErr(err)                        // typed watch delivery should succeed
				is.Equal(entry.Operation, nkv.OpPut) // typed watch should preserve put metadata
				if test.wantMetaOnly {
					is.Equal(len(entry.Entry.Value), 0) // metadata-only watch should omit encoded bytes
				}
				got[entry.Key] = entry.Value
			}
			is.Equal(got, test.want) // typed watch should decode only matching entries

			if test.wantSentinel {
				entry, err := watcher.Next()
				is.NoErr(err)                  // replay sentinel should not fail
				is.True(entry == nil)          // typed watcher should preserve the replay sentinel
				is.True(watcher.InitialDone()) // typed watcher should expose replay state
			}
		})
	}
}
