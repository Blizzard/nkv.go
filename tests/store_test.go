package nkv_test

import (
	"context"
	"encoding/json"
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

func TestStoreMixedTypes(t *testing.T) {
	type user struct {
		Name string `json:"name"`
	}
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_CRUD"})
	is.NoErr(err) // bucket creation should succeed
	store := nkv.NewStore(bucket, nkv.WithPrefix("app..."))
	is.Equal(store.Bucket(), bucket) // Store should retain the byte bucket

	revision, err := store.Put(t.Context(), "user", user{Name: "Tom"})
	is.NoErr(err) // write type should be inferred from its argument
	_, err = store.Create[int](t.Context(), "count", 42)
	is.NoErr(err) // explicit write type arguments should also work
	_, err = store.Create(t.Context(), "count", 43)
	is.True(errors.Is(err, nkv.ErrKeyExists)) // Store should preserve create conflicts
	value, readRevision, err := store.Get[user](t.Context(), "user", nkv.WithRevision(revision))
	is.NoErr(err)                      // Store should forward read options
	is.Equal(value, user{Name: "Tom"}) // Store should decode the selected type
	is.Equal(readRevision, revision)   // typed reads should retain the revision
	count, _, err := store.Get[int](t.Context(), "count")
	is.NoErr(err)       // one Store should read different types
	is.Equal(count, 42) // primitive values should round trip
	_, err = store.Update(t.Context(), "user", user{Name: "Updated"}, revision)
	is.NoErr(err) // Store should preserve CAS writes
	_, err = store.Update(t.Context(), "user", user{Name: "Stale"}, revision)
	is.True(errors.Is(err, nkv.ErrRevisionMismatch)) // stale CAS should fail

	_, err = store.Put(t.Context(), "pointer", &user{Name: "Pointer"})
	is.NoErr(err) // pointer write arguments should work
	pointer, _, err := store.Get[*user](t.Context(), "pointer")
	is.NoErr(err)                             // pointer decode targets should work
	is.Equal(pointer, &user{Name: "Pointer"}) // pointer values should round trip
	_, err = store.Put[*user](t.Context(), "nil", nil)
	is.NoErr(err) // explicitly typed nil values should encode
	pointer, _, err = store.Get[*user](t.Context(), "nil")
	is.NoErr(err)           // JSON null should decode
	is.True(pointer == nil) // typed nil should round trip
	raw, err := bucket.Get(t.Context(), "app.user")
	is.NoErr(err)                                                // byte reads should remain interoperable
	is.Equal(raw.ContentType, "application/json")                // Store should stamp the write codec
	is.Equal(string(raw.Value), `{"name":"Updated"}`)            // wire values should remain standard JSON
	is.NoErr(store.Delete(t.Context(), "count", nkv.WithTTL(0))) // deletes should use the Store prefix
	count, readRevision, err = store.Get[int](t.Context(), "count")
	is.True(errors.Is(err, nkv.ErrKeyNotFound)) // missing values should preserve the byte error
	is.Equal(count, 0)                          // failed reads should return a zero value
	is.Equal(readRevision, uint64(0))           // failed reads should return a zero revision
}

func TestStoreCodecDispatch(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_CODECS"})
	is.NoErr(err) // bucket creation should succeed
	write := storeTestCodec("application/x-write", "write:")
	read := storeTestCodec("application/x-read", "read:")
	fallback := storeTestCodec("application/x-fallback", "fallback:")
	store := nkv.NewStore(bucket, nkv.WithWriteCodec(write), nkv.WithReadCodec(read), nkv.WithFallbackCodec(fallback))

	_, err = store.Put(t.Context(), "write", "value", nkv.WithHeaders(nats.Header{
		"Content-Type": {"application/wrong"},
		"X-Custom":     {"kept"},
	}))
	is.NoErr(err) // custom codec write should succeed
	message := lastRawMessage(t, nc, bucket, "write")
	is.Equal(message.Header.Values("Content-Type"), []string{write.ContentType}) // codec should override caller content type
	is.Equal(message.Header.Get("X-Custom"), "kept")                             // unrelated headers should survive

	for _, test := range []struct {
		key         string
		contentType string
		data        string
	}{
		{key: "read", contentType: read.ContentType, data: `read:"value"`},
		{key: "headerless", data: `fallback:"value"`},
		{key: "unknown", contentType: "application/unknown", data: `fallback:"value"`},
	} {
		_, err := bucket.Put(t.Context(), test.key, []byte(test.data), nkv.WithHeaders(nats.Header{"Content-Type": {test.contentType}}))
		is.NoErr(err) // fixture write should succeed
	}
	for _, key := range []string{"write", "read", "headerless", "unknown"} {
		value, _, err := store.Get[string](t.Context(), key)
		is.NoErr(err)            // content type should select the expected codec or fallback
		is.Equal(value, "value") // every codec should decode its fixture
	}

	defaultFallback := nkv.NewStore(bucket, nkv.WithWriteCodec(write))
	_, err = bucket.Put(t.Context(), "default", []byte(`write:123`))
	is.NoErr(err) // headerless fixture should be written
	value, _, err := defaultFallback.Get[int](t.Context(), "default")
	is.NoErr(err)        // write codec should be the default fallback
	is.Equal(value, 123) // fallback should not fix the Go value type
}

func TestStoreOptionValidation(t *testing.T) {
	jsonCodec := nkv.JSONCodec()
	for _, test := range []struct {
		name  string
		codec nkv.Codec
		match string
	}{
		{name: "content type", codec: nkv.Codec{}, match: "ContentType must not be empty"},
		{name: "marshal", codec: nkv.Codec{ContentType: "application/test", Unmarshal: jsonCodec.Unmarshal}, match: "Marshal must not be nil"},
		{name: "unmarshal", codec: nkv.Codec{ContentType: "application/test", Marshal: jsonCodec.Marshal}, match: "Unmarshal must not be nil"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, option := range []nkv.StoreOption{nkv.WithWriteCodec(test.codec), nkv.WithReadCodec(test.codec), nkv.WithFallbackCodec(test.codec)} {
				assertPanicContains(t, test.match, func() { _ = nkv.NewStore(nil, option) })
			}
			assertPanicContains(t, test.match, func() { _ = nkv.NewStoreTx(nil, nkv.WithWriteCodec(test.codec)) })
		})
	}
	assertPanicContains(t, "duplicate codec", func() { _ = nkv.NewStore(nil, nkv.WithReadCodec(jsonCodec)) })
	for _, prefix := range []string{".app", "app..users", "app.*", "app.>", "app users"} {
		assertPanicContains(t, "invalid prefix", func() { _ = nkv.NewStore(nil, nkv.WithPrefix(prefix)) })
		assertPanicContains(t, "invalid prefix", func() { _ = nkv.NewStoreTx(nil, nkv.WithPrefix(prefix)) })
	}

	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_PREFIX"})
	is.NoErr(err) // bucket creation should succeed
	for _, prefix := range []string{"", ".", ".."} {
		store := nkv.NewStore(bucket, nkv.WithPrefix(prefix))
		_, err := store.Put(t.Context(), "key", prefix)
		is.NoErr(err) // empty normalized prefixes should not qualify the key
		value, _, err := nkv.NewStore(bucket).Get[string](t.Context(), "key")
		is.NoErr(err)           // unqualified key should be readable
		is.Equal(value, prefix) // empty prefix should leave the key unchanged
	}
}

func TestStoreDefaultTTL(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_TTL"})
	is.NoErr(err) // bucket creation should succeed
	store := nkv.NewStore(bucket, nkv.WithDefaultTTL(5*time.Minute))
	for _, test := range []struct {
		name string
		ttl  string
		opts []nkv.PutOption
	}{
		{name: "default", ttl: "5m0s"},
		{name: "override", ttl: "1h0m0s", opts: []nkv.PutOption{nkv.WithTTL(time.Hour)}},
		{name: "disabled", opts: []nkv.PutOption{nkv.WithTTL(0)}},
	} {
		_, err := store.Put(t.Context(), test.name, 1, test.opts...)
		is.NoErr(err) // write should apply the caller's TTL precedence
		message := lastRawMessage(t, nc, bucket, test.name)
		is.Equal(message.Header.Get("Nats-TTL"), test.ttl) // stored TTL should match the expected default or override
	}
	revision, err := store.Create(t.Context(), "create", "before")
	is.NoErr(err)                                                                    // create should apply the default TTL
	is.Equal(lastRawMessage(t, nc, bucket, "create").Header.Get("Nats-TTL"), "5m0s") // create should stamp the default TTL
	_, err = store.Update(t.Context(), "create", "after", revision, nkv.WithTTL(0))
	is.NoErr(err)                                                                    // update should accept an explicit zero TTL
	is.Equal(lastRawMessage(t, nc, bucket, "create").Header.Get("Nats-TTL"), "")     // explicit zero should override the default
	is.NoErr(store.Delete(t.Context(), "create"))                                    // deletion should not inherit the value TTL
	is.Equal(lastRawMessage(t, nc, bucket, "create").Header.Get("Nats-TTL"), "1m0s") // delete should use the byte bucket's marker TTL
}

func TestStoreListAndKeys(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_LIST"})
	is.NoErr(err) // bucket creation should succeed
	store := nkv.NewStore(bucket, nkv.WithPrefix("scope"))
	_, err = store.Put(t.Context(), "one", 1)
	is.NoErr(err) // first fixture write should succeed
	_, err = store.Put(t.Context(), "bad", "not an integer")
	is.NoErr(err) // incompatible fixture write should succeed
	_, err = store.Put(t.Context(), "two", 2)
	is.NoErr(err) // second fixture write should succeed
	values := store.List[int](t.Context(), "*", nkv.WithListBatch(1))
	_, err = store.Put(t.Context(), "later", 3)
	is.NoErr(err) // write after snapshot creation should succeed
	_, err = bucket.Put(t.Context(), "outside", []byte("4"))
	is.NoErr(err) // outside-prefix fixture should be written

	var got []int
	errorsSeen := 0
	for value, err := range values {
		if err != nil {
			errorsSeen++
			is.Equal(value, 0) // failed decode should yield a zero value
			continue
		}
		got = append(got, value)
	}
	is.Equal(got, []int{1, 2}) // iterator should continue past decode errors and exclude later writes
	is.Equal(errorsSeen, 1)    // incompatible data should be surfaced as an error

	keys := make([]string, 0)
	for key, err := range store.Keys(t.Context(), "*", nkv.WithListBatch(1)) {
		is.NoErr(err) // keys should not decode values
		keys = append(keys, key)
	}
	sort.Strings(keys)
	is.Equal(keys, []string{"bad", "later", "one", "two"}) // keys should stay inside the prefix and strip it

	for _, err := range store.List[int](t.Context(), "one") {
		is.NoErr(err) // early stop after a value should not fail
		break         // stopping after a value must stop the underlying iterator
	}
	for _, err := range store.List[int](t.Context(), "bad") {
		is.True(err != nil) // early stop should also work on decode failure
		break               // stopping after a decode error must stop the underlying iterator
	}
	for _, err := range store.Keys(t.Context(), "*") {
		is.NoErr(err) // early key iteration should not fail
		break         // stopping key iteration must not leak the byte iterator
	}
	for _, err := range store.List[int](t.Context(), "*", nkv.WithListBatch(0)) {
		is.True(errors.Is(err, nkv.ErrInvalidOption)) // byte enumeration errors should pass through
		break
	}
	for _, err := range store.Keys(t.Context(), "*", nkv.WithListBatch(0)) {
		is.True(errors.Is(err, nkv.ErrInvalidOption)) // key errors should pass through
		break
	}
}

func TestStoreCodecErrors(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_ERRORS"})
	is.NoErr(err) // bucket creation should succeed
	codecErr := errors.New("codec failed")
	failing := nkv.Codec{
		ContentType: "application/failing",
		Marshal:     func(any) ([]byte, error) { return nil, codecErr },
		Unmarshal:   func([]byte, any) error { return codecErr },
	}
	store := nkv.NewStore(bucket, nkv.WithWriteCodec(failing))
	_, err = store.Put(t.Context(), "key", 1)
	is.True(errors.Is(err, codecErr)) // put should preserve the marshal error
	_, err = store.Create(t.Context(), "key", 1)
	is.True(errors.Is(err, codecErr)) // create should preserve the marshal error
	_, err = store.Update(t.Context(), "key", 1, 1)
	is.True(errors.Is(err, codecErr)) // update should preserve the marshal error
	_, err = bucket.Put(t.Context(), "key", []byte("value"))
	is.NoErr(err) // decode fixture should be written
	value, revision, err := store.Get[int](t.Context(), "key")
	is.True(errors.Is(err, codecErr))               // get should preserve the unmarshal error
	is.True(strings.Contains(err.Error(), `"key"`)) // decode errors should include the key
	is.Equal(value, 0)                              // decode failures should return zero
	is.Equal(revision, uint64(0))                   // decode failures should return no revision

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = store.Get[int](ctx, "key")
	is.True(errors.Is(err, context.Canceled)) // read cancellation should pass through
}

func storeTestCodec(contentType, prefix string) nkv.Codec {
	return nkv.Codec{
		ContentType: contentType,
		Marshal: func(value any) ([]byte, error) {
			data, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			return append([]byte(prefix), data...), nil
		},
		Unmarshal: func(data []byte, target any) error {
			text, found := strings.CutPrefix(string(data), prefix)
			if !found {
				return fmt.Errorf("missing codec prefix %q", prefix)
			}
			return json.Unmarshal([]byte(text), target)
		},
	}
}
