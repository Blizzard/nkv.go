package nkv_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
)

func TestInvalidKeyPatterns(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "INVALID_PATTERNS"})
	is.NoErr(err) // bucket creation should succeed

	// A closed connection makes accidental server requests fail differently
	// from local validation, without waiting for malformed requests to time out.
	nc.Close()

	patterns := []string{
		"",
		".",
		"..",
		".users",
		"users.",
		"users..tom",
		"users..*",
		"users..>",
		"users.*tail",
		"users.tom*",
		"users.**",
		"users.>tail",
		"users.tom>",
		"users.>>",
		"users.>.name",
		"users.>.*",
		"users.>.>",
		"users name",
		"users.\tname",
		"users.é",
	}

	for _, pattern := range patterns {
		t.Run(fmt.Sprintf("%q", pattern), func(t *testing.T) {
			t.Run("list", func(t *testing.T) {
				is := is.New(t)
				count := 0

				for entry, err := range bucket.List(t.Context(), pattern) {
					is.True(entry == nil)                      // invalid patterns should not return entries
					is.True(errors.Is(err, nkv.ErrInvalidKey)) // validation must precede server access
					count++
				}

				is.Equal(count, 1) // invalid list should yield one error and terminate
			})

			t.Run("keys", func(t *testing.T) {
				is := is.New(t)
				count := 0

				for key, err := range bucket.Keys(t.Context(), pattern) {
					is.Equal(key, "")                          // invalid patterns should not return keys
					is.True(errors.Is(err, nkv.ErrInvalidKey)) // validation must precede server access
					count++
				}

				is.Equal(count, 1) // invalid keys iterator should yield one error and terminate
			})

			t.Run("watch", func(t *testing.T) {
				is := is.New(t)

				watcher, err := bucket.Watch(t.Context(), pattern)
				is.True(watcher == nil)                    // invalid patterns must not create a watcher
				is.True(errors.Is(err, nkv.ErrInvalidKey)) // validation must precede server access
			})

			t.Run("additional watch filter", func(t *testing.T) {
				is := is.New(t)

				watcher, err := bucket.Watch(t.Context(), "users.*", nkv.WithAdditionalKeys("config.*", pattern))
				is.True(watcher == nil)                    // any invalid additional filter must prevent watcher creation
				is.True(errors.Is(err, nkv.ErrInvalidKey)) // every additional filter must be validated before server access
			})
		})
	}
}

func TestStoreValidatesPrefixedKeysAndFilters(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	bucket, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STORE_VALIDATION"})
	is.NoErr(err) // bucket creation should succeed

	store := nkv.NewStore(bucket, nkv.WithPrefix("scope"))
	nc.Close()

	_, _, err = store.Get[int](t.Context(), ".name")
	is.True(errors.Is(err, nkv.ErrInvalidKey)) // prefixing must not hide an invalid leading empty token

	watcher, err := store.Watch[int](t.Context(), "*", nkv.WithAdditionalKeys("config..*"))
	is.True(watcher == nil)                    // prefixing must not make an invalid additional filter usable
	is.True(errors.Is(err, nkv.ErrInvalidKey)) // typed watches should propagate byte-level validation
}
