package nkv

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Store provides codec-backed access to a byte-oriented Bucket. Each operation
// selects its own value type, so one Store can access multiple types of values.
// Content-Type selects the read codec, not the Go type. Callers must select a
// type compatible with the stored value and use compatible patterns for List
// and Watch. Incompatible values may fail to decode or silently decode into an
// unintended value, depending on the codec.
type Store struct {
	kv         *Bucket
	codec      Codec
	fallback   Codec
	codecs     map[string]Codec
	prefix     string
	defaultTTL time.Duration
}

// NewStore returns a Store over kv. The default codec is JSONCodec(). The write
// codec is registered for reads and used as the fallback for absent or unknown
// Content-Type headers unless WithFallbackCodec overrides it. It panics if a
// configured codec is incomplete or multiple codecs register one content type.
func NewStore(kv *Bucket, opts ...StoreOption) *Store {
	cfg := configureStore(opts)
	codec := cfg.writeCodec()
	s := &Store{
		kv:         kv,
		codec:      codec,
		fallback:   codec,
		codecs:     map[string]Codec{codec.ContentType: codec},
		prefix:     cfg.prefix,
		defaultTTL: cfg.defaultTTL,
	}

	for _, c := range cfg.codecs {
		validateCodec(c)
		if _, exists := s.codecs[c.ContentType]; exists {
			panic(fmt.Sprintf("kv: duplicate codec for content type %q", c.ContentType))
		}
		s.codecs[c.ContentType] = c
	}
	if cfg.fallback != nil {
		validateCodec(*cfg.fallback)
		s.fallback = *cfg.fallback
	}

	return s
}

// Bucket returns the underlying byte-oriented Bucket.
func (s *Store) Bucket() *Bucket {
	return s.kv
}

// Tx creates a transaction inheriting the Store's write codec and key prefix.
// The Store's default TTL does not apply; use WithTxTTL to set transaction TTL.
func (s *Store) Tx(opts ...TxOption) *StoreTx {
	return &StoreTx{tx: s.kv.Tx(opts...), codec: s.codec, prefix: s.prefix}
}

// Get retrieves and decodes a value as T, returning its revision for CAS writes.
// WithRevision selects a specific revision instead of the latest value.
func (s *Store) Get[T any](ctx context.Context, key string, opts ...GetOption) (T, uint64, error) {
	entry, err := s.kv.Get(ctx, s.prefix+key, opts...)
	if err != nil {
		var zero T

		return zero, 0, err
	}

	v, err := s.decodeValue[T](entry)
	if err != nil {
		var zero T

		return zero, 0, err
	}

	return v, entry.Revision, nil
}

// Put encodes and stores a value, returning its new revision.
func (s *Store) Put[T any](ctx context.Context, key string, value T, opts ...PutOption) (uint64, error) {
	data, err := s.codec.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("kv: marshal: %w", err)
	}

	opts = s.prependPutDefaults(opts)
	opts = append(opts, WithHeaders(nats.Header{hdrContentType: []string{s.codec.ContentType}}))

	return s.kv.Put(ctx, s.prefix+key, data, opts...)
}

// Create encodes and stores a value only if the key does not exist.
func (s *Store) Create[T any](ctx context.Context, key string, value T, opts ...CreateOption) (uint64, error) {
	data, err := s.codec.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("kv: marshal: %w", err)
	}

	opts = s.prependCreateDefaults(opts)
	opts = append(opts, WithHeaders(nats.Header{hdrContentType: []string{s.codec.ContentType}}))

	return s.kv.Create(ctx, s.prefix+key, data, opts...)
}

// Update encodes and stores a value only if the key's revision matches rev.
func (s *Store) Update[T any](ctx context.Context, key string, value T, rev uint64, opts ...UpdateOption) (uint64, error) {
	data, err := s.codec.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("kv: marshal: %w", err)
	}

	opts = s.prependUpdateDefaults(opts)
	opts = append(opts, WithHeaders(nats.Header{hdrContentType: []string{s.codec.ContentType}}))

	return s.kv.Update(ctx, s.prefix+key, data, rev, opts...)
}

// Delete writes a delete tombstone for key.
func (s *Store) Delete(ctx context.Context, key string, opts ...DeleteOption) error {
	return s.kv.Delete(ctx, s.prefix+key, opts...)
}

// List returns a snapshot iterator decoding every matching value as T. The
// pattern must select values compatible with T; Content-Type only selects the
// codec. Decode failures are yielded as errors and iteration may continue.
func (s *Store) List[T any](ctx context.Context, pattern string, opts ...ListOption) iter.Seq2[T, error] {
	entries := s.kv.List(ctx, s.prefix+pattern, opts...)

	return func(yield func(T, error) bool) {
		for entry, err := range entries {
			var value T
			if err == nil {
				value, err = s.decodeValue[T](entry)
			}
			if !yield(value, err) {
				return
			}
		}
	}
}

// Keys returns matching keys with the Store's prefix removed.
func (s *Store) Keys(ctx context.Context, pattern string, opts ...KeysOption) iter.Seq2[string, error] {
	keys := s.kv.Keys(ctx, s.prefix+pattern, opts...)

	return func(yield func(string, error) bool) {
		for key, err := range keys {
			if !yield(strings.TrimPrefix(key, s.prefix), err) {
				return
			}
		}
	}
}

func (s *Store) decodeValue[T any](entry *Entry) (T, error) {
	codec := s.fallback
	if c, ok := s.codecs[entry.ContentType]; ok {
		codec = c
	}

	var value T
	if err := codec.Unmarshal(entry.Value, &value); err != nil {
		var zero T

		return zero, fmt.Errorf("kv: unmarshal key %q (content-type %q): %w", entry.Key, entry.ContentType, err)
	}

	return value, nil
}

// Prepend defaults so caller options applied later can override them.
func (s *Store) prependPutDefaults(opts []PutOption) []PutOption {
	if s.defaultTTL == 0 {
		return opts
	}

	return append([]PutOption{WithTTL(s.defaultTTL)}, opts...)
}

func (s *Store) prependCreateDefaults(opts []CreateOption) []CreateOption {
	if s.defaultTTL == 0 {
		return opts
	}

	return append([]CreateOption{WithTTL(s.defaultTTL)}, opts...)
}

func (s *Store) prependUpdateDefaults(opts []UpdateOption) []UpdateOption {
	if s.defaultTTL == 0 {
		return opts
	}

	return append([]UpdateOption{WithTTL(s.defaultTTL)}, opts...)
}
