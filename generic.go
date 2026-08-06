package nkv

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// genericConfig holds non-generic configuration applied via GenericOption.
type genericConfig struct {
	codec      *Codec
	fallback   *Codec
	codecs     []Codec
	prefix     string
	defaultTTL time.Duration
}

// GenericOption configures a Generic bucket wrapper. Options are non-generic
// so callers never need type constraints on configuration.
type GenericOption func(*genericConfig)

// WithWriteCodec sets the write codec used for encoding on Put/Create/Update.
// Its ContentType is stamped into the Content-Type header and registered
// for read dispatch. If not specified, JSONCodec() is used.
func WithWriteCodec(c Codec) GenericOption {
	return func(cfg *genericConfig) {
		cfg.codec = &c
	}
}

// WithFallbackCodec sets the codec used to decode entries that have no
// Content-Type header (legacy data) or an unknown content type. If not specified, the write codec
// is used as fallback.
func WithFallbackCodec(c Codec) GenericOption {
	return func(cfg *genericConfig) {
		cfg.fallback = &c
	}
}

// WithReadCodec registers an additional codec for read dispatch. When an
// entry's Content-Type matches the codec's ContentType, it is used for
// decoding. May be specified multiple times for different content types.
func WithReadCodec(c Codec) GenericOption {
	return func(cfg *genericConfig) {
		cfg.codecs = append(cfg.codecs, c)
	}
}

// WithPrefix sets a key prefix that is automatically prepended (with a dot
// separator) to all keys in Get/Put/Create/Update/List/Tx operations. Empty
// prefixes and prefixes containing only dots disable prefixing. Other trailing
// dots are removed. It panics if the normalized prefix is not a valid key.
func WithPrefix(prefix string) GenericOption {
	return func(cfg *genericConfig) {
		cfg.prefix = normalizePrefix(prefix)
	}
}

func normalizePrefix(prefix string) string {
	normalized := strings.TrimRight(prefix, ".")
	if normalized == "" {
		return ""
	}

	if !validKey(normalized) || strings.Contains(normalized, "..") {
		panic(fmt.Sprintf("kv: invalid prefix %q", prefix))
	}

	return normalized + "."
}

// WithDefaultTTL sets a default TTL applied to all writes (Put/Create/Update)
// unless the caller explicitly passes WithTTL on the individual operation.
func WithDefaultTTL(ttl time.Duration) GenericOption {
	return func(cfg *genericConfig) {
		cfg.defaultTTL = ttl
	}
}

// Generic wraps a Bucket with a codec for type-safe data operations.
//
// The write codec is used for all encode operations and its ContentType is
// stamped into the Content-Type header. On reads, the Content-Type header
// selects the decode codec; if absent, the fallback codec is used (defaults
// to the write codec).
type Generic[T any] struct {
	kv         *Bucket
	codec      Codec
	fallback   Codec
	codecs     map[string]Codec // content-type -> codec
	prefix     string           // prepended to keys (includes trailing dot)
	defaultTTL time.Duration    // applied to writes when no WithTTL is specified
}

// GenericEntry is a watched entry with its value decoded as T. Entry retains
// the operation metadata and raw encoded value. Tombstones and metadata-only
// entries have the zero value of T.
type GenericEntry[T any] struct {
	Entry

	Value T
}

// GenericWatcher decodes entries delivered by a Watcher as T.
type GenericWatcher[T any] struct {
	watcher  *Watcher
	generic  *Generic[T]
	metaOnly bool
}

// Bucket returns the underlying Bucket.
func (t *Generic[T]) Bucket() *Bucket {
	return t.kv
}

// Tx creates a GenericTx that inherits this wrapper's codec and prefix.
func (t *Generic[T]) Tx(opts ...TxOption) *GenericTx[T] {
	return &GenericTx[T]{
		tx:     t.kv.Tx(opts...),
		codec:  t.codec,
		prefix: t.prefix,
	}
}

// NewGeneric returns a generic typed wrapper around a Bucket.
//
// If no WithWriteCodec option is provided, JSONCodec() is used as the default.
// The write codec is registered for read dispatch and used as the fallback
// for headerless entries unless overridden with WithFallbackCodec. It panics
// if any configured codec is incomplete or if two codecs use the same content
// type.
func NewGeneric[T any](kv *Bucket, opts ...GenericOption) *Generic[T] {
	var cfg genericConfig

	for _, opt := range opts {
		opt(&cfg)
	}

	// Default write codec to JSON.
	codec := JSONCodec()
	if cfg.codec != nil {
		codec = *cfg.codec
	}

	validateCodec(codec)

	t := &Generic[T]{
		kv:         kv,
		codec:      codec,
		codecs:     map[string]Codec{codec.ContentType: codec},
		prefix:     cfg.prefix,
		defaultTTL: cfg.defaultTTL,
	}

	// Register additional read codecs.
	for _, c := range cfg.codecs {
		validateCodec(c)

		if _, exists := t.codecs[c.ContentType]; exists {
			panic(fmt.Sprintf("kv: duplicate codec for content type %q", c.ContentType))
		}

		t.codecs[c.ContentType] = c
	}

	// Set fallback.
	if cfg.fallback != nil {
		validateCodec(*cfg.fallback)

		t.fallback = *cfg.fallback
	} else {
		t.fallback = codec
	}

	return t
}

func validateCodec(codec Codec) {
	if codec.ContentType == "" {
		panic("kv: codec ContentType must not be empty")
	}

	if codec.Marshal == nil {
		panic("kv: codec Marshal must not be nil")
	}

	if codec.Unmarshal == nil {
		panic("kv: codec Unmarshal must not be nil")
	}
}

// Get retrieves and decodes the latest value for key. Returns the decoded
// value and its revision (needed for CAS Update calls).
func (t *Generic[T]) Get(ctx context.Context, key string, opts ...GetOption) (T, uint64, error) {
	entry, err := t.kv.Get(ctx, t.prefix+key, opts...)
	if err != nil {
		var zero T

		return zero, 0, err
	}

	v, err := t.decodeValue(entry)
	if err != nil {
		var zero T

		return zero, 0, err
	}

	return v, entry.Revision, nil
}

// Put encodes and stores a value, returning the new revision.
func (t *Generic[T]) Put(ctx context.Context, key string, value T, opts ...PutOption) (uint64, error) {
	data, err := t.codec.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("kv: marshal: %w", err)
	}

	opts = t.prependPutDefaults(opts)
	opts = append(opts, WithHeaders(nats.Header{hdrContentType: []string{t.codec.ContentType}}))

	return t.kv.Put(ctx, t.prefix+key, data, opts...)
}

// Create encodes and stores a value only if the key does not exist.
func (t *Generic[T]) Create(ctx context.Context, key string, value T, opts ...CreateOption) (uint64, error) {
	data, err := t.codec.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("kv: marshal: %w", err)
	}

	opts = t.prependCreateDefaults(opts)
	opts = append(opts, WithHeaders(nats.Header{hdrContentType: []string{t.codec.ContentType}}))

	return t.kv.Create(ctx, t.prefix+key, data, opts...)
}

// Update encodes and stores a value only if the key's revision matches rev.
func (t *Generic[T]) Update(ctx context.Context, key string, value T, rev uint64, opts ...UpdateOption) (uint64, error) {
	data, err := t.codec.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("kv: marshal: %w", err)
	}

	opts = t.prependUpdateDefaults(opts)
	opts = append(opts, WithHeaders(nats.Header{hdrContentType: []string{t.codec.ContentType}}))

	return t.kv.Update(ctx, t.prefix+key, data, rev, opts...)
}

// Delete writes a delete tombstone for key.
func (t *Generic[T]) Delete(ctx context.Context, key string, opts ...DeleteOption) error {
	return t.kv.Delete(ctx, t.prefix+key, opts...)
}

// Purge writes a purge tombstone and removes prior revisions of key.
func (t *Generic[T]) Purge(ctx context.Context, key string, opts ...PurgeOption) error {
	return t.kv.Purge(ctx, t.prefix+key, opts...)
}

// Watch starts a typed watcher for entries matching pattern.
func (t *Generic[T]) Watch(ctx context.Context, pattern string, opts ...WatchOption) (*GenericWatcher[T], error) {
	var configured watchOpts
	for _, opt := range opts {
		opt.applyWatch(&configured)
	}

	watcher, err := t.kv.Watch(ctx, t.prefix+pattern, append(opts, prefixWatchFilters(t.prefix))...)
	if err != nil {
		return nil, err
	}

	return &GenericWatcher[T]{watcher: watcher, generic: t, metaOnly: configured.headersOnly}, nil
}

// Next blocks until the next typed entry is available. It returns nil, nil at
// the end of the initial replay, matching Watcher.Next.
func (w *GenericWatcher[T]) Next() (*GenericEntry[T], error) {
	entry, err := w.watcher.Next()
	if err != nil || entry == nil {
		return nil, err
	}

	typed := &GenericEntry[T]{Entry: *entry}
	typed.Key = strings.TrimPrefix(entry.Key, w.generic.prefix)
	if entry.IsTombstone() || w.metaOnly {
		return typed, nil
	}

	typed.Value, err = w.generic.decodeValue(entry)
	if err != nil {
		return nil, err
	}

	return typed, nil
}

// InitialDone reports whether the initial replay has completed.
func (w *GenericWatcher[T]) InitialDone() bool {
	return w.watcher.InitialDone()
}

// Stop tears down the watcher and its underlying consumer.
func (w *GenericWatcher[T]) Stop() {
	w.watcher.Stop()
}

// Updates returns a channel adapter around the typed watcher. A nil entry
// signals the end of the initial replay.
func (w *GenericWatcher[T]) Updates() <-chan *GenericEntry[T] {
	ch := make(chan *GenericEntry[T], updatesChanBuffer)

	go func() {
		defer close(ch)

		for {
			entry, err := w.Next()
			if err != nil {
				return
			}

			select {
			case ch <- entry:
			case <-w.watcher.done:
				return
			}
		}
	}()

	return ch
}

type prefixWatchFilters string

func (prefix prefixWatchFilters) applyWatch(opts *watchOpts) {
	for i, filter := range opts.extraFilters {
		opts.extraFilters[i] = string(prefix) + filter
	}
}

// List returns a typed iterator over values matching the pattern.
func (t *Generic[T]) List(ctx context.Context, pattern string, opts ...ListOption) iter.Seq2[T, error] {
	entries := t.kv.List(ctx, t.prefix+pattern, opts...)

	return func(yield func(T, error) bool) {
		for entry, err := range entries {
			if err != nil {
				var zero T

				if !yield(zero, err) {
					return
				}

				continue
			}

			v, err := t.decodeValue(entry)
			if err != nil {
				var zero T

				if !yield(zero, err) {
					return
				}

				continue
			}

			if !yield(v, nil) {
				return
			}
		}
	}
}

// Keys returns an iterator over keys matching the pattern.
func (t *Generic[T]) Keys(ctx context.Context, pattern string, opts ...KeysOption) iter.Seq2[string, error] {
	keys := t.kv.Keys(ctx, t.prefix+pattern, opts...)

	return func(yield func(string, error) bool) {
		for key, err := range keys {
			if !yield(strings.TrimPrefix(key, t.prefix), err) {
				return
			}
		}
	}
}

// decodeValue selects the appropriate codec based on the entry's Content-Type
// header and decodes the value.
func (t *Generic[T]) decodeValue(entry *Entry) (T, error) {
	codec := t.codecForEntry(entry)

	var v T

	if err := codec.Unmarshal(entry.Value, &v); err != nil {
		var zero T

		return zero, fmt.Errorf("kv: unmarshal key %q (content-type %q): %w", entry.Key, entry.ContentType, err)
	}

	return v, nil
}

// codecForEntry returns the codec to use for decoding an entry. Uses the
// Content-Type header to select from registered codecs; falls back to the
// fallback codec if the header is absent or unrecognized.
func (t *Generic[T]) codecForEntry(entry *Entry) Codec {
	ct := entry.ContentType
	if ct == "" {
		return t.fallback
	}

	if c, ok := t.codecs[ct]; ok {
		return c
	}

	return t.fallback
}

// prependPutDefaults prepends the default TTL so caller opts applied later
// can override it.
func (t *Generic[T]) prependPutDefaults(opts []PutOption) []PutOption {
	if t.defaultTTL == 0 {
		return opts
	}

	return append([]PutOption{WithTTL(t.defaultTTL)}, opts...)
}

func (t *Generic[T]) prependCreateDefaults(opts []CreateOption) []CreateOption {
	if t.defaultTTL == 0 {
		return opts
	}

	return append([]CreateOption{WithTTL(t.defaultTTL)}, opts...)
}

func (t *Generic[T]) prependUpdateDefaults(opts []UpdateOption) []UpdateOption {
	if t.defaultTTL == 0 {
		return opts
	}

	return append([]UpdateOption{WithTTL(t.defaultTTL)}, opts...)
}
