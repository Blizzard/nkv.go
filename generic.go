package nkv

import (
	"context"
	"iter"
)

// GenericOption is the compatibility name for StoreOption.
type GenericOption = StoreOption

// Generic fixes the value type of a Store for callers that prefer a typed handle.
// All encoding, key handling, and storage operations are implemented by Store.
type Generic[T any] struct {
	store *Store
}

// GenericEntry is the compatibility name for StoreEntry.
type GenericEntry[T any] = StoreEntry[T]

// GenericWatcher is the compatibility name for StoreWatcher.
type GenericWatcher[T any] = StoreWatcher[T]

// NewGeneric returns a fixed-type wrapper with the same options and validation
// as NewStore. The default codec is JSONCodec().
func NewGeneric[T any](kv *Bucket, opts ...GenericOption) *Generic[T] {
	return &Generic[T]{store: NewStore(kv, opts...)}
}

// Bucket returns the underlying byte-oriented Bucket.
func (g *Generic[T]) Bucket() *Bucket {
	return g.store.Bucket()
}

// Tx creates a transaction inheriting this wrapper's codec and prefix.
func (g *Generic[T]) Tx(opts ...TxOption) *GenericTx[T] {
	return &GenericTx[T]{store: g.store.Tx(opts...)}
}

// Get retrieves and decodes a value, returning its revision for CAS writes.
func (g *Generic[T]) Get(ctx context.Context, key string, opts ...GetOption) (T, uint64, error) {
	return g.store.Get[T](ctx, key, opts...)
}

// Put encodes and stores a value, returning its new revision.
func (g *Generic[T]) Put(ctx context.Context, key string, value T, opts ...PutOption) (uint64, error) {
	return g.store.Put(ctx, key, value, opts...)
}

// Create encodes and stores a value only if the key does not exist.
func (g *Generic[T]) Create(ctx context.Context, key string, value T, opts ...CreateOption) (uint64, error) {
	return g.store.Create(ctx, key, value, opts...)
}

// Update encodes and stores a value only if the key's revision matches rev.
func (g *Generic[T]) Update(ctx context.Context, key string, value T, rev uint64, opts ...UpdateOption) (uint64, error) {
	return g.store.Update(ctx, key, value, rev, opts...)
}

// Delete writes a delete tombstone for key.
func (g *Generic[T]) Delete(ctx context.Context, key string, opts ...DeleteOption) error {
	return g.store.Delete(ctx, key, opts...)
}

// Watch starts a watcher decoding matching entries as T.
func (g *Generic[T]) Watch(ctx context.Context, pattern string, opts ...WatchOption) (*GenericWatcher[T], error) {
	return g.store.Watch[T](ctx, pattern, opts...)
}

// List returns a snapshot iterator decoding matching values as T.
func (g *Generic[T]) List(ctx context.Context, pattern string, opts ...ListOption) iter.Seq2[T, error] {
	return g.store.List[T](ctx, pattern, opts...)
}

// Keys returns matching keys with the configured prefix removed.
func (g *Generic[T]) Keys(ctx context.Context, pattern string, opts ...KeysOption) iter.Seq2[string, error] {
	return g.store.Keys(ctx, pattern, opts...)
}
