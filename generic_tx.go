package nkv

import (
	"context"

	"github.com/nats-io/nats.go"
)

// GenericTx is a typed wrapper around a Tx. It marshals values using the
// provided codec and stamps Content-Type on each staged operation so the
// read side can dispatch correctly.
//
// Multiple GenericTx instances can share the same underlying Tx to stage
// operations for different types in a single atomic commit.
type GenericTx[T any] struct {
	tx     *Tx
	codec  Codec
	prefix string
}

// NewGenericTx wraps an existing Tx with a codec. The codec's ContentType
// is stamped on every message staged through this wrapper. It panics when
// the write codec is incomplete.
func NewGenericTx[T any](tx *Tx, opts ...GenericOption) *GenericTx[T] {
	var cfg genericConfig

	for _, opt := range opts {
		opt(&cfg)
	}

	codec := JSONCodec()
	if cfg.codec != nil {
		codec = *cfg.codec
	}

	validateCodec(codec)

	return &GenericTx[T]{tx: tx, codec: codec, prefix: cfg.prefix}
}

// Put stages an unconditional put with the marshaled value.
func (g *GenericTx[T]) Put(key string, val T) error {
	data, err := g.codec.Marshal(val)
	if err != nil {
		return err
	}

	key = g.prefix + key
	if err := g.tx.reserveKey(key); err != nil {
		return err
	}

	g.tx.ops = append(g.tx.ops, batchOp{
		key:     key,
		value:   data,
		headers: nats.Header{hdrContentType: []string{g.codec.ContentType}},
		kind:    batchPut,
	})

	return nil
}

// Create stages a create (fails on commit if key exists).
func (g *GenericTx[T]) Create(key string, val T) error {
	data, err := g.codec.Marshal(val)
	if err != nil {
		return err
	}

	key = g.prefix + key
	if err := g.tx.reserveKey(key); err != nil {
		return err
	}

	g.tx.ops = append(g.tx.ops, batchOp{
		key:     key,
		value:   data,
		headers: nats.Header{hdrContentType: []string{g.codec.ContentType}},
		kind:    batchCreate,
	})

	return nil
}

// Update stages a CAS update (fails on commit if revision doesn't match).
func (g *GenericTx[T]) Update(key string, val T, revision uint64) error {
	data, err := g.codec.Marshal(val)
	if err != nil {
		return err
	}

	key = g.prefix + key
	if err := g.tx.reserveKey(key); err != nil {
		return err
	}

	g.tx.ops = append(g.tx.ops, batchOp{
		key:      key,
		value:    data,
		headers:  nats.Header{hdrContentType: []string{g.codec.ContentType}},
		kind:     batchUpdate,
		revision: revision,
	})

	return nil
}

// Delete stages a delete tombstone. WithRevision makes it CAS.
func (g *GenericTx[T]) Delete(key string, opts ...DeleteOption) error {
	return g.tx.Delete(g.prefix+key, opts...)
}

// Purge stages a purge (rollup) tombstone.
func (g *GenericTx[T]) Purge(key string) error {
	return g.tx.Purge(g.prefix + key)
}

// Len returns the number of staged operations.
func (g *GenericTx[T]) Len() int {
	return g.tx.Len()
}

// Abort marks the tx as closed without committing. Idempotent and safe to defer.
func (g *GenericTx[T]) Abort() {
	g.tx.Abort()
}

// Commit atomically writes all staged operations. On success all messages
// appear in the stream; on failure none do.
func (g *GenericTx[T]) Commit(ctx context.Context) (*TxResult, error) {
	return g.tx.Commit(ctx)
}
