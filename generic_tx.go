package nkv

import "context"

// GenericTx fixes the value type of a StoreTx. Multiple wrappers may share an
// underlying Tx to write different value types in one atomic commit.
type GenericTx[T any] struct {
	store *StoreTx
}

// NewGenericTx wraps an existing Tx with the same options and validation as
// NewStoreTx. The default write codec is JSONCodec().
func NewGenericTx[T any](tx *Tx, opts ...GenericOption) *GenericTx[T] {
	return &GenericTx[T]{store: NewStoreTx(tx, opts...)}
}

// Put stages an unconditional put of an encoded value.
func (g *GenericTx[T]) Put(key string, value T) error {
	return g.store.Put(key, value)
}

// Create stages a create that fails on commit if the key exists.
func (g *GenericTx[T]) Create(key string, value T) error {
	return g.store.Create(key, value)
}

// Update stages a CAS update that fails on commit if the revision differs.
func (g *GenericTx[T]) Update(key string, value T, revision uint64) error {
	return g.store.Update(key, value, revision)
}

// Delete stages a delete tombstone. WithRevision makes it CAS.
func (g *GenericTx[T]) Delete(key string, opts ...DeleteOption) error {
	return g.store.Delete(key, opts...)
}

// Len returns the number of operations staged in the underlying Tx.
func (g *GenericTx[T]) Len() int {
	return g.store.Len()
}

// Abort marks the underlying Tx as closed. It is idempotent and safe to defer.
func (g *GenericTx[T]) Abort() {
	g.store.Abort()
}

// Commit atomically writes all operations staged in the underlying Tx.
func (g *GenericTx[T]) Commit(ctx context.Context) (*TxResult, error) {
	return g.store.Commit(ctx)
}
