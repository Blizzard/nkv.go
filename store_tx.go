package nkv

import (
	"context"

	"github.com/nats-io/nats.go"
)

// StoreTx encodes values into an underlying atomic Tx. Each write selects its
// own value type. Multiple StoreTx wrappers may share a Tx with different codecs
// or prefixes. Like Tx, it is not safe for concurrent use.
type StoreTx struct {
	tx     *Tx
	codec  Codec
	prefix string
}

// NewStoreTx wraps an existing Tx. Only WithWriteCodec and WithPrefix affect
// staged operations; read codecs and WithDefaultTTL do not apply. Use WithTxTTL
// when creating the underlying Tx to set its TTL. It panics if the write codec
// is incomplete. The default write codec is JSONCodec().
func NewStoreTx(tx *Tx, opts ...StoreOption) *StoreTx {
	cfg := configureStore(opts)

	return &StoreTx{tx: tx, codec: cfg.writeCodec(), prefix: cfg.prefix}
}

// Put stages an unconditional put of an encoded value.
func (s *StoreTx) Put[T any](key string, value T) error {
	return s.stage(key, value, batchPut, 0)
}

// Create stages a create that fails on commit if the key exists.
func (s *StoreTx) Create[T any](key string, value T) error {
	return s.stage(key, value, batchCreate, 0)
}

// Update stages a CAS update that fails on commit if the revision differs.
func (s *StoreTx) Update[T any](key string, value T, revision uint64) error {
	return s.stage(key, value, batchUpdate, revision)
}

// Delete stages a delete tombstone. WithRevision makes it CAS.
func (s *StoreTx) Delete(key string, opts ...DeleteOption) error {
	return s.tx.Delete(s.prefix+key, opts...)
}

// Len returns the number of operations staged in the underlying Tx.
func (s *StoreTx) Len() int {
	return s.tx.Len()
}

// Abort marks the underlying Tx as closed. It is idempotent and safe to defer.
func (s *StoreTx) Abort() {
	s.tx.Abort()
}

// Commit atomically writes all operations staged in the underlying Tx. On
// success all messages appear in the stream; on failure none do. After Commit
// the transaction cannot be reused, except that an empty Tx remains open.
func (s *StoreTx) Commit(ctx context.Context) (*TxResult, error) {
	return s.tx.Commit(ctx)
}

func (s *StoreTx) stage[T any](key string, value T, kind batchOpKind, revision uint64) error {
	data, err := s.codec.Marshal(value)
	if err != nil {
		return err
	}

	key = s.prefix + key
	if err := s.tx.reserveKey(key); err != nil {
		return err
	}

	s.tx.ops = append(s.tx.ops, batchOp{
		key:      key,
		value:    data,
		headers:  nats.Header{hdrContentType: []string{s.codec.ContentType}},
		kind:     kind,
		revision: revision,
	})

	return nil
}
