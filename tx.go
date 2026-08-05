package nkv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Atomic batch publish protocol headers (wire format).
const (
	hdrBatchID       = "Nats-Batch-Id"
	hdrBatchSequence = "Nats-Batch-Sequence"
	hdrBatchCommit   = "Nats-Batch-Commit"
)

// Batch operation kinds (wire protocol semantics).
type batchOpKind int

const (
	batchPut batchOpKind = iota
	batchCreate
	batchUpdate
	batchDelete
	batchPurge
)

// txIDBytes is the length of the random Nats-Batch-Id, hex encoded on the wire.
const txIDBytes = 8

type batchOp struct {
	key      string
	value    []byte
	headers  nats.Header
	ttl      time.Duration
	ttlSet   bool
	revision uint64
	kind     batchOpKind
}

// Tx errors.
var (
	ErrDuplicateKey = errors.New("kv: duplicate key in tx")
	ErrEmptyTx      = errors.New("kv: empty tx")
	ErrTxClosed     = errors.New("kv: tx already committed or aborted")
	ErrTxConflict   = errors.New("kv: tx conflict")
)

// TxStageError is returned when an intermediate tx message is rejected
// by the server (e.g. CAS conflict). The server discards the entire pending
// tx on any intermediate failure.
type TxStageError struct {
	OpIndex int
	Key     string
	Detail  string
}

func (e *TxStageError) Error() string {
	return fmt.Sprintf("kv: tx stage error on op %d (key %q): %s", e.OpIndex, e.Key, e.Detail)
}

// TxResult contains metadata from a committed tx.
type TxResult struct {
	ID       string
	Size     int
	Sequence uint64
}

// TxOption configures a Tx.
type TxOption func(*Tx)

// WithTxTTL sets the TTL for all messages in the tx. Values below 1s
// are clamped to 1s (NATS server minimum). Zero or negative disables TTL.
func WithTxTTL(ttl time.Duration) TxOption {
	return func(tx *Tx) {
		if ttl <= 0 {
			tx.ttl = 0

			return
		}

		tx.ttl = max(time.Second, ttl)
	}
}

// Tx creates a new atomic transaction. All staged operations are
// buffered on the server and committed atomically. If any operation
// fails (e.g. CAS conflict), the entire tx is discarded.
//
// A Tx is not safe for concurrent use. Use defer tx.Abort() to ensure
// cleanup if Commit is not reached.
func (b *Bucket) Tx(opts ...TxOption) *Tx {
	id := make([]byte, txIDBytes)
	_, _ = rand.Read(id)

	tx := &Tx{
		nc:   b.nc,
		js:   b.js,
		pre:  b.prefix,
		id:   hex.EncodeToString(id),
		keys: make(map[string]struct{}),
	}

	for _, opt := range opts {
		opt(tx)
	}

	return tx
}

type Tx struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	pre    string
	id     string
	ops    []batchOp
	keys   map[string]struct{}
	ttl    time.Duration
	closed bool
}

// Put stages an unconditional put.
func (tx *Tx) Put(key string, value []byte) error {
	if err := tx.reserveKey(key); err != nil {
		return err
	}

	tx.ops = append(tx.ops, batchOp{key: key, value: value, kind: batchPut})

	return nil
}

// Create stages a create (fails on commit if key exists).
func (tx *Tx) Create(key string, value []byte) error {
	if err := tx.reserveKey(key); err != nil {
		return err
	}

	tx.ops = append(tx.ops, batchOp{key: key, value: value, kind: batchCreate})

	return nil
}

// Update stages a CAS update (fails on commit if revision doesn't match).
func (tx *Tx) Update(key string, value []byte, revision uint64) error {
	if err := tx.reserveKey(key); err != nil {
		return err
	}

	tx.ops = append(tx.ops, batchOp{key: key, value: value, kind: batchUpdate, revision: revision})

	return nil
}

// Delete stages a delete tombstone. WithRevision makes it CAS.
func (tx *Tx) Delete(key string, opts ...DeleteOption) error {
	var o deleteOpts

	for _, opt := range opts {
		opt.applyDelete(&o)
	}
	if err := validateTTL(o.ttl); err != nil {
		return err
	}

	if err := tx.reserveKey(key); err != nil {
		return err
	}

	tx.ops = append(tx.ops, batchOp{
		key:      key,
		headers:  o.headers,
		ttl:      o.ttl,
		ttlSet:   o.ttlSet,
		kind:     batchDelete,
		revision: o.expectedRevision,
	})

	return nil
}

// Purge stages a purge (rollup) tombstone.
func (tx *Tx) Purge(key string) error {
	if err := tx.reserveKey(key); err != nil {
		return err
	}

	tx.ops = append(tx.ops, batchOp{key: key, kind: batchPurge})

	return nil
}

// Len returns the number of staged operations.
func (tx *Tx) Len() int {
	return len(tx.ops)
}

// Abort marks the tx as closed without committing. Buffered messages
// on the server are discarded automatically. Abort is idempotent and
// safe to defer.
func (tx *Tx) Abort() {
	tx.closed = true
}

// Commit atomically writes all staged operations to the bucket. On success,
// all messages appear in the stream; on failure, none do.
//
// After Commit (success or failure) the tx cannot be reused.
func (tx *Tx) Commit(ctx context.Context) (*TxResult, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}

	if len(tx.ops) == 0 {
		return nil, ErrEmptyTx
	}

	defer func() { tx.closed = true }()

	last := len(tx.ops) - 1

	for i, op := range tx.ops {
		msg := tx.buildMsg(i, op)

		if i == last {
			return tx.commitFinal(ctx, msg, op)
		}

		if err := tx.stageOp(ctx, msg, i, op); err != nil {
			return nil, err
		}
	}

	return nil, errors.New("kv: unexpected tx commit state")
}

// buildMsg renders one staged operation into its wire message. The index i is
// zero-based; the batch sequence header is one-based.
func (tx *Tx) buildMsg(i int, op batchOp) *nats.Msg {
	msg := &nats.Msg{
		Subject: tx.pre + op.key,
		Data:    op.value,
		Header:  nats.Header{},
	}

	// Apply per-op headers (e.g. Content-Type from GenericTx).
	applyUserHeaders(msg, op.headers)

	msg.Header.Set(hdrBatchID, tx.id)
	msg.Header.Set(hdrBatchSequence, strconv.Itoa(i+1))

	switch op.kind {
	case batchCreate:
		msg.Header.Set(hdrExpectedSeq, "0")

	case batchUpdate:
		msg.Header.Set(hdrExpectedSeq, strconv.FormatUint(op.revision, 10))

	case batchDelete:
		msg.Header.Set(hdrOperation, opDel)
		msg.Data = nil

		if op.revision > 0 {
			msg.Header.Set(hdrExpectedSeq, strconv.FormatUint(op.revision, 10))
		}

	case batchPurge:
		msg.Header.Set(hdrOperation, opPurge)
		msg.Header.Set(hdrRollup, rollupSub)
		msg.Data = nil

	case batchPut:
		// No additional headers.
	}

	switch {
	case op.ttlSet:
		ttlHeader(msg.Header, op.ttl)
	case tx.ttl > 0:
		msg.Header.Set(hdrMsgTTL, tx.ttl.String())
	}

	return msg
}

// commitFinal publishes the last message of the batch, which is what makes
// the server write the whole batch to the stream.
func (tx *Tx) commitFinal(ctx context.Context, msg *nats.Msg, op batchOp) (*TxResult, error) {
	msg.Header.Set(hdrBatchCommit, "1")

	pa, err := tx.js.PublishMsg(ctx, msg)
	if err != nil {
		if isTxWrongLastSequence(err) {
			return nil, fmt.Errorf("%w: key %s: %w", ErrTxConflict, op.key, err)
		}

		return nil, fmt.Errorf("kv: tx %s: commit publish: %w", tx.id, err)
	}

	return &TxResult{
		ID:       tx.id,
		Size:     len(tx.ops),
		Sequence: pa.Sequence,
	}, nil
}

// stageOp sends an intermediate message via raw request-reply. The server
// buffers it without writing to the stream, and reports staging failures
// (CAS conflicts) in the reply.
func (tx *Tx) stageOp(ctx context.Context, msg *nats.Msg, i int, op batchOp) error {
	resp, err := tx.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return fmt.Errorf("kv: tx %s: stage op %d (%s): %w", tx.id, i, op.key, err)
	}

	if len(resp.Data) > 0 {
		detail := string(resp.Data)
		stageErr := &TxStageError{OpIndex: i, Key: op.key, Detail: detail}

		if strings.Contains(detail, "wrong last sequence") {
			return fmt.Errorf("%w: %w", ErrTxConflict, stageErr)
		}

		return stageErr
	}

	if status := resp.Header.Get(hdrStatus); status != "" && status != statusOK {
		desc := resp.Header.Get(hdrDescription)

		return &TxStageError{OpIndex: i, Key: op.key, Detail: status + " " + desc}
	}

	return nil
}

// reserveKey validates a key and claims it for this tx; a tx may touch a
// given key at most once.
func (tx *Tx) reserveKey(key string) error {
	if tx.closed {
		return ErrTxClosed
	}

	if !validKey(key) {
		return fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}

	if _, exists := tx.keys[key]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateKey, key)
	}

	tx.keys[key] = struct{}{}

	return nil
}

func isTxWrongLastSequence(err error) bool {
	var apiErr *jetstream.APIError

	return errors.As(err, &apiErr) && apiErr.ErrorCode == 10071
}
