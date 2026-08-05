package nkv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// directGetReq is the JSON body for $JS.API.DIRECT.GET.<stream> (ADR-31).
type directGetReq struct {
	Seq        uint64 `json:"seq,omitempty"`
	LastBySubj string `json:"last_by_subj,omitempty"`
	NextBySubj string `json:"next_by_subj,omitempty"`
	Batch      int    `json:"batch,omitempty"`
	UpToSeq    uint64 `json:"up_to_seq,omitempty"`
}

func (b *Bucket) directGetSubject() string {
	return jsDirectGetPrefix + b.stream
}

// directGetOne issues a single-message direct get request.
func (b *Bucket) directGetOne(ctx context.Context, req directGetReq) (*nats.Msg, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("kv: encode direct get request: %w", err)
	}

	msg, err := b.nc.RequestWithContext(ctx, b.directGetSubject(), payload)
	if err != nil {
		return nil, fmt.Errorf("kv: direct get: %w", err)
	}

	switch status := msg.Header.Get(hdrStatus); status {
	case "":
		return msg, nil
	case statusNotFound:
		return nil, ErrKeyNotFound
	default:
		return nil, fmt.Errorf("kv: direct get status %s: %s", status, msg.Header.Get("Description"))
	}
}

// Get returns the latest revision of a key, or a specific revision with
// WithRevision. Tombstoned keys return ErrKeyNotFound. Consumer-free: a
// single DIRECT.GET request served by any stream replica.
func (b *Bucket) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	if !validKey(key) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}

	var o getOpts

	for _, opt := range opts {
		opt.applyGet(&o)
	}

	req := directGetReq{LastBySubj: b.subject(key)}
	if o.revision > 0 {
		req = directGetReq{Seq: o.revision}
	}

	msg, err := b.directGetOne(ctx, req)
	if err != nil {
		return nil, err
	}

	entry, err := b.entryFromDirectMsg(msg)
	if err != nil {
		return nil, err
	}

	if o.revision > 0 && entry.Key != key {
		// seq-based fetch returns whatever message lives at that sequence;
		// reject if it belongs to another key
		return nil, ErrKeyNotFound
	}

	if entry.IsTombstone() {
		return nil, ErrKeyNotFound
	}

	return entry, nil
}

func (b *Bucket) publish(ctx context.Context, msg *nats.Msg) (uint64, error) {
	ack, err := b.js.PublishMsg(ctx, msg)
	if err != nil {
		return 0, fmt.Errorf("kv: publish %s: %w", msg.Subject, err)
	}

	return ack.Sequence, nil
}

func ttlHeader(h nats.Header, ttl interface{ String() string }) {
	if s := ttl.String(); s != zeroTTL {
		h.Set(hdrMsgTTL, s)
	}
}

// applyUserHeaders copies user-supplied headers onto a message header.
// Called before setting KV-internal headers so those always take precedence.
func applyUserHeaders(msg *nats.Msg, userHdrs nats.Header) {
	if len(userHdrs) == 0 {
		return
	}

	if msg.Header == nil {
		msg.Header = make(nats.Header, len(userHdrs))
	}

	for k, vs := range userHdrs {
		if isReservedHeader(k) {
			continue
		}

		for _, v := range vs {
			msg.Header.Add(k, v)
		}
	}
}

func isReservedHeader(header string) bool {
	for _, reserved := range []string{
		hdrOperation,
		hdrRollup,
		hdrExpectedSeq,
		hdrMsgTTL,
		hdrBatchID,
		hdrBatchSequence,
		hdrBatchCommit,
	} {
		if strings.EqualFold(header, reserved) {
			return true
		}
	}

	return false
}

// Put stores a value under key and returns the new revision.
func (b *Bucket) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (uint64, error) {
	if !validKey(key) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}

	var o putOpts

	for _, opt := range opts {
		opt.applyPut(&o)
	}
	if err := validateTTL(o.ttl); err != nil {
		return 0, err
	}

	msg := &nats.Msg{Subject: b.subject(key), Data: value, Header: nats.Header{}}

	applyUserHeaders(msg, o.headers)
	ttlHeader(msg.Header, o.ttl)

	return b.publish(ctx, msg)
}

func isWrongLastSequence(err error) bool {
	var apiErr *jetstream.APIError

	return errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}

// Create stores a value only if the key does not currently exist (or its
// latest revision is a tombstone). Returns ErrKeyExists otherwise.
func (b *Bucket) Create(ctx context.Context, key string, value []byte, opts ...CreateOption) (uint64, error) {
	if !validKey(key) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}

	var o createOpts

	for _, opt := range opts {
		opt.applyCreate(&o)
	}
	if err := validateTTL(o.ttl); err != nil {
		return 0, err
	}

	expected := uint64(0)

	for {
		msg := &nats.Msg{Subject: b.subject(key), Data: value, Header: nats.Header{}}

		applyUserHeaders(msg, o.headers)
		msg.Header.Set(hdrExpectedSeq, strconv.FormatUint(expected, 10))
		ttlHeader(msg.Header, o.ttl)

		rev, err := b.publish(ctx, msg)
		if err == nil {
			return rev, nil
		}

		if !isWrongLastSequence(err) {
			return 0, err
		}

		// key has a latest revision; if it is a tombstone, retry the
		// create expecting that revision, otherwise the key truly exists
		latest, gerr := b.directGetOne(ctx, directGetReq{LastBySubj: b.subject(key)})
		if gerr != nil {
			return 0, fmt.Errorf("%w (and failed to inspect latest: %w)", ErrKeyExists, gerr)
		}

		entry, derr := b.entryFromDirectMsg(latest)
		if derr != nil {
			return 0, derr
		}

		if !entry.IsTombstone() || entry.Revision == expected {
			return 0, ErrKeyExists
		}

		expected = entry.Revision
	}
}

// Update stores a value only if the key's latest revision matches rev
// (compare-and-swap). Returns ErrRevisionMismatch on conflict.
func (b *Bucket) Update(ctx context.Context, key string, value []byte, rev uint64, opts ...UpdateOption) (uint64, error) {
	if !validKey(key) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}

	var o updateOpts

	for _, opt := range opts {
		opt.applyUpdate(&o)
	}
	if err := validateTTL(o.ttl); err != nil {
		return 0, err
	}

	msg := &nats.Msg{Subject: b.subject(key), Data: value, Header: nats.Header{}}

	applyUserHeaders(msg, o.headers)
	msg.Header.Set(hdrExpectedSeq, strconv.FormatUint(rev, 10))
	ttlHeader(msg.Header, o.ttl)

	newRev, err := b.publish(ctx, msg)
	if isWrongLastSequence(err) {
		return 0, fmt.Errorf("%w: key %q at revision != %d", ErrRevisionMismatch, key, rev)
	}

	return newRev, err
}

// Delete writes a delete tombstone for key; history is retained up to the
// bucket's History limit. WithRevision makes it a CAS delete; WithTTL bounds
// the tombstone's lifetime.
func (b *Bucket) Delete(ctx context.Context, key string, opts ...DeleteOption) error {
	if !validKey(key) {
		return fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}

	var o deleteOpts

	for _, opt := range opts {
		opt.applyDelete(&o)
	}
	if err := validateTTL(o.ttl); err != nil {
		return err
	}

	msg := &nats.Msg{Subject: b.subject(key), Header: nats.Header{}}

	applyUserHeaders(msg, o.headers)
	msg.Header.Set(hdrOperation, opDel)

	if o.expectedRevision > 0 {
		msg.Header.Set(hdrExpectedSeq, strconv.FormatUint(o.expectedRevision, 10))
	}

	ttlHeader(msg.Header, o.ttl)

	_, err := b.publish(ctx, msg)
	if isWrongLastSequence(err) {
		return fmt.Errorf("%w: key %q at revision != %d", ErrRevisionMismatch, key, o.expectedRevision)
	}

	return err
}

// Purge writes a purge tombstone that rolls up (removes) all prior
// revisions of the key. WithTTL bounds the tombstone's lifetime.
func (b *Bucket) Purge(ctx context.Context, key string, opts ...PurgeOption) error {
	if !validKey(key) {
		return fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}

	var o purgeOpts

	for _, opt := range opts {
		opt.applyPurge(&o)
	}
	if err := validateTTL(o.ttl); err != nil {
		return err
	}

	msg := &nats.Msg{Subject: b.subject(key), Header: nats.Header{}}

	applyUserHeaders(msg, o.headers)
	msg.Header.Set(hdrOperation, opPurge)
	msg.Header.Set(hdrRollup, rollupSub)
	ttlHeader(msg.Header, o.ttl)

	_, err := b.publish(ctx, msg)

	return err
}
