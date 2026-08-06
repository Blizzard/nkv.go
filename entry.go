package nkv

import (
	"errors"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
)

// Operation describes what a stored revision represents.
type Operation int

const (
	OpPut Operation = iota
	OpDelete
	OpPurge
)

func (o Operation) String() string {
	switch o {
	case OpPut:
		return opPut
	case OpDelete:
		return opDel
	case OpPurge:
		return opPurge
	default:
		return opPut
	}
}

// Wire-level headers shared with the nats.go KV implementation.
const (
	hdrOperation    = "KV-Operation"
	hdrRollup       = "Nats-Rollup"
	hdrExpectedSeq  = "Nats-Expected-Last-Subject-Sequence"
	hdrMsgTTL       = "Nats-TTL"
	hdrSubject      = "Nats-Subject"
	hdrSequence     = "Nats-Sequence"
	hdrTimestamp    = "Nats-Time-Stamp"
	hdrLastSeq      = "Nats-Last-Sequence"
	hdrUpToSeq      = "Nats-UpTo-Sequence"
	hdrNumPending   = "Nats-Num-Pending"
	hdrStatus       = "Status"
	hdrDescription  = "Description"
	hdrMarkerReason = "Nats-Marker-Reason"
	hdrContentType  = "Content-Type"

	opPut   = "PUT"
	opDel   = "DEL"
	opPurge = "PURGE"

	rollupSub = "sub"

	statusOK       = "200"
	statusEOB      = "204"
	statusNotFound = "404"

	// JetStream API prefix for direct get requests.
	jsDirectGetPrefix = "$JS.API.DIRECT.GET."

	// Prepended to bucket names to form stream names.
	streamPrefix = "KV_"

	// Multi-level NATS subject wildcard.
	subjectWildcard = ">"

	// String representation of no TTL.
	zeroTTL = "0s"
)

// Errors returned by bucket operations.
var (
	ErrKeyNotFound      = errors.New("kv: key not found")
	ErrKeyExists        = errors.New("kv: key exists")
	ErrRevisionMismatch = errors.New("kv: revision mismatch")
	ErrInvalidKey       = errors.New("kv: invalid key")
	ErrInvalidOption    = errors.New("kv: invalid option")
)

// Entry is a single revision of a key.
type Entry struct {
	Bucket      string
	Key         string
	Value       []byte
	Revision    uint64 // stream sequence of this revision
	Delta       uint64 // messages after this entry in the delivery sequence
	Created     time.Time
	Operation   Operation
	ContentType string // MIME type from Content-Type header, empty if absent
	Headers     nats.Header
}

// IsTombstone reports whether the entry is a delete/purge marker.
func (e *Entry) IsTombstone() bool {
	return e.Operation != OpPut
}

func opFromHeader(h nats.Header) Operation {
	switch h.Get(hdrOperation) {
	case opDel:
		return OpDelete
	case opPurge:
		return OpPurge
	}

	// limit-marker tombstones (e.g. MaxAge / per-key TTL) carry a marker
	// reason instead of a KV-Operation header
	if h.Get(hdrMarkerReason) != "" {
		return OpPurge
	}

	return OpPut
}

// entryFromDirectMsg decodes a Direct Get response message into an Entry.
func (b *Bucket) entryFromDirectMsg(msg *nats.Msg) (*Entry, error) {
	seq, err := strconv.ParseUint(msg.Header.Get(hdrSequence), 10, 64)
	if err != nil {
		return nil, errors.New("kv: direct get response missing Nats-Sequence")
	}
	delta, err := strconv.ParseUint(msg.Header.Get(hdrNumPending), 10, 64)
	if err != nil {
		delta = 0
	}

	// A malformed or absent timestamp yields the zero time rather than an error.
	ts, err := time.Parse(time.RFC3339Nano, msg.Header.Get(hdrTimestamp))
	if err != nil {
		ts = time.Time{}
	}

	return &Entry{
		Bucket:      b.name,
		Key:         b.key(msg.Header.Get(hdrSubject)),
		Value:       msg.Data,
		Revision:    seq,
		Delta:       delta,
		Created:     ts,
		Operation:   opFromHeader(msg.Header),
		ContentType: msg.Header.Get(hdrContentType),
		Headers:     msg.Header,
	}, nil
}
