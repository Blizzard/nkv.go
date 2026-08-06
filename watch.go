package nkv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// Pull window for the ordered consumer backing a watch.
	defaultPullBatch = 512

	// How long the server keeps the watch consumer alive across
	// disconnects before requiring a replay.
	defaultInactiveThreshold = 30 * time.Second

	// Buffer depth of the Updates channel adapter.
	updatesChanBuffer = 64
)

// ErrWatcherStopped is returned by Next after the watcher has been stopped.
var ErrWatcherStopped = errors.New("kv: watcher stopped")

// Watcher delivers a stream of entries from a pull-based ordered consumer.
// Backpressure is native: the client pulls what it can process. No push
// delivery, no connection-level slow consumer risk.
type Watcher struct {
	bucket   *Bucket
	cons     jetstream.Consumer
	iter     jetstream.MessagesContext
	nextMu   sync.Mutex
	stateMu  sync.RWMutex
	initial  bool // true while initial replay is in-flight
	sentinel bool // true when the initial replay sentinel is pending
	pending  uint64
	received uint64
	cancel   context.CancelFunc
	done     <-chan struct{}
	stopOnce sync.Once
}

// Watch starts a pull-ordered-consumer-backed watcher on the given key
// pattern (e.g. "users.>" or "config.*"). After the initial replay of
// current state completes, Next returns live updates.
//
// The caller must call Stop when done. Unlike the nats.go KV Watch, the
// underlying consumer is pull-based: stalls do not slow-consumer the
// connection.
func (b *Bucket) Watch(ctx context.Context, pattern string, opts ...WatchOption) (*Watcher, error) {
	o := watchOpts{
		pullBatch: defaultPullBatch,
		inactive:  defaultInactiveThreshold,
	}

	for _, opt := range opts {
		opt.applyWatch(&o)
	}
	if err := validateWatchOptions(o); err != nil {
		return nil, err
	}

	// Determine filter subjects
	filters := make([]string, 0, 1+len(o.extraFilters))
	filters = append(filters, b.subject(pattern))

	for _, extra := range o.extraFilters {
		filters = append(filters, b.subject(extra))
	}

	// Pick deliver policy
	deliver := jetstream.DeliverLastPerSubjectPolicy
	if o.updatesOnly {
		deliver = jetstream.DeliverNewPolicy
	}

	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:    filters,
		DeliverPolicy:     deliver,
		InactiveThreshold: o.inactive,
		HeadersOnly:       o.headersOnly,
	}

	s, err := b.js.Stream(ctx, b.stream)
	if err != nil {
		return nil, fmt.Errorf("kv: watch: stream info: %w", err)
	}

	cons, err := s.OrderedConsumer(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("kv: watch: create consumer: %w", err)
	}

	info := cons.CachedInfo()
	if info == nil {
		return nil, errors.New("kv: watch: consumer info unavailable")
	}

	initialPending := info.NumPending

	cctx, cancel := context.WithCancel(ctx)

	iter, err := cons.Messages(jetstream.PullMaxMessages(o.pullBatch), jetstream.WithMessagesErrOnMissingHeartbeat(true))
	if err != nil {
		cancel()

		return nil, fmt.Errorf("kv: watch: messages: %w", err)
	}

	w := &Watcher{
		bucket:   b,
		cons:     cons,
		iter:     iter,
		initial:  !o.updatesOnly && initialPending > 0,
		sentinel: !o.updatesOnly && initialPending == 0,
		pending:  initialPending,
		cancel:   cancel,
		done:     cctx.Done(),
	}

	go func() {
		<-cctx.Done()
		w.Stop()
	}()

	return w, nil
}

// Next blocks until the next entry is available. Concurrent calls are
// serialized; each entry is delivered to exactly one caller. Returns nil, nil
// once the initial replay is complete (sentinel) — subsequent calls deliver
// live updates. Returns an error on context cancellation or unrecoverable
// consumer failure.
func (w *Watcher) Next() (*Entry, error) {
	w.nextMu.Lock()
	defer w.nextMu.Unlock()

	w.stateMu.Lock()
	if w.sentinel {
		w.sentinel = false
		w.stateMu.Unlock()

		//nolint:nilnil // documented API: nil,nil marks the end of the initial replay
		return nil, nil
	}
	w.stateMu.Unlock()

	msg, err := w.iter.Next()
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			return nil, ErrWatcherStopped
		}

		return nil, fmt.Errorf("kv: watch next: %w", err)
	}

	entry, err := w.bucket.entryFromJSMsg(msg)
	if err != nil {
		// Ordered consumers use AckNone; nak is a no-op but keeps
		// the interface honest. Errors here are connection-level and
		// the consumer will recreate itself.
		//nolint:errcheck // AckNone consumer; nothing actionable on failure
		_ = msg.Nak()

		return nil, err
	}

	// Ordered consumers don't track ack state server-side (AckNone
	// policy). The only possible error is a connection write failure,
	// which triggers automatic consumer recreation.
	//nolint:errcheck // AckNone consumer; nothing actionable on failure
	_ = msg.Ack()

	// Check if we've completed the initial replay
	w.stateMu.Lock()
	if w.initial {
		w.received++

		md, merr := msg.Metadata()
		if w.received >= w.pending || (merr == nil && md.NumPending == 0) {
			w.initial = false
			w.sentinel = true
		}
	}
	w.stateMu.Unlock()

	return entry, nil
}

// InitialDone reports whether the initial replay has completed. After
// this returns true, all entries from Next are live updates.
func (w *Watcher) InitialDone() bool {
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()

	return !w.initial
}

// Stop tears down the watcher and its underlying consumer.
func (w *Watcher) Stop() {
	w.stopOnce.Do(func() {
		w.iter.Stop()
		w.cancel()
	})
}

// WatchAll is a convenience for watching all keys in the bucket.
func (b *Bucket) WatchAll(ctx context.Context, opts ...WatchOption) (*Watcher, error) {
	return b.Watch(ctx, subjectWildcard, opts...)
}

// Updates returns a channel adapter around the pull-based watcher for callers
// that prefer channel semantics. Multiple adapters and concurrent Next calls
// share one delivery stream; each entry is sent to exactly one consumer. The
// channel closes when the watcher is stopped or the context is canceled. A nil
// *Entry signals end of initial replay.
func (w *Watcher) Updates() <-chan *Entry {
	ch := make(chan *Entry, updatesChanBuffer)

	go func() {
		defer close(ch)

		for {
			entry, err := w.Next()
			if err != nil {
				return
			}

			select {
			case ch <- entry:
			case <-w.done:
				return
			}
		}
	}()

	return ch
}

// entryFromJSMsg decodes a consumed stream message (watch path) into an Entry.
func (b *Bucket) entryFromJSMsg(msg jetstream.Msg) (*Entry, error) {
	md, err := msg.Metadata()
	if err != nil {
		return nil, fmt.Errorf("kv: watch: message metadata: %w", err)
	}

	hdrs := msg.Headers()

	return &Entry{
		Bucket:      b.name,
		Key:         b.key(msg.Subject()),
		Value:       msg.Data(),
		Revision:    md.Sequence.Stream,
		Delta:       md.NumPending,
		Created:     md.Timestamp,
		Operation:   opFromHeader(hdrs),
		ContentType: hdrs.Get(hdrContentType),
		Headers:     hdrs,
	}, nil
}
