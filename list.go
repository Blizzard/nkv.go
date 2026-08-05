package nkv

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strconv"

	"github.com/nats-io/nats.go"
)

const defaultListBatch = 256

// List returns an iterator over all entries matching the key pattern (e.g.
// "users.*" or ">" for all). Tombstones are skipped unless WithDeletes() is
// specified. Serves from any replica via Direct Get — zero consumers.
//
// Uses paged next_by_subj direct get (batched stream walk). Pins to the
// stream's last_seq at call time for a consistent snapshot. No subject
// count limits, no consumer lifecycle.
func (b *Bucket) List(ctx context.Context, pattern string, opts ...ListOption) iter.Seq2[*Entry, error] {
	o := listOpts{batch: defaultListBatch}

	for _, opt := range opts {
		opt.applyList(&o)
	}

	return b.list(ctx, pattern, o)
}

func (b *Bucket) list(ctx context.Context, pattern string, o listOpts) iter.Seq2[*Entry, error] {
	if err := validateListOptions(o); err != nil {
		return func(yield func(*Entry, error) bool) {
			yield(nil, err)
		}
	}

	info, err := b.js.Stream(ctx, b.stream)
	if err != nil {
		return func(yield func(*Entry, error) bool) {
			yield(nil, fmt.Errorf("kv: list: %w", err))
		}
	}

	return b.listWalk(ctx, pattern, info.CachedInfo().State.LastSeq, o)
}

// Keys returns an iterator over all keys matching the pattern. Tombstones
// are skipped. A thin wrapper around List that discards values.
func (b *Bucket) Keys(ctx context.Context, pattern string, opts ...KeysOption) iter.Seq2[string, error] {
	o := listOpts{batch: defaultListBatch}

	for _, opt := range opts {
		opt.applyKeys(&o)
	}

	entries := b.list(ctx, pattern, o)

	return func(yield func(string, error) bool) {
		for entry, err := range entries {
			if err != nil {
				if !yield("", err) {
					return
				}

				continue
			}

			if entry.IsTombstone() {
				continue
			}

			if !yield(entry.Key, nil) {
				return
			}
		}
	}
}

// listWalk pages through the stream using next_by_subj + batch (ADR-31
// Option B). Pins up_to_seq for a consistent snapshot. Streams entries
// directly from the subscription without intermediate buffering.
func (b *Bucket) listWalk(ctx context.Context, pattern string, upToSeq uint64, o listOpts) iter.Seq2[*Entry, error] {
	return func(yield func(*Entry, error) bool) {
		subject := b.subject(pattern)
		seq := uint64(1)

		if upToSeq == 0 {
			return // empty stream
		}

		for seq <= upToSeq {
			lastSeq, more := b.streamPage(ctx, subject, seq, upToSeq, o, yield)
			if !more {
				return
			}

			if lastSeq == 0 || lastSeq >= upToSeq {
				return
			}

			seq = lastSeq + 1
		}
	}
}

// requestPage publishes one direct get page request and returns the inbox
// subscription that will receive the response messages. The caller owns the
// returned subscription and must unsubscribe.
func (b *Bucket) requestPage(subject string, seq, upToSeq uint64, o listOpts) (*nats.Subscription, error) {
	payload, err := json.Marshal(directGetReq{
		NextBySubj: subject,
		Batch:      o.batch,
		Seq:        seq,
		UpToSeq:    upToSeq,
	})
	if err != nil {
		return nil, fmt.Errorf("kv: list: encode request: %w", err)
	}

	sub, err := b.nc.SubscribeSync(nats.NewInbox())
	if err != nil {
		return nil, fmt.Errorf("kv: list: subscribe: %w", err)
	}

	if err := b.nc.PublishRequest(b.directGetSubject(), sub.Subject, payload); err != nil {
		//nolint:errcheck // best-effort cleanup on the error path
		_ = sub.Unsubscribe()

		return nil, fmt.Errorf("kv: list: publish: %w", err)
	}

	return sub, nil
}

// streamPage sends one direct get page request and yields entries one by one
// from the subscription. Returns the last sequence seen (for pagination
// resume) and whether more pages may follow (false on error or yield refusal).
func (b *Bucket) streamPage(ctx context.Context, subject string, seq, upToSeq uint64, o listOpts, yield func(*Entry, error) bool) (uint64, bool) {
	var lastSeq uint64

	sub, err := b.requestPage(subject, seq, upToSeq, o)
	if err != nil {
		yield(nil, err)

		return 0, false
	}

	defer func() {
		//nolint:errcheck // best-effort cleanup; the subscription is already going out of scope
		_ = sub.Unsubscribe()
	}()

	for {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			yield(nil, fmt.Errorf("kv: list: %w", err))

			return lastSeq, false
		}

		status := msg.Header.Get(hdrStatus)

		switch status {
		case statusEOB:
			if s := msg.Header.Get(hdrUpToSeq); s != "" {
				if upTo, perr := strconv.ParseUint(s, 10, 64); perr == nil {
					lastSeq = upTo
				}
			}

			return lastSeq, true

		case statusNotFound:
			return lastSeq, true

		case "":
			// Data message, fall through.

		default:
			yield(nil, fmt.Errorf("kv: list: status %s: %s", status, msg.Header.Get(hdrDescription)))

			return lastSeq, false
		}

		entry, err := b.entryFromDirectMsg(msg)
		if err != nil {
			yield(nil, err)

			return lastSeq, false
		}

		if entry.Revision > upToSeq {
			return lastSeq, false
		}

		lastSeq = entry.Revision

		if entry.IsTombstone() && !o.includeDeletes {
			continue
		}

		if !yield(entry, nil) {
			return lastSeq, false
		}
	}
}
