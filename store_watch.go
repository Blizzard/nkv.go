package nkv

import (
	"context"
	"strings"
	"sync"
)

// StoreEntry is an entry decoded as T. Entry retains the metadata and encoded
// bytes. Tombstones and metadata-only entries have the zero value of T.
type StoreEntry[T any] struct {
	Entry

	Value T
}

// StoreWatcher decodes each entry delivered by a Watcher as T.
type StoreWatcher[T any] struct {
	watcher     *Watcher
	store       *Store
	metaOnly    bool
	stateMu     sync.RWMutex
	terminated  bool
	err         error
	updatesOnce sync.Once
	updates     chan *StoreEntry[T]
}

// Watch starts a watcher decoding matching values as T. The pattern and any
// additional patterns must select values compatible with T. Content-Type
// selects the codec, not the Go type. Call Stop when the watcher is no longer
// needed. Updates closes on any error; use Err to inspect the cause.
func (s *Store) Watch[T any](ctx context.Context, pattern string, opts ...WatchOption) (*StoreWatcher[T], error) {
	var configured watchOpts
	for _, opt := range opts {
		opt.applyWatch(&configured)
	}

	watcher, err := s.kv.Watch(ctx, s.prefix+pattern, append(opts, prefixWatchFilters(s.prefix))...)
	if err != nil {
		return nil, err
	}

	return &StoreWatcher[T]{watcher: watcher, store: s, metaOnly: configured.headersOnly}, nil
}

// Next blocks until the next entry is available. It returns nil, nil once at
// the end of the initial replay, matching Watcher.Next. Decode failures consume
// the offending entry and return an error; subsequent calls can continue.
// Errors returned directly by Next are not retained by Err.
func (w *StoreWatcher[T]) Next() (*StoreEntry[T], error) {
	entry, err := w.watcher.Next()
	if err != nil || entry == nil {
		return nil, err
	}

	typed := &StoreEntry[T]{Entry: *entry}
	typed.Key = strings.TrimPrefix(entry.Key, w.store.prefix)
	if entry.IsTombstone() || w.metaOnly {
		return typed, nil
	}

	typed.Value, err = w.store.decodeValue[T](entry)
	if err != nil {
		return nil, err
	}

	return typed, nil
}

// InitialDone reports whether the initial replay has completed.
func (w *StoreWatcher[T]) InitialDone() bool {
	return w.watcher.InitialDone()
}

// Err reports the error that ended Updates, including a decoding failure, or
// the watch context's cancellation or deadline error after shutdown. It returns
// nil before termination and after an explicit Stop with an active watch
// context. Cleanup does not overwrite a recorded failure. The original error
// chain is preserved for errors.Is and errors.As. Err is safe to call
// concurrently with all watcher methods.
//
// Errors returned directly by Next are not retained. The initial-replay nil
// entry is not a failure and does not change Err.
func (w *StoreWatcher[T]) Err() error {
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()

	if w.terminated {
		return w.err
	}
	return w.watcher.Err()
}

func (w *StoreWatcher[T]) finish(err error) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	if !w.terminated {
		w.err = w.watcher.terminationError(err)
		w.terminated = true
	}
}

// Stop tears down the watcher and its underlying consumer. An explicit stop
// with an active watch context leaves Err nil unless a failure is already
// recorded. If the watch context has ended, Err retains its cancellation or
// deadline error instead. Stop does not wait for Updates to close.
func (w *StoreWatcher[T]) Stop() {
	w.watcher.Stop()
}

// Updates returns a channel adapter. Repeated calls return the same channel.
// A nil entry signals the end of the initial replay. The channel closes on any
// error, including a decode error. Call Stop even after the channel closes.
// Err retains the cause before the channel closes.
// Do not mix Updates consumption with calls to Next.
func (w *StoreWatcher[T]) Updates() <-chan *StoreEntry[T] {
	w.updatesOnce.Do(func() {
		w.updates = make(chan *StoreEntry[T], updatesChanBuffer)

		go func() {
			defer close(w.updates)

			for {
				entry, err := w.Next()
				if err != nil {
					w.finish(err)
					return
				}

				select {
				case w.updates <- entry:
				case <-w.watcher.done:
					w.finish(nil)
					return
				}
			}
		}()
	})

	return w.updates
}

type prefixWatchFilters string

func (prefix prefixWatchFilters) applyWatch(opts *watchOpts) {
	for i, filter := range opts.extraFilters {
		opts.extraFilters[i] = string(prefix) + filter
	}
}
