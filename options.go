package nkv

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Every operation accepts variadic options. Options are interfaces so a
// single option (e.g. WithTTL) can apply to several operations, and new
// options can be added without breaking call sites.

type getOpts struct {
	revision uint64
}

type putOpts struct {
	ttl     time.Duration
	headers nats.Header
}

type createOpts struct {
	ttl     time.Duration
	headers nats.Header
}

type updateOpts struct {
	ttl     time.Duration
	headers nats.Header
}

type deleteOpts struct {
	ttl              time.Duration
	ttlSet           bool
	expectedRevision uint64
	headers          nats.Header
}

type purgeOpts struct {
	ttl              time.Duration
	expectedRevision uint64
	headers          nats.Header
}

type listOpts struct {
	batch          int
	includeDeletes bool
}

type watchOpts struct {
	updatesOnly  bool
	headersOnly  bool
	extraFilters []string
	pullBatch    int
	inactive     time.Duration
	revision     uint64
}

// GetOption configures Get.
type GetOption interface {
	applyGet(o *getOpts)
}

// PutOption configures Put.
type PutOption interface {
	applyPut(o *putOpts)
}

// CreateOption configures Create.
type CreateOption interface {
	applyCreate(o *createOpts)
}

// UpdateOption configures Update.
type UpdateOption interface {
	applyUpdate(o *updateOpts)
}

// DeleteOption configures Delete.
type DeleteOption interface {
	applyDelete(o *deleteOpts)
}

// PurgeOption configures Purge.
type PurgeOption interface {
	applyPurge(o *purgeOpts)
}

// ListOption configures List.
type ListOption interface {
	applyList(o *listOpts)
}

// KeysOption configures Keys.
type KeysOption interface {
	applyKeys(o *listOpts)
}

// WatchOption configures Watch.
type WatchOption interface {
	applyWatch(o *watchOpts)
}

// TTL applies to Put, Create, Update, Delete (tombstone) and Purge.

type ttlOption time.Duration

func (t ttlOption) applyPut(o *putOpts) {
	o.ttl = time.Duration(t)
}

func (t ttlOption) applyCreate(o *createOpts) {
	o.ttl = time.Duration(t)
}

func (t ttlOption) applyUpdate(o *updateOpts) {
	o.ttl = time.Duration(t)
}

func (t ttlOption) applyDelete(o *deleteOpts) {
	o.ttl = time.Duration(t)
	o.ttlSet = true
}

func (t ttlOption) applyPurge(o *purgeOpts) {
	o.ttl = time.Duration(t)
}

// WithTTL sets a per-key TTL (Nats-TTL) on the written revision. On
// Delete/Purge it bounds the tombstone's lifetime. Zero disables TTL; positive
// values must be at least one second. Requires AllowMsgTTL on the bucket (set
// by CreateBucket and required by Open).
func WithTTL(d time.Duration) ttlOption {
	return ttlOption(d)
}

func validateTTL(ttl time.Duration) error {
	if ttl < 0 || (ttl > 0 && ttl < time.Second) {
		return fmt.Errorf("%w: TTL must be zero or at least 1s, got %s", ErrInvalidOption, ttl)
	}

	return nil
}

// Revision targeting.

type revisionOption uint64

func (r revisionOption) applyGet(o *getOpts) {
	o.revision = uint64(r)
}

func (r revisionOption) applyDelete(o *deleteOpts) {
	o.expectedRevision = uint64(r)
}

func (r revisionOption) applyPurge(o *purgeOpts) {
	o.expectedRevision = uint64(r)
}

func (r revisionOption) applyWatch(o *watchOpts) {
	o.revision = uint64(r)
}

// WithRevision targets a specific revision. On Get it fetches that exact
// revision; on Delete and Purge it makes the operation conditional on the
// key's latest revision matching; on Watch it resumes inclusively from that
// stream revision.
func WithRevision(rev uint64) revisionOption {
	return revisionOption(rev)
}

// List and Keys paging.

type listBatchOption int

func (n listBatchOption) applyList(o *listOpts) {
	o.batch = int(n)
}

func (n listBatchOption) applyKeys(o *listOpts) {
	o.batch = int(n)
}

// WithListBatch sets the direct-get page size (default 256). It must be
// positive.
func WithListBatch(n int) listBatchOption {
	return listBatchOption(n)
}

func validateListOptions(o listOpts) error {
	if o.batch <= 0 {
		return fmt.Errorf("%w: list batch must be positive, got %d", ErrInvalidOption, o.batch)
	}

	return nil
}

type includeDeletesOption struct{}

func (includeDeletesOption) applyList(o *listOpts) {
	o.includeDeletes = true
}

// WithDeletes makes List yield tombstone entries instead of skipping them.
func WithDeletes() ListOption {
	return includeDeletesOption{}
}

// Watch consumer tuning.

type updatesOnlyOption struct{}

func (updatesOnlyOption) applyWatch(o *watchOpts) {
	o.updatesOnly = true
}

// WithUpdatesOnly skips the initial replay and only delivers new revisions.
func WithUpdatesOnly() WatchOption {
	return updatesOnlyOption{}
}

type headersOnlyOption struct{}

func (headersOnlyOption) applyWatch(o *watchOpts) {
	o.headersOnly = true
}

// WithMetaOnly delivers entries without values (consumer headers_only).
func WithMetaOnly() WatchOption {
	return headersOnlyOption{}
}

type extraFiltersOption []string

func (f extraFiltersOption) applyWatch(o *watchOpts) {
	o.extraFilters = append(o.extraFilters, f...)
}

// WithAdditionalKeys adds more key patterns to the watch — one consumer can
// cover several prefixes (FilterSubjects is plural on pull consumers).
func WithAdditionalKeys(patterns ...string) extraFiltersOption {
	return extraFiltersOption(patterns)
}

type pullBatchOption int

func (n pullBatchOption) applyWatch(o *watchOpts) {
	o.pullBatch = int(n)
}

// WithPullBatch sets the pull batch size for the underlying ordered consumer
// (default 512). It must be positive.
func WithPullBatch(n int) pullBatchOption {
	return pullBatchOption(n)
}

type inactiveThresholdOption time.Duration

func (d inactiveThresholdOption) applyWatch(o *watchOpts) {
	o.inactive = time.Duration(d)
}

// WithInactiveThreshold tunes how long the server keeps the watch consumer
// alive across disconnects before a recreate + replay (default 30s). It must
// be positive.
func WithInactiveThreshold(d time.Duration) inactiveThresholdOption {
	return inactiveThresholdOption(d)
}

func validateWatchOptions(o watchOpts) error {
	if o.pullBatch <= 0 {
		return fmt.Errorf("%w: pull batch must be positive, got %d", ErrInvalidOption, o.pullBatch)
	}

	if o.inactive <= 0 {
		return fmt.Errorf("%w: inactive threshold must be positive, got %s", ErrInvalidOption, o.inactive)
	}

	return nil
}

// User-supplied message headers.

type headersOption struct {
	h nats.Header
}

func (ho headersOption) applyPut(o *putOpts) {
	o.headers = mergeHeaders(o.headers, ho.h)
}

func (ho headersOption) applyCreate(o *createOpts) {
	o.headers = mergeHeaders(o.headers, ho.h)
}

func (ho headersOption) applyUpdate(o *updateOpts) {
	o.headers = mergeHeaders(o.headers, ho.h)
}

func (ho headersOption) applyDelete(o *deleteOpts) {
	o.headers = mergeHeaders(o.headers, ho.h)
}

func (ho headersOption) applyPurge(o *purgeOpts) {
	o.headers = mergeHeaders(o.headers, ho.h)
}

// WithHeaders attaches additional NATS message headers to the write
// operation. Headers set here will not override KV-internal headers
// (KV-Operation, Nats-Rollup, etc.); those take precedence.
func WithHeaders(h nats.Header) headersOption {
	return headersOption{h: h}
}

// mergeHeaders copies src entries into dst, initializing dst if nil.
func mergeHeaders(dst, src nats.Header) nats.Header {
	if dst == nil {
		dst = make(nats.Header, len(src))
	}

	for k, vs := range src {
		dst.Del(k)

		for _, v := range vs {
			dst.Add(k, v)
		}
	}

	return dst
}
