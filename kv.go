// Package nkv is a replacement for the nats.go jetstream.KeyValue client. It
// is wire compatible with the standard KV bucket layout (stream KV_<bucket>,
// subjects $KV.<bucket>.<key>, KV-Operation headers, rollup purges) but NOT
// API compatible with nats.go.
//
// Design goals:
//   - every operation takes variadic options so the surface can grow
//   - per-key TTL on Put/Create/Delete/Purge (Nats-TTL)
//   - List/Keys via JetStream Direct Get (no ephemeral consumers)
//   - Watch via pull ordered consumers (no push, native backpressure)
//   - typed values via a generic codec wrapper instead of a []byte-only API
package nkv

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Bucket is a handle to a KV bucket. It is wire compatible with buckets
// created by nats.go's jetstream.KeyValue implementation.
type Bucket struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	name   string
	stream string
	prefix string // "$KV.<bucket>."
}

// Config describes a bucket on creation. Accepts a jetstream.StreamConfig
// for full control over non-KV-specific stream settings (replicas, storage,
// max bytes, placement, mirror, sources, etc.).
//
// KV-invariant fields are enforced/overwritten:
//   - Name (set to KV_<Bucket>)
//   - Subjects (set to $KV.<Bucket>.>)
//   - AllowRollup (true)
//   - DenyDelete (true)
//   - AllowDirect (true — required for consumer-free list)
//   - AllowMsgTTL (true — required for per-key TTL)
//   - SubjectDeleteMarkerTTL (must be at least 1s — required for TTL expiry notifications)
//   - AllowAtomicPublish (true — required for batch writes)
//   - Discard (DiscardNew)
//
// Fields given reasonable defaults if zero:
//   - MaxMsgsPerSubject → 1 (history=1)
//   - Replicas → 1
//   - Storage → FileStorage
//   - Duplicates → 2m
//   - SubjectDeleteMarkerTTL → 1m
//
// Callers who want the simplest config can pass just a Bucket name and
// leave StreamConfig at its zero value.
type Config struct {
	// StreamConfig carries all non-KV-specific stream settings. Fields
	// that conflict with KV semantics will be overwritten or rejected.
	jetstream.StreamConfig

	// Bucket is the KV bucket name. Required. Must match [a-zA-Z0-9_-]+.
	Bucket string
}

const (
	kvSubjectPrefix               = "$KV."
	defaultSubjectDeleteMarkerTTL = time.Minute
)

var (
	validBucketRe = regexp.MustCompile(`\A[a-zA-Z0-9_-]+\z`)
	validKeyRe    = regexp.MustCompile(`\A[-/_=.a-zA-Z0-9]+\z`)
)

func validKey(key string) bool {
	return validKeyRe.MatchString(key) && !strings.HasPrefix(key, ".") && !strings.HasSuffix(key, ".")
}

func streamName(bucket string) string {
	return streamPrefix + bucket
}

func normalizeSubjectDeleteMarkerTTL(ttl time.Duration) (time.Duration, error) {
	if ttl < 0 || (ttl > 0 && ttl < time.Second) {
		return 0, fmt.Errorf("kv: SubjectDeleteMarkerTTL must be zero or at least 1s, got %s", ttl)
	}
	if ttl == 0 {
		return defaultSubjectDeleteMarkerTTL, nil
	}

	return ttl, nil
}

func validateMarkerSupport(bucket string, cfg jetstream.StreamConfig) error {
	if !cfg.AllowMsgTTL {
		return fmt.Errorf("kv: bucket %q does not allow per-key TTL (allow_msg_ttl=false)", bucket)
	}
	if cfg.SubjectDeleteMarkerTTL <= 0 {
		return fmt.Errorf("kv: bucket %q does not retain TTL expiry markers (subject_delete_marker_ttl=%s)", bucket, cfg.SubjectDeleteMarkerTTL)
	}

	return nil
}

func newBucket(nc *nats.Conn, name string) (*Bucket, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("kv: create jetstream context: %w", err)
	}

	return &Bucket{
		nc:     nc,
		js:     js,
		name:   name,
		stream: streamName(name),
		prefix: kvSubjectPrefix + name + ".",
	}, nil
}

// CreateBucket creates a KV bucket or updates its existing backing stream and
// returns a handle to it. Updating applies the required nkv settings, allowing
// an existing KV stream to be upgraded to nkv compatibility without manually
// modifying its stream configuration.
//
// KV-specific invariants are enforced on the StreamConfig: conflicting values
// are rejected with an error, unset fields get correct defaults.
func CreateBucket(ctx context.Context, nc *nats.Conn, cfg Config) (*Bucket, error) {
	if !validBucketRe.MatchString(cfg.Bucket) {
		return nil, fmt.Errorf("kv: invalid bucket name %q", cfg.Bucket)
	}

	b, err := newBucket(nc, cfg.Bucket)
	if err != nil {
		return nil, err
	}

	sc := cfg.StreamConfig

	// --- Reject conflicting settings ---
	if sc.Name != "" {
		return nil, fmt.Errorf("kv: StreamConfig.Name must not be set (use Config.Bucket); got %q", sc.Name)
	}

	if sc.MaxMsgsPerSubject != 0 && sc.MaxMsgsPerSubject != 1 {
		return nil, fmt.Errorf("kv: MaxMsgsPerSubject (history) must be 1, got %d", sc.MaxMsgsPerSubject)
	}

	if sc.Retention != 0 && sc.Retention != jetstream.LimitsPolicy {
		return nil, fmt.Errorf("kv: Retention must be LimitsPolicy for KV buckets, got %v", sc.Retention)
	}

	sc.SubjectDeleteMarkerTTL, err = normalizeSubjectDeleteMarkerTTL(sc.SubjectDeleteMarkerTTL)
	if err != nil {
		return nil, err
	}

	// --- KV invariants: overwrite unconditionally ---
	sc.Name = b.stream
	sc.Subjects = []string{b.prefix + subjectWildcard}
	sc.AllowRollup = true
	sc.DenyDelete = true
	sc.AllowDirect = true
	sc.AllowMsgTTL = true
	sc.AllowAtomicPublish = true
	sc.Discard = jetstream.DiscardNew
	sc.Retention = jetstream.LimitsPolicy
	sc.MaxMsgsPerSubject = 1

	// --- Defaults for unset fields ---
	if sc.Replicas == 0 {
		sc.Replicas = 1
	}

	if sc.Storage == 0 {
		sc.Storage = jetstream.FileStorage
	}

	if sc.Duplicates == 0 {
		sc.Duplicates = 2 * time.Minute
	}

	if _, err := b.js.CreateOrUpdateStream(ctx, sc); err != nil {
		return nil, fmt.Errorf("kv: create bucket %q: %w", cfg.Bucket, err)
	}

	return b, nil
}

// Open returns a handle to an existing bucket. It verifies the backing stream
// exists and has every configuration setting required by the nkv API.
func Open(ctx context.Context, nc *nats.Conn, bucket string) (*Bucket, error) {
	if !validBucketRe.MatchString(bucket) {
		return nil, fmt.Errorf("kv: invalid bucket name %q", bucket)
	}

	b, err := newBucket(nc, bucket)
	if err != nil {
		return nil, err
	}

	s, err := b.js.Stream(ctx, b.stream)
	if err != nil {
		return nil, fmt.Errorf("kv: open bucket %q: %w", bucket, err)
	}

	info := s.CachedInfo()
	if info == nil {
		return nil, fmt.Errorf("kv: open bucket %q: stream info unavailable", bucket)
	}

	cfg := info.Config
	expectedSubject := b.prefix + subjectWildcard
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != expectedSubject {
		return nil, fmt.Errorf("kv: bucket %q has incompatible subjects %v (want [%s])", bucket, cfg.Subjects, expectedSubject)
	}

	if !cfg.AllowRollup {
		return nil, fmt.Errorf("kv: bucket %q does not allow rollup (allow_rollup=false)", bucket)
	}

	if !cfg.DenyDelete {
		return nil, fmt.Errorf("kv: bucket %q permits direct deletion (deny_delete=false)", bucket)
	}

	if !cfg.AllowDirect {
		return nil, fmt.Errorf("kv: bucket %q does not allow direct get (allow_direct=false)", bucket)
	}

	if err := validateMarkerSupport(bucket, cfg); err != nil {
		return nil, err
	}

	if !cfg.AllowAtomicPublish {
		return nil, fmt.Errorf("kv: bucket %q does not allow atomic publish (allow_atomic_publish=false)", bucket)
	}

	if cfg.Discard != jetstream.DiscardNew {
		return nil, fmt.Errorf("kv: bucket %q does not discard new messages (discard=%v)", bucket, cfg.Discard)
	}

	if cfg.Retention != jetstream.LimitsPolicy {
		return nil, fmt.Errorf("kv: bucket %q does not use limits retention (retention=%v)", bucket, cfg.Retention)
	}

	if cfg.MaxMsgsPerSubject != 1 {
		return nil, fmt.Errorf("kv: bucket %q does not have history=1 (max_msgs_per_subject=%d)", bucket, cfg.MaxMsgsPerSubject)
	}

	return b, nil
}

// Name returns the bucket name.
func (b *Bucket) Name() string {
	return b.name
}

// Stream returns the backing stream name (KV_<bucket>).
func (b *Bucket) Stream() string {
	return b.stream
}

// Status fetches live (non-cached) stream info for the backing stream.
func (b *Bucket) Status(ctx context.Context) (*jetstream.StreamInfo, error) {
	s, err := b.js.Stream(ctx, b.stream)
	if err != nil {
		return nil, fmt.Errorf("kv: status %q: %w", b.name, err)
	}

	info, err := s.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("kv: status %q: %w", b.name, err)
	}

	return info, nil
}

// IsClusterLocal reports whether the bucket's stream is hosted by the same
// cluster the connection is attached to. If either side reports no cluster
// information (e.g. a single non-clustered server), the bucket is considered
// local.
func (b *Bucket) IsClusterLocal(ctx context.Context) (bool, error) {
	info, err := b.Status(ctx)
	if err != nil {
		return false, err
	}

	connCluster := b.nc.ConnectedClusterName()
	if info.Cluster == nil || info.Cluster.Name == "" || connCluster == "" {
		return true, nil
	}

	return info.Cluster.Name == connCluster, nil
}

// subject maps a key to its stream subject.
func (b *Bucket) subject(key string) string {
	return b.prefix + key
}

// key maps a stream subject back to its key.
func (b *Bucket) key(subject string) string {
	return strings.TrimPrefix(subject, b.prefix)
}
