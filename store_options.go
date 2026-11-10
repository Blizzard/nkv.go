package nkv

import (
	"fmt"
	"strings"
	"time"
)

type storeConfig struct {
	codec      *Codec
	fallback   *Codec
	codecs     []Codec
	prefix     string
	defaultTTL time.Duration
}

// StoreOption configures a Store or StoreTx independently of its value types.
type StoreOption func(*storeConfig)

// WithWriteCodec sets the codec used to encode Put/Create/Update values.
// Its ContentType is stamped into the Content-Type header and registered
// for read dispatch. If not specified, JSONCodec() is used.
func WithWriteCodec(c Codec) StoreOption {
	return func(cfg *storeConfig) {
		cfg.codec = &c
	}
}

// WithFallbackCodec sets the codec used to decode entries with an absent or
// unknown Content-Type. If not specified, the write codec is used.
func WithFallbackCodec(c Codec) StoreOption {
	return func(cfg *storeConfig) {
		cfg.fallback = &c
	}
}

// WithReadCodec registers an additional codec for Content-Type read dispatch.
// May be specified multiple times for different content types.
func WithReadCodec(c Codec) StoreOption {
	return func(cfg *storeConfig) {
		cfg.codecs = append(cfg.codecs, c)
	}
}

// WithPrefix prepends a key prefix with a dot separator to all key operations.
// Empty prefixes and prefixes containing only dots disable prefixing. Other
// trailing dots are removed. It panics if the normalized prefix is invalid.
func WithPrefix(prefix string) StoreOption {
	return func(cfg *storeConfig) {
		cfg.prefix = normalizePrefix(prefix)
	}
}

// WithDefaultTTL sets the default TTL for Store.Put/Create/Update unless the
// operation explicitly specifies WithTTL. It does not apply to Delete or
// transactions; use WithTxTTL to configure transaction TTL.
func WithDefaultTTL(ttl time.Duration) StoreOption {
	return func(cfg *storeConfig) {
		cfg.defaultTTL = ttl
	}
}

func configureStore(opts []StoreOption) storeConfig {
	var cfg storeConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg
}

func (cfg storeConfig) writeCodec() Codec {
	codec := JSONCodec()
	if cfg.codec != nil {
		codec = *cfg.codec
	}
	validateCodec(codec)

	return codec
}

func normalizePrefix(prefix string) string {
	normalized := strings.TrimRight(prefix, ".")
	if normalized == "" {
		return ""
	}

	if !validKey(normalized) || strings.Contains(normalized, "..") {
		panic(fmt.Sprintf("kv: invalid prefix %q", prefix))
	}

	return normalized + "."
}

func validateCodec(codec Codec) {
	if codec.ContentType == "" {
		panic("kv: codec ContentType must not be empty")
	}
	if codec.Marshal == nil {
		panic("kv: codec Marshal must not be nil")
	}
	if codec.Unmarshal == nil {
		panic("kv: codec Unmarshal must not be nil")
	}
}
