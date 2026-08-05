package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/blizzard/nkv.go"
)

const exampleTimeout = 15 * time.Second

func main() {
	log.SetFlags(0)

	if err := run(); err != nil {
		log.Printf("example failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	// Expiration is asynchronous. The longer timeout prevents an unsupported or
	// misconfigured server from making the example wait forever.
	ctx, cancel := context.WithTimeout(context.Background(), exampleTimeout)
	defer cancel()

	nc, connectErr := nats.Connect(natsURL())
	if connectErr != nil {
		return fmt.Errorf("connect to NATS: %w", connectErr)
	}
	defer nc.Close()

	bucketName := fmt.Sprintf("EXAMPLE_TTL_%d", time.Now().UnixNano())
	bucket, bucketErr := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: bucketName})
	if bucketErr != nil {
		return fmt.Errorf("create bucket: %w", bucketErr)
	}

	// UpdatesOnly skips an initial snapshot. Because the watch starts before the
	// write, it will receive both the PUT and the later expiration marker.
	watcher, watchErr := bucket.Watch(ctx, "session.*", nkv.WithUpdatesOnly())
	if watchErr != nil {
		return fmt.Errorf("watch sessions: %w", watchErr)
	}
	defer watcher.Stop()

	// WithTTL applies to this revision only. NATS Server removes it after roughly
	// two seconds and publishes a limit marker for observers.
	if _, putErr := bucket.Put(ctx, "session.ada", []byte("active"), nkv.WithTTL(2*time.Second)); putErr != nil {
		return fmt.Errorf("put session: %w", putErr)
	}

	written, writeErr := watcher.Next()
	if writeErr != nil {
		return fmt.Errorf("read put event: %w", writeErr)
	}
	log.Printf("write event: key=%s operation=%s value=%q", written.Key, written.Operation, written.Value)

	// nkv translates the server-generated limit marker into a tombstone Entry.
	expired, expirationErr := watcher.Next()
	if expirationErr != nil {
		return fmt.Errorf("wait for expiration: %w", expirationErr)
	}
	log.Printf("expiration event: key=%s operation=%s tombstone=%t", expired.Key, expired.Operation, expired.IsTombstone())

	// Tombstones are observable by watches and listings, but point reads treat
	// the expired key as missing.
	_, missingErr := bucket.Get(ctx, "session.ada")
	log.Printf("get after expiration reports key missing: %t", errors.Is(missingErr, nkv.ErrKeyNotFound))

	return nil
}

func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}

	return nats.DefaultURL
}
