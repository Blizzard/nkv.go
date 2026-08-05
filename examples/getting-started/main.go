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

func main() {
	log.SetFlags(0)

	if err := run(); err != nil {
		log.Printf("example failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	// Bound every server operation so an unavailable server cannot leave the
	// example hanging indefinitely.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// NATS_URL makes the example usable with a remote server while retaining a
	// convenient local default.
	nc, connectErr := nats.Connect(natsURL())
	if connectErr != nil {
		return fmt.Errorf("connect to NATS: %w", connectErr)
	}
	defer nc.Close()

	// A unique name makes repeated runs independent, even when the NATS server
	// keeps its JetStream data between runs.
	bucketName := fmt.Sprintf("EXAMPLE_GETTING_STARTED_%d", time.Now().UnixNano())
	bucket, bucketErr := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: bucketName})
	if bucketErr != nil {
		return fmt.Errorf("create bucket: %w", bucketErr)
	}

	// Put stores bytes and returns the stream revision assigned to this write.
	revision, putErr := bucket.Put(ctx, "greeting", []byte("hello, nkv"))
	if putErr != nil {
		return fmt.Errorf("put greeting: %w", putErr)
	}
	log.Printf("put greeting at revision %d", revision)

	// Get returns an Entry containing both the value and KV metadata such as
	// its revision and creation time.
	entry, getErr := bucket.Get(ctx, "greeting")
	if getErr != nil {
		return fmt.Errorf("get greeting: %w", getErr)
	}
	log.Printf("get greeting: %q (revision %d)", entry.Value, entry.Revision)

	if deleteErr := bucket.Delete(ctx, "greeting"); deleteErr != nil {
		return fmt.Errorf("delete greeting: %w", deleteErr)
	}

	// A deleted key reads as missing. errors.Is is the stable way to recognize
	// nkv sentinel errors even when an operation adds context to them.
	_, missingErr := bucket.Get(ctx, "greeting")
	log.Printf("get after delete reports key missing: %t", errors.Is(missingErr, nkv.ErrKeyNotFound))

	return nil
}

func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}

	return nats.DefaultURL
}
