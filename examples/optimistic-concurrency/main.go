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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, connectErr := nats.Connect(natsURL())
	if connectErr != nil {
		return fmt.Errorf("connect to NATS: %w", connectErr)
	}
	defer nc.Close()

	bucketName := fmt.Sprintf("EXAMPLE_CAS_%d", time.Now().UnixNano())
	bucket, bucketErr := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: bucketName})
	if bucketErr != nil {
		return fmt.Errorf("create bucket: %w", bucketErr)
	}

	// Create is a compare-and-set operation for a missing key. It succeeds only
	// when no live value currently exists.
	revision, createErr := bucket.Create(ctx, "inventory.widget", []byte("10"))
	if createErr != nil {
		return fmt.Errorf("create inventory: %w", createErr)
	}
	log.Printf("created inventory at revision %d", revision)

	// A second creator loses without replacing the first writer's value.
	_, existsErr := bucket.Create(ctx, "inventory.widget", []byte("20"))
	log.Printf("second create reports key exists: %t", errors.Is(existsErr, nkv.ErrKeyExists))

	// Update writes only if the latest revision still matches the revision that
	// this process observed. A successful write returns a new revision.
	newRevision, updateErr := bucket.Update(ctx, "inventory.widget", []byte("9"), revision)
	if updateErr != nil {
		return fmt.Errorf("update inventory: %w", updateErr)
	}
	log.Printf("updated inventory at revision %d", newRevision)

	// Reusing the original revision simulates another writer acting on stale
	// state. The server rejects it rather than overwriting the value "9".
	_, staleErr := bucket.Update(ctx, "inventory.widget", []byte("8"), revision)
	log.Printf("stale update reports revision mismatch: %t", errors.Is(staleErr, nkv.ErrRevisionMismatch))

	entry, getErr := bucket.Get(ctx, "inventory.widget")
	if getErr != nil {
		return fmt.Errorf("get inventory: %w", getErr)
	}
	log.Printf("current inventory is %q at revision %d", entry.Value, entry.Revision)

	return nil
}

func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}

	return nats.DefaultURL
}
