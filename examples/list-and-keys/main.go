package main

import (
	"context"
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

	bucketName := fmt.Sprintf("EXAMPLE_LIST_%d", time.Now().UnixNano())
	bucket, bucketErr := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: bucketName})
	if bucketErr != nil {
		return fmt.Errorf("create bucket: %w", bucketErr)
	}

	// Map iteration order is deliberately unimportant here; listing reflects
	// stream sequence order, which depends on the order these writes arrive.
	values := map[string]string{
		"app.production": "v3.2.0",
		"app.staging":    "v3.3.0-rc1",
		"database.host":  "db.internal",
	}
	for key, value := range values {
		if _, putErr := bucket.Put(ctx, key, []byte(value)); putErr != nil {
			return fmt.Errorf("put %s: %w", key, putErr)
		}
	}

	// Delete records a tombstone as the key's latest entry. It does not make
	// that deletion invisible to callers that explicitly request tombstones.
	if deleteErr := bucket.Delete(ctx, "app.staging"); deleteErr != nil {
		return fmt.Errorf("delete app.staging: %w", deleteErr)
	}

	log.Print("live app keys:")
	// "app.*" matches one token after "app". Keys yields only keys whose latest
	// entry is live; each iterator item carries its own possible error.
	for key, keyErr := range bucket.Keys(ctx, "app.*") {
		if keyErr != nil {
			return fmt.Errorf("list keys: %w", keyErr)
		}
		log.Printf("  %s", key)
	}

	log.Print("app entries, including tombstones:")
	// List yields full Entry values. WithDeletes includes DEL and PURGE markers,
	// which is useful for synchronization and auditing workflows.
	for entry, listErr := range bucket.List(ctx, "app.*", nkv.WithDeletes()) {
		if listErr != nil {
			return fmt.Errorf("list entries: %w", listErr)
		}
		log.Printf("  %-14s operation=%-6s value=%q", entry.Key, entry.Operation, entry.Value)
	}

	return nil
}

func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}

	return nats.DefaultURL
}
