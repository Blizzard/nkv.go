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

	bucketName := fmt.Sprintf("EXAMPLE_WATCH_%d", time.Now().UnixNano())
	bucket, bucketErr := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: bucketName})
	if bucketErr != nil {
		return fmt.Errorf("create bucket: %w", bucketErr)
	}

	// Values written before Watch starts are delivered as its initial replay.
	// Map iteration means their replay order is intentionally unspecified.
	for key, value := range map[string]string{
		"config.color": "blue",
		"config.size":  "medium",
	} {
		if _, seedErr := bucket.Put(ctx, key, []byte(value)); seedErr != nil {
			return fmt.Errorf("seed %s: %w", key, seedErr)
		}
	}

	// Watch uses a pull-based ordered consumer, so the caller controls the pace
	// by calling Next. Stop releases the consumer resources when we are done.
	watcher, watchErr := bucket.Watch(ctx, "config.>")
	if watchErr != nil {
		return fmt.Errorf("watch config: %w", watchErr)
	}
	defer watcher.Stop()

	log.Print("initial values:")
	for {
		entry, nextErr := watcher.Next()
		if nextErr != nil {
			return fmt.Errorf("read initial values: %w", nextErr)
		}

		// nil, nil is the documented sentinel separating the initial replay from
		// live updates. It does not mean the watcher has stopped.
		if entry == nil {
			break
		}
		log.Printf("  %s=%q", entry.Key, entry.Value)
	}

	// This write happens after the replay boundary, so the next event is a live
	// update rather than part of the initial snapshot.
	if _, updateErr := bucket.Put(ctx, "config.color", []byte("green")); updateErr != nil {
		return fmt.Errorf("update color: %w", updateErr)
	}

	entry, liveErr := watcher.Next()
	if liveErr != nil {
		return fmt.Errorf("read live update: %w", liveErr)
	}
	log.Printf("live update: %s=%q at revision %d", entry.Key, entry.Value, entry.Revision)

	return nil
}

func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}

	return nats.DefaultURL
}
