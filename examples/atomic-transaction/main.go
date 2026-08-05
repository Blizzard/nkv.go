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

	bucketName := fmt.Sprintf("EXAMPLE_TX_%d", time.Now().UnixNano())
	bucket, bucketErr := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: bucketName})
	if bucketErr != nil {
		return fmt.Errorf("create bucket: %w", bucketErr)
	}

	// Tx collects operations without making partial values visible. Abort is
	// safe to defer and closes the transaction on every early-return path.
	tx := bucket.Tx()
	defer tx.Abort()

	if stageErr := tx.Put("account.alice", []byte("100")); stageErr != nil {
		return fmt.Errorf("stage account.alice: %w", stageErr)
	}

	if stageErr := tx.Put("account.bob", []byte("50")); stageErr != nil {
		return fmt.Errorf("stage account.bob: %w", stageErr)
	}

	// Commit publishes the staged operations as one atomic NATS batch. The
	// result describes the batch and its final stream sequence.
	result, commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return fmt.Errorf("commit transfer setup: %w", commitErr)
	}
	log.Printf("committed %d values atomically at stream sequence %d", result.Size, result.Sequence)

	// account.alice already exists, so Create's precondition will fail. The
	// account.carol Put is in the same transaction and must also remain hidden.
	conflicting := bucket.Tx()
	defer conflicting.Abort()

	if stageErr := conflicting.Create("account.alice", []byte("999")); stageErr != nil {
		return fmt.Errorf("stage conflicting account.alice: %w", stageErr)
	}

	if stageErr := conflicting.Put("account.carol", []byte("25")); stageErr != nil {
		return fmt.Errorf("stage conflicting account.carol: %w", stageErr)
	}

	_, conflictErr := conflicting.Commit(ctx)
	log.Printf("conflicting transaction rejected: %t", errors.Is(conflictErr, nkv.ErrTxConflict))

	_, missingErr := bucket.Get(ctx, "account.carol")
	log.Printf("other staged value was not written: %t", errors.Is(missingErr, nkv.ErrKeyNotFound))

	return nil
}
func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}

	return nats.DefaultURL
}
