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

type User struct {
	// JSON tags define the wire representation used by nkv.JSONCodec.
	Name  string   `json:"name"`
	Email string   `json:"email"`
	Roles []string `json:"roles"`
}

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

	bucketName := fmt.Sprintf("EXAMPLE_TYPED_JSON_%d", time.Now().UnixNano())
	bucket, bucketErr := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: bucketName})
	if bucketErr != nil {
		return fmt.Errorf("create bucket: %w", bucketErr)
	}

	// Generic[User] provides typed Put and Get methods. The prefix keeps this
	// type in its own key namespace: logical key "ada" is stored as "users.ada".
	users := nkv.NewGeneric[User](bucket, nkv.WithPrefix("users"))
	written := User{
		Name:  "Ada Lovelace",
		Email: "ada@example.com",
		Roles: []string{"admin", "developer"},
	}

	revision, putErr := users.Put(ctx, "ada", written)
	if putErr != nil {
		return fmt.Errorf("put user: %w", putErr)
	}

	// Get decodes JSON back into User and returns the revision of the entry it
	// read. That revision can later be supplied to a compare-and-swap Update.
	read, readRevision, getErr := users.Get(ctx, "ada")
	if getErr != nil {
		return fmt.Errorf("get user: %w", getErr)
	}
	log.Printf("user at revision %d: %+v", readRevision, read)

	// Reading through the underlying byte-oriented bucket exposes the metadata
	// that Generic wrote alongside the JSON payload.
	raw, rawErr := bucket.Get(ctx, "users.ada")
	if rawErr != nil {
		return fmt.Errorf("get raw entry: %w", rawErr)
	}
	log.Printf("stored content type: %s", raw.ContentType)
	log.Printf("put and get revisions match: %t", revision == readRevision)

	return nil
}

func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}

	return nats.DefaultURL
}
