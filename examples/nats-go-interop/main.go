package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

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

	bucketName := fmt.Sprintf("EXAMPLE_INTEROP_%d", time.Now().UnixNano())
	modern, bucketErr := nkv.CreateBucket(ctx, nc, nkv.Config{Bucket: bucketName})
	if bucketErr != nil {
		return fmt.Errorf("create nkv bucket: %w", bucketErr)
	}

	// Build the standard nats.go JetStream client over the same NATS connection,
	// then open the bucket that nkv created. Both handles address one KV stream.
	js, jetStreamErr := jetstream.New(nc)
	if jetStreamErr != nil {
		return fmt.Errorf("create JetStream context: %w", jetStreamErr)
	}
	standard, openErr := js.KeyValue(ctx, bucketName)
	if openErr != nil {
		return fmt.Errorf("open bucket with nats.go: %w", openErr)
	}

	// Write through nkv and read the same wire value through nats.go.
	nkvRevision, modernPutErr := modern.Put(ctx, "written.by.nkv", []byte("hello from nkv"))
	if modernPutErr != nil {
		return fmt.Errorf("write with nkv: %w", modernPutErr)
	}
	standardEntry, standardGetErr := standard.Get(ctx, "written.by.nkv")
	if standardGetErr != nil {
		return fmt.Errorf("read nkv value with nats.go: %w", standardGetErr)
	}
	log.Printf("nats.go read %q at revision %d", standardEntry.Value(), nkvRevision)

	// Reverse the direction. Revisions are stream sequence numbers shared by
	// both clients, so this second key receives the next revision.
	standardRevision, standardPutErr := standard.Put(ctx, "written.by.nats", []byte("hello from nats.go"))
	if standardPutErr != nil {
		return fmt.Errorf("write with nats.go: %w", standardPutErr)
	}
	modernEntry, modernGetErr := modern.Get(ctx, "written.by.nats")
	if modernGetErr != nil {
		return fmt.Errorf("read nats.go value with nkv: %w", modernGetErr)
	}
	log.Printf("nkv read %q at revision %d", modernEntry.Value, standardRevision)

	return nil
}

func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}

	return nats.DefaultURL
}
