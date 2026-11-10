package nkv_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestReplicatedCASConflicts(t *testing.T) {
	is := is.New(t)
	connections := testClusterConnections(t)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	bucket, err := nkv.CreateBucket(ctx, connections[0], nkv.Config{Bucket: "REPLICATED_CAS", Replicas: 3})
	is.NoErr(err) // replicated bucket creation should succeed

	buckets := make([]*nkv.Bucket, len(connections))
	for i, nc := range connections {
		buckets[i], err = nkv.Open(ctx, nc, bucket.Name())
		is.NoErr(err) // each cluster node should open the replicated bucket
	}

	t.Run("create", func(t *testing.T) {
		assertConcurrentCASConflicts(t, buckets, nkv.ErrKeyExists, func(kv *nkv.Bucket) error {
			_, err := kv.Create(ctx, "create", []byte("after"))

			return err
		})
	})

	t.Run("update", func(t *testing.T) {
		is := is.New(t)

		revision, err := bucket.Put(ctx, "update", []byte("before"))
		is.NoErr(err) // competing updates should start from a known revision

		assertConcurrentCASConflicts(t, buckets, nkv.ErrRevisionMismatch, func(kv *nkv.Bucket) error {
			_, err := kv.Update(ctx, "update", []byte("after"), revision)

			return err
		})
	})

	t.Run("delete", func(t *testing.T) {
		is := is.New(t)

		revision, err := bucket.Put(ctx, "delete", []byte("before"))
		is.NoErr(err) // competing deletes should start from a known revision

		assertConcurrentCASConflicts(t, buckets, nkv.ErrRevisionMismatch, func(kv *nkv.Bucket) error {
			return kv.Delete(ctx, "delete", nkv.WithRevision(revision), nkv.WithTTL(0))
		})
	})

	t.Run("create after tombstone", func(t *testing.T) {
		is := is.New(t)

		_, err := bucket.Put(ctx, "recreate", []byte("before"))
		is.NoErr(err) // recreate fixture should be written

		is.NoErr(bucket.Delete(ctx, "recreate", nkv.WithTTL(0))) // tombstone should remain available for the retry

		_, err = bucket.Create(ctx, "recreate", []byte("after"))
		is.NoErr(err) // replicated create should retry against the tombstone revision
	})
}

func TestReplicatedTxCASConflicts(t *testing.T) {
	is := is.New(t)
	connections := testClusterConnections(t)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	bucket, err := nkv.CreateBucket(ctx, connections[0], nkv.Config{Bucket: "REPLICATED_TX_CAS", Replicas: 3})
	is.NoErr(err) // replicated bucket creation should succeed

	revision, err := bucket.Put(ctx, "guard", []byte("before"))
	is.NoErr(err) // shared CAS guard should be written

	buckets := make([]*nkv.Bucket, len(connections))
	for i, nc := range connections {
		buckets[i], err = nkv.Open(ctx, nc, bucket.Name())
		is.NoErr(err) // each cluster node should open the replicated bucket
	}

	type result struct {
		key string
		err error
	}

	const writers = 48
	start := make(chan struct{})
	results := make(chan result, writers)

	for i := range writers {
		go func() {
			<-start

			key := fmt.Sprintf("member%d", i)
			tx := buckets[i%len(buckets)].Tx()
			defer tx.Abort()

			if err := tx.Put(key, []byte("value")); err != nil {
				results <- result{key: key, err: err}

				return
			}

			if err := tx.Update("guard", []byte("after"), revision); err != nil {
				results <- result{key: key, err: err}

				return
			}

			_, err := tx.Commit(ctx)
			results <- result{key: key, err: err}
		}()
	}

	close(start)

	succeeded := 0
	for range writers {
		result := <-results
		if result.err == nil {
			succeeded++

			continue
		}

		is.True(errors.Is(result.err, nkv.ErrTxConflict)) // final publish should classify both replicated CAS error codes

		_, err := bucket.Get(ctx, result.key)
		is.True(errors.Is(err, nkv.ErrKeyNotFound)) // failed transactions must not publish their other member
	}

	is.Equal(succeeded, 1) // only one atomic batch should satisfy the shared guard
}

func assertConcurrentCASConflicts(t *testing.T, buckets []*nkv.Bucket, wantErr error, write func(*nkv.Bucket) error) {
	t.Helper()
	is := is.New(t)

	const writers = 48
	start := make(chan struct{})
	results := make(chan error, writers)

	// Release all writers together to exercise replicated in-flight conflicts.
	for i := range writers {
		go func() {
			<-start

			results <- write(buckets[i%len(buckets)])
		}()
	}

	close(start)

	succeeded := 0
	for range writers {
		err := <-results
		if err == nil {
			succeeded++

			continue
		}

		is.True(errors.Is(err, wantErr)) // every conflict should map to the operation's public error
	}

	is.Equal(succeeded, 1) // only one operation should satisfy the shared expectation
}

func testClusterConnections(t *testing.T) []*nats.Conn {
	t.Helper()
	is := is.New(t)

	const size = 3

	// JetStream requires a configured route even on the first cluster node.
	// Reserve its address before starting it so its seed route can name itself.
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	is.NoErr(err) // seed route address allocation should succeed

	address, ok := listener.Addr().(*net.TCPAddr)
	is.True(ok) // TCP listener should expose a TCP address

	seed := &url.URL{Scheme: "nats", Host: address.String()}
	is.NoErr(listener.Close()) // release the address for the first server

	servers := make([]*server.Server, 0, size)
	connections := make([]*nats.Conn, 0, size)

	for i := range size {
		opts := &server.Options{
			ServerName: fmt.Sprintf("cas%d", i),
			Host:       "127.0.0.1",
			Port:       -1,
			JetStream:  true,
			StoreDir:   t.TempDir(),
			NoLog:      true,
			NoSigs:     true,
			Routes:     []*url.URL{seed},
			Cluster: server.ClusterOpts{
				Name:     "CAS",
				Host:     "127.0.0.1",
				Port:     -1,
				PoolSize: -1,
			},
		}
		if i == 0 {
			opts.Cluster.Port = address.Port
		}

		ns, err := server.NewServer(opts)
		is.NoErr(err) // cluster node creation should succeed

		ns.Start()
		t.Cleanup(func() {
			ns.Shutdown()
			ns.WaitForShutdown()
		})
		is.True(ns.ReadyForConnections(5 * time.Second)) // cluster node should become ready
		servers = append(servers, ns)

		nc, err := nats.Connect("", nats.InProcessServer(ns))
		is.NoErr(err) // in-process client should connect to the cluster node

		t.Cleanup(nc.Close)
		connections = append(connections, nc)
	}

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		for _, ns := range servers {
			if ns.JetStreamIsLeader() && len(ns.JetStreamClusterPeers()) == size {
				return connections
			}
		}

		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("timed out waiting for JetStream cluster membership and leader")
		}
	}
}
