# nkv examples

Each folder is a standalone Go module focused on one `nkv` use case. Every
example uses a local `replace` directive so it runs against the checkout in
this repository.

These programs are intentionally small, runnable explorations rather than an
exhaustive API reference or test suite. Start with `getting-started`, then pick
the example closest to the behavior you are building.

| Example | Focus |
| --- | --- |
| [getting-started](getting-started/) | Create a bucket and put, get, and delete a byte value |
| [typed-json](typed-json/) | Encode and decode Go structs with `Generic[T]` |
| [optimistic-concurrency](optimistic-concurrency/) | Create-once and revision-based compare-and-swap |
| [list-and-keys](list-and-keys/) | Enumerate live keys and tombstones with subject patterns |
| [watch-changes](watch-changes/) | Move from an initial snapshot to live updates |
| [key-expiration](key-expiration/) | Apply a per-key TTL and observe its expiration marker |
| [atomic-transaction](atomic-transaction/) | Commit multiple changes atomically and handle conflicts |
| [nats-go-interop](nats-go-interop/) | Read and write one bucket through both KV clients |

## Before you start

The examples require:

- Go 1.26 or newer;
- NATS Server 2.12.0 or newer;
- JetStream enabled on the server.

Each example README includes a server command with an isolated storage path.
The general shape is:

```bash
nats-server -js -sd /tmp/nkv-example
```

Run `go run .` from inside an individual example folder. Its `go.mod` replaces
`github.com/blizzard/nkv.go` with `../..`, so the program uses the current
checkout instead of a published release.

Programs connect to `nats://127.0.0.1:4222` by default. Set `NATS_URL` to use a
different endpoint:

```bash
NATS_URL=nats://example-host:4222 go run .
```

Every run creates a uniquely named bucket to avoid collisions with earlier
runs. The buckets remain in JetStream storage until that storage is removed.
Examples that seed values from Go maps may print them in a different order.

## Lint an example

The examples share the strict golangci-lint v2 configuration in
`.golangci.yml`. Run the linter from an individual example directory so it
uses that example's Go module:

```bash
cd getting-started
golangci-lint run
```

CI runs the same check independently for every example module.