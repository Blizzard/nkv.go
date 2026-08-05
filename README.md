# nkv: A wire-compatible NATS JetStream KV client

[![CI](https://github.com/blizzard/nkv.go/actions/workflows/ci.yml/badge.svg)](https://github.com/blizzard/nkv.go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/blizzard/nkv.go.svg)](https://pkg.go.dev/github.com/blizzard/nkv.go)
[![Go Version](https://img.shields.io/github/go-mod/go-version/blizzard/nkv.go?logo=go)](go.mod)
[![License](https://img.shields.io/github/license/blizzard/nkv.go)](LICENSE)

`nkv` is a redesigned Go client for [NATS][nats] [JetStream][jetstream] Key-Value buckets. It keeps the
standard KV wire format so buckets written by `nkv` and [`nats.go`][nats-go] are fully
interoperable, while replacing several costly client-side paths: point reads and
enumeration use Direct Get instead of creating ephemeral consumers, watches use
client-driven pull consumers for native backpressure, atomic transactions for grouped
writes, and an API with per-operation options and typed values.

It is a deliberate alternative to the [`nats.go`][nats-go] `jetstream.KeyValue` client, not a
drop-in replacement. The stream layout (`KV_<bucket>`), subjects
(`$KV.<bucket>.<key>`), `KV-Operation` headers, and rollup purges remain compatible;
the public method signatures do not.

**Installation**

```bash
go get github.com/blizzard/nkv.go@latest
```

**Examples**

See the [examples guide](examples/README.md) for focused, runnable programs covering
core operations, typed JSON values, optimistic concurrency, enumeration, watches,
per-key TTL, atomic transactions, and `nats.go` interoperability. Each example is a
standalone Go module with instructions for starting NATS Server and running the code.

## Status

**Pre-release. The API is not finalized and will change without notice.**

`nkv` is at `v0.x`. Until a `v1.0.0` release is tagged:

- Any release may introduce breaking changes to exported types, functions, and
  behavior — including within a single minor version.
- No deprecation period is guaranteed. Symbols may be renamed or removed outright.
- Pin an exact version and read the release notes before upgrading.

Wire compatibility with the standard NATS KV layout is the one thing treated as
stable: buckets written by `nkv` remain readable by [`nats.go`][nats-go], and vice versa. Data
written today will not be orphaned by a future API change.

Wire compatibility describes the stored subjects, headers, and values; it does not
relax bucket configuration validation. `Open` requires every setting listed below and
therefore rejects a bucket created with the default [`nats.go`][nats-go] KV configuration,
which does not enable per-message TTL or atomic publish. Update the backing stream with
the required settings before opening it with `nkv`.

Once `v1.0.0` is released, the module follows [Semantic Versioning](https://semver.org)
and the exported API becomes subject to the usual Go compatibility guarantees.

## Requirements

| | Minimum | Why |
| --- | --- | --- |
| Go | 1.26 | Declared in `go.mod`. |
| [`nats-server`][nats-server] | 2.12.0 | Atomic batch publish. |
| [`nats.go`][nats-go] | v1.52.0 | Client support for the above. |

### Required server features

`CreateBucket` enforces and `Open` requires a stream configuration that enables every
feature below, so 2.12.0 is a hard floor even if your code never calls the newer APIs.

| Feature | Stream setting / header | Used by | Since |
| --- | --- | --- | --- |
| Direct Get | `AllowDirect` | `Get`, `List`, `Keys` | 2.9.0 |
| Multi-subject consumer filters | `FilterSubjects` | `Watch` with `WithAdditionalKeys` | 2.10.0 |
| Batched Direct Get | `batch`, `next_by_subj`, `up_to_seq` | `List`, `Keys` paging | 2.11.0 |
| Per-message TTL | `AllowMsgTTL`, `Nats-TTL` | `WithTTL` | 2.11.0 |
| Limit markers | `SubjectDeleteMarkerTTL`, `Nats-Marker-Reason` | TTL/max-age tombstone detection | 2.11.0 |
| Atomic batch publish | `AllowAtomicPublish`, `Nats-Batch-*` | `Tx`, `GenericTx` | 2.12.0 |

`List` and `Keys` page through Direct Get rather than creating ephemeral consumers, and
`Watch` uses a pull ordered consumer — neither leaves server-side state behind.

`CreateBucket` defaults `SubjectDeleteMarkerTTL` to one minute so TTL and max-age
expirations remain observable long enough for watchers to receive the server-generated
marker. Set a longer value through `Config.StreamConfig` when watchers may be disconnected
for longer periods. Values below one second are rejected by both `nkv` and the server.

### External stream creation requirements

> [!WARNING]
> `nkv` expects the KV-specific settings shown in the command below. Omitting or
> changing those settings causes `Open` to reject the stream. Deployment-specific
> settings may be changed as needed, including `--replicas`, `--storage`, resource
> limits such as `--max-bytes`, and placement, mirror, or source configuration.

Instead of calling `CreateBucket`, you can create the underlying KV stream with the
[NATS CLI][nats-cli]. This command was verified against `nats` CLI `v0.4.0`
(`nats stream add --help`):

```bash
BUCKET_NAME=MY_BUCKET

stream_args=(
  "KV_${BUCKET_NAME}"
  --defaults
  --subjects "\$KV.${BUCKET_NAME}.>"  # required
  --retention limits                  # required
  --discard new                       # required
  --max-msgs-per-subject 1            # required
  --allow-rollup                      # required
  --deny-delete                       # required
  --allow-direct                      # required
  --allow-msg-ttl                     # required
  --subject-del-markers-ttl 1m        # required; can increase
  --allow-batch                       # required
  --storage file                      # can change
  --replicas 1                        # can change
)
nats stream add "${stream_args[@]}"
```

`BUCKET_NAME` supplies both the bucket name and the required naming relationship: a
bucket named `MY_BUCKET` uses stream `KV_MY_BUCKET` and subject `$KV.MY_BUCKET.>`.
`--replicas 1` is suitable for a single-node JetStream cluster but provides no replica
redundancy.

#### Upgrading an existing KV bucket

`CreateBucket` automatically updates an existing bucket to the required configuration.
To perform the same upgrade with the NATS CLI while retaining the bucket's messages,
edit its backing stream:

```bash
BUCKET_NAME=MY_BUCKET

stream_args=(
  "KV_${BUCKET_NAME}"
  --force
  --subjects "\$KV.${BUCKET_NAME}.>"   # required
  --retention limits                   # required
  --discard new                        # required
  --max-msgs-per-subject 1             # required
  --allow-rollup                       # required
  --deny-delete                        # required
  --allow-direct                       # required
  --allow-msg-ttl                      # required
  --allow-batch                        # required
  --subject-del-markers-ttl 1m         # required; can increase
)
nats stream edit "${stream_args[@]}"
```

The command enforces the nkv-specific settings without changing deployment settings
such as storage, replicas, resource limits, placement, mirrors, or sources. The delete
marker TTL controls how long TTL and max-age expiration markers remain available to
watchers; increase it when watchers may be disconnected for longer than one minute.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the module layout, how to run the test suite,
and the dependency rules.

## Security

To report a vulnerability, see [SECURITY.md](SECURITY.md). Please do not open a public
issue.

## License

See [LICENSE](LICENSE).

[jetstream]: https://docs.nats.io/nats-concepts/jetstream
[nats]: https://nats.io/
[nats-cli]: https://github.com/nats-io/natscli
[nats-go]: https://github.com/nats-io/nats.go
[nats-server]: https://github.com/nats-io/nats-server
