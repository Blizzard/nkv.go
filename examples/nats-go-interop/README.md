# nats.go interoperability

This example opens one bucket through both `nkv` and the standard
`nats.go/jetstream` KV API. Each client reads a value written by the other.

## What is interoperable

Both clients use the same NATS KV wire layout:

- stream name `KV_<bucket>`;
- subjects shaped as `$KV.<bucket>.<key>`;
- standard KV operation, rollup, and revision headers;
- the same raw value bytes and stream sequence revisions.

The example creates the bucket with `nkv.CreateBucket`, which enables Direct
Get, per-message TTL, expiration markers, and atomic publish. The standard
client can open and use that stream, but a default bucket created by
`nats.go` may need its backing stream upgraded before `nkv.Open` accepts it.

| Writer | Key | Reader |
| --- | --- | --- |
| `nkv` | `written.by.nkv` | `nats.go/jetstream` |
| `nats.go/jetstream` | `written.by.nats` | `nkv` |

## Run it

Start a NATS server with JetStream enabled (version 2.12.0 or newer):

```bash
nats-server -js -sd /tmp/nkv-nats-go-interop
```

In another terminal, run the example from this directory:

```bash
go run .
```

For a server at another address:

```bash
NATS_URL=nats://example-host:4222 go run .
```

## Expected output

```text
nats.go read "hello from nkv" at revision 1
nkv read "hello from nats.go" at revision 2
```

The revisions advance in one shared stream regardless of which client writes.
Wire compatibility does not mean the two Go APIs have identical method
signatures or configuration requirements.

## Explore further

- Update a value through one client and read the new revision through the other.
- Delete with `nats.go` and confirm that `nkv.Get` returns `nkv.ErrKeyNotFound`.
- Inspect the `KV_<bucket>` stream with the NATS CLI.