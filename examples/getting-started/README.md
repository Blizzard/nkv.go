# Getting started

This example creates an `nkv` bucket and walks through a basic put, get, and
delete using byte values. It is the smallest complete example of connecting to
NATS, creating a compatible bucket, and working with an `nkv.Entry`.

## What it demonstrates

- `nkv.CreateBucket` creates or configures the JetStream stream backing a KV
	bucket.
- `Bucket.Put` accepts a `[]byte` value and returns its stream revision.
- `Bucket.Get` returns the value together with revision and timestamp metadata.
- `Bucket.Delete` writes a tombstone, after which `Get` returns
	`nkv.ErrKeyNotFound`.

## Run it

This repository requires Go 1.26 and NATS Server 2.14.0 or newer. Start a NATS
server with JetStream enabled:

```bash
nats-server -js -sd /tmp/nkv-getting-started
```

In another terminal, run the example from this directory:

```bash
go run .
```

Set `NATS_URL` when the server is not listening at `nats://127.0.0.1:4222`:

```bash
NATS_URL=nats://example-host:4222 go run .
```

## Expected output

Revision numbers are assigned by the backing JetStream stream. A fresh bucket
produces output similar to:

```text
put greeting at revision 1
get greeting: "hello, nkv" (revision 1)
get after delete reports key missing: true
```

The program creates a uniquely named bucket on each run so old state cannot
change the result. Those buckets remain in the server's storage directory;
stop the server and remove `/tmp/nkv-getting-started` when finished.

## Explore further

- Put a second key and compare its revision with the first.
- Print `entry.Created` and `entry.Operation` from the returned `nkv.Entry`.
- Replace `Delete` with `Purge` and inspect the operation through a watch or
	`List` with `nkv.WithDeletes()`.