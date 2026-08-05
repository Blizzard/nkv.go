# Typed JSON values

This example uses `nkv.NewGeneric[T]` to store a Go struct as JSON. The typed
wrapper handles encoding, decoding, the `Content-Type` header, and a key prefix.

## What it demonstrates

- `nkv.NewGeneric[User]` turns a byte-oriented bucket into a typed interface.
- The default `nkv.JSONCodec` uses the struct's JSON tags and records
	`Content-Type: application/json`.
- `nkv.WithPrefix("users")` maps the logical key `ada` to the stored key
	`users.ada`.
- The underlying `Bucket` remains available when an application needs raw
	values or entry metadata.

## Run it

Start a NATS server with JetStream enabled (version 2.12.0 or newer):

```bash
nats-server -js -sd /tmp/nkv-typed-json
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
user at revision 1: {Name:Ada Lovelace Email:ada@example.com Roles:[admin developer]}
stored content type: application/json
put and get revisions match: true
```

The revision belongs to the underlying stream entry, so typed and raw reads
observe the same revision. The content type is metadata used for codec
dispatch; the value itself is still stored as bytes in the KV stream.

## Explore further

- Add another `User` and iterate over `users.List(ctx, "*")`.
- Use `users.Update` with the revision returned by `Get`.
- Add `nkv.WithDefaultTTL` when creating the wrapper and observe values expire.