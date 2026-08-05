# List entries and keys

This example enumerates keys matching `app.*`. It contrasts `Keys`, which
returns live keys, with `List(WithDeletes())`, which also exposes tombstones.
Enumeration uses paged Direct Get and does not create consumers.

## Keys, entries, and tombstones

NATS KV keys are subject-like strings. The pattern `app.*` matches one token
after `app`, such as `app.production`, but not `database.host` or a deeper key
such as `app.production.region`.

`Keys` returns only the names of live keys. `List` returns complete `nkv.Entry`
values, including values, revisions, operations, and timestamps. Deleting a
key writes a tombstone as its latest entry; `nkv.WithDeletes()` asks `List` to
include those `DEL` or `PURGE` markers.

## Run it

Start a NATS server with JetStream enabled (version 2.14.0 or newer):

```bash
nats-server -js -sd /tmp/nkv-list-and-keys
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
live app keys:
	app.production
app entries, including tombstones:
	app.production operation=PUT    value="v3.2.0"
	app.staging    operation=DEL    value=""
```

The deleted staging key is absent from `Keys` but appears as a `DEL` entry in
the second listing. The exact entry order can vary because the example seeds
data from a Go map.

Both methods use paged Direct Get requests and return Go iterators. Always
check the error yielded with each item, as the example does.

## Explore further

- Change the pattern to `>` to enumerate every key in the bucket.
- Remove `nkv.WithDeletes()` and compare the two listings.
- Print each entry's revision and creation timestamp.