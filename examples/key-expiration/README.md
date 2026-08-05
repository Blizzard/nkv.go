# Key expiration

This example writes a session with a two-second per-key TTL and watches both
the write and the server-generated expiration marker.

## How expiration works

`nkv.WithTTL` sets NATS per-message TTL metadata on one KV revision. Expiration
is performed by NATS Server, so it continues even if the writer disconnects.
When the value expires, the server emits a limit marker. `nkv` exposes that
marker as a tombstone entry so watches can remove the key from local state.

The bucket must allow per-message TTL and retain subject delete markers long
enough for watchers to receive them. `nkv.CreateBucket` configures both and uses
a one-minute marker retention by default.

## Run it

Start a NATS server with JetStream enabled (version 2.12.0 or newer):

```bash
nats-server -js -sd /tmp/nkv-key-expiration
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
write event: key=session.ada operation=PUT value="active"
expiration event: key=session.ada operation=PURGE tombstone=true
get after expiration reports key missing: true
```

Expiration timing is approximate. After roughly two seconds, the watcher sees
the tombstone and `Get` reports `nkv.ErrKeyNotFound`. The example's 15-second
timeout turns missing expiration support into an error instead of an indefinite
wait.

## Explore further

- Change the TTL and observe how server timing affects delivery.
- Call `List` with `nkv.WithDeletes()` after expiration to inspect the marker.
- Use `nkv.WithDefaultTTL` on a typed wrapper to apply one policy to its writes.