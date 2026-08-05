# Optimistic concurrency

This example uses `Create` and revision-based `Update` as compare-and-swap
operations. It also shows how to recognize key-exists and stale-revision
errors with `errors.Is`.

Optimistic concurrency is useful when several processes may update one key.
Each writer reads a value and revision, computes a change, then updates only if
that revision is still current. No distributed lock is required.

## Flow

1. `Create` writes inventory value `"10"` because the key is absent.
2. A second `Create` is rejected with `nkv.ErrKeyExists`.
3. `Update` changes the value to `"9"` using the revision from step 1.
4. Another `Update` reuses the now-stale revision and is rejected with
	`nkv.ErrRevisionMismatch`.
5. A final `Get` confirms that the rejected write did not change the value.

## Run it

Start a NATS server with JetStream enabled (version 2.12.0 or newer):

```bash
nats-server -js -sd /tmp/nkv-optimistic-concurrency
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
created inventory at revision 1
second create reports key exists: true
updated inventory at revision 2
stale update reports revision mismatch: true
current inventory is "9" at revision 2
```

This is a single-process demonstration of the same conflict that competing
writers encounter. Production code can catch `nkv.ErrRevisionMismatch`, read
the current value again, and decide whether to retry its operation.

## Explore further

- Change the final update to use `newRevision` and observe it succeed.
- Use `nkv.WithRevision` to make a delete conditional on the latest revision.
- Run two copies that target a shared bucket and race their updates.