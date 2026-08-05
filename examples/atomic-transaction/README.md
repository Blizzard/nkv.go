# Atomic transaction

This example stages multiple writes and commits them as one atomic NATS batch.
It then triggers a create conflict and verifies that another value staged in
the same transaction was not written.

## Staging and commit

Calls such as `tx.Put` and `tx.Create` add operations to a transaction. They can
report local staging errors, including invalid or duplicate keys, but they do
not expose partial values to readers. `Commit` sends the operations using NATS
atomic batch publish: either every operation becomes visible or none does.

Create and update preconditions are checked as part of the batch. If a key
already exists or a revision is stale, `Commit` returns `nkv.ErrTxConflict` and
the server discards the complete transaction.

## Run it

Start a NATS server with JetStream enabled (version 2.14.0 or newer):

```bash
nats-server -js -sd /tmp/nkv-atomic-transaction
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
committed 2 values atomically at stream sequence 2
conflicting transaction rejected: true
other staged value was not written: true
```

The second transaction tries to create the existing `account.alice` key and to
put `account.carol`. Checking that Carol is missing demonstrates that the
conflict rejected the whole batch, not only the failing operation.

Atomic batch publish was introduced in NATS Server 2.12.0; `nkv` supports
NATS Server 2.14.0 or newer.

## Explore further

- Replace the conflicting `Create` with a `Put` and observe the batch succeed.
- Stage an `Update` using a stale revision to produce the same conflict class.
- Use `Generic[T].Tx()` to atomically write typed JSON values.