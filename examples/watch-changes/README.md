# Watch changes

This example starts a pull-based watch over `config.>`. It consumes the current
values until `Watcher.Next` returns the initial-replay sentinel, then receives a
live update.

## Watch lifecycle

An `nkv` watch has two phases:

1. **Initial replay:** by default, the latest value for every matching key is
	delivered first. This lets an application build its current in-memory view.
2. **Live updates:** `Watcher.Next` returns `(nil, nil)` once to mark the replay
	boundary. Subsequent calls block for new matching revisions.

The watcher is backed by a pull-based ordered consumer. Calls to `Next` control
the rate of delivery, providing natural backpressure when processing is slower
than writes. Applications must call `Watcher.Stop` when finished.

## Run it

Start a NATS server with JetStream enabled (version 2.14.0 or newer):

```bash
nats-server -js -sd /tmp/nkv-watch-changes
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
initial values:
	config.color="blue"
	config.size="medium"
live update: config.color="green" at revision 3
```

The two initial lines may appear in either order because the example seeds them
from a Go map. The `nil` entry separating replayed values from live updates is
handled inside the loop and is not printed or treated as an error.

The color update occurs only after the watcher has crossed the replay boundary,
so it cannot be mistaken for an initial value.

## Explore further

- Add `nkv.WithUpdatesOnly()` to skip the initial replay.
- Use `nkv.WithAdditionalKeys("feature.>")` to watch several key spaces with
	one consumer.
- Replace calls to `Next` with the `Updates` channel adapter.