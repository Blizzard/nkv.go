# Contributing to nkv

Participation in this project is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

## Module layout

This repository keeps the library, integration tests, and runnable examples in
separate Go modules:

| Path                | Module                                       | Purpose                           |
| ------------------- | -------------------------------------------- | --------------------------------- |
| `./`                | `github.com/blizzard/nkv.go`                 | The library itself                |
| `./tests`           | `github.com/blizzard/nkv.go/tests`           | Integration/behavioral test suite |
| `./examples/<name>` | `github.com/blizzard/nkv.go/examples/<name>` | Standalone runnable example       |

The `tests` module uses a `replace` directive to point at the repository root, so it
always builds against your local working copy.

Each example does the same, allowing `go run .` to exercise the current checkout
without adding example-only dependencies to the published library module.

### Why the tests live in a separate module

The test suite starts a real embedded NATS server (`github.com/nats-io/nats-server/v2`)
to exercise JetStream KV behavior end-to-end. That server pulls in a large dependency
tree (`jwt`, `nkeys`, `highwayhash`, `go-tpm`, `golang.org/x/time`, and friends).

If the tests lived in the root module, every one of those would land in the consumer's
`go.sum` and dependency graph — even though none of them are needed at runtime. Keeping
the tests in `./tests` means the published `nkv` module's only direct dependency stays
`github.com/nats-io/nats.go`.

**Do not add test-only dependencies to the root `go.mod`.** If a change requires a new
testing helper or a running server, it belongs in `./tests`.

## Running the tests

From the repository root:

```bash
go test -C tests ./...
```

Equivalently, without the `-C` flag:

```bash
cd tests && go test ./...
```

Because these are separate modules, `go test ./...` at the repository root will **not**
run the suite in `./tests`.

Useful variations:

```bash
go test -C tests -run TestGeneric ./...   # single test or pattern
go test -C tests -v ./...                 # verbose
go test -C tests -race ./...              # race detector
go test -C tests -count=1 ./...           # bypass the test cache
```

## Adding tests

- Test files use the `nkv_test` package so they exercise the public API only.
- Use `testConnection(t)` from [tests/helpers_test.go](tests/helpers_test.go) to get a
  connection to a fresh embedded server; it uses `t.TempDir()` for storage and a random
  port, so tests can run in parallel and leave nothing behind.
- Assertions use [`github.com/matryer/is`](https://github.com/matryer/is). Add a trailing
  comment to each assertion — `is` prints it as the failure message.
- Prefer shared helpers (`assertEntry`, `assertPanicContains`) over hand-rolled checks.

## Linting

This project uses [golangci-lint](https://golangci-lint.run) **v2** (latest) with a strict
configuration. The library, tests, and examples have configs appropriate to their
modules:

- [.golangci.yml](.golangci.yml) — the library
- [tests/.golangci.yml](tests/.golangci.yml) — the test suite
- [examples/.golangci.yml](examples/.golangci.yml) — all standalone examples

They start from `default: all` and subtract only what is dogmatic or actively wrong for
this codebase. **New linters are opt-out, not opt-in** — an upgrade that introduces a
linter will start reporting immediately, which is intended.

### Install

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```

Or see the [install docs](https://golangci-lint.run/docs/welcome/install/local/). The
configs use the `version: "2"` schema, so a v1 binary will reject them.

### Run

Each module must be linted separately. Unlike `go`, golangci-lint has no `-C` flag, so
change directory for the test suite and each example:

```bash
golangci-lint run
(cd tests && golangci-lint run)
for example in examples/*/; do (cd "$example" && golangci-lint run); done
```

Autofix what can be fixed, and apply the formatters:

```bash
golangci-lint run --fix
golangci-lint fmt

(cd tests && golangci-lint run --fix && golangci-lint fmt)
for example in examples/*/; do
  (cd "$example" && golangci-lint run --fix && golangci-lint fmt)
done
```

Validate the config itself after editing it:

```bash
golangci-lint config verify
(cd tests && golangci-lint config verify)
(cd examples/getting-started && golangci-lint config verify)
```

Formatting is `goimports` with `github.com/blizzard/nkv.go` as the local import prefix —
imports group as stdlib, third-party, then local.

### Dependency policy is enforced by the linter

The root config uses `gomodguard` to block `github.com/nats-io/nats-server/v2` (and other
test-only modules) from the library. If you get a lint failure for importing it, the code
belongs in `./tests` — see [Why the tests live in a separate module](#why-the-tests-live-in-a-separate-module).

### Suppressing a finding

`nolintlint` is enabled with `require-explanation` and `require-specific`, so a bare
`//nolint` is itself an error. Suppressions must name the linter and say why:

```go
//nolint:gochecknoglobals // compiled once; used on every key validation
var keyRE = regexp.MustCompile(...)
```

Prefer fixing the code. If a rule is wrong for the project as a whole, disable it in the
config with a comment rather than scattering suppressions.

## Dependency changes

After touching imports, tidy the affected module(s) individually:

```bash
go mod tidy
go mod tidy -C tests
```

Review the diff to the root `go.mod` carefully — any new entry there is a dependency
imposed on every consumer of the library.
