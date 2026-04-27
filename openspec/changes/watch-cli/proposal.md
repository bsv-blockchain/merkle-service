## Why

There is no command-line tool for registering transactions with the merkle-service API, forcing operators and integrators to use raw `curl` or write one-off scripts. A dedicated CLI makes it easy to register txids in bulk, from scripts, or interactively during development and testing.

## What Changes

- **Add** `cmd/watch/main.go` — a standalone Go CLI binary named `watch` that registers one or more txids against the merkle-service API
- **Flags**: `--url` (API base URL), `--callback` (callback URL), `--txid` (single txid), `--file` (path to newline-delimited txid file), `--timeout` (HTTP timeout), `--concurrency` (parallel requests for bulk), `--verbose` / `--quiet` output modes
- **Exit codes**: 0 on full success, 1 on any registration failure (scripting-friendly)
- **Update** `Dockerfile` to build the `watch` binary
- **Add** `deploy/k8s/README.md` usage example for the CLI

## Capabilities

### New Capabilities
- `watch-cli`: CLI tool for registering txids via `POST /watch`; supports single txid, bulk file input, concurrent requests, and structured error output

### Modified Capabilities
- none

## Impact

- `cmd/watch/` — new entrypoint
- `Dockerfile` — new build line
- No changes to existing services, Kafka topics, Aerospike schemas, or API contracts
