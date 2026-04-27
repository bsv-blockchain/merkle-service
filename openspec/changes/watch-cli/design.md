## Context

The merkle-service exposes `POST /watch` for registering txid/callbackURL pairs. Operators currently register transactions by writing one-off `curl` commands. A dedicated CLI binary removes that friction, enables scripted bulk registration, and serves as a reference client for the API.

The existing codebase uses `flag` (stdlib) for simple CLI tools, has no external CLI framework dependency, and builds self-contained binaries via `go build` in the Dockerfile.

## Goals / Non-Goals

**Goals:**
- Single-txid registration: `watch --url http://host:8080 --txid <hash> --callback http://cb.example/`
- Bulk registration from a file: `watch --url http://host:8080 --file txids.txt --callback http://cb.example/`
- Concurrent bulk requests with a configurable worker pool
- Scripting-friendly: exit 0 on full success, exit 1 on any failure
- Human-readable output by default; `--quiet` for machine use
- `--timeout` for the HTTP client
- `--concurrency` for bulk parallelism

**Non-Goals:**
- Deregistration (not a supported API operation)
- Per-txid callback URL (all txids share one callback URL per invocation)
- Authentication/TLS client certs (not yet supported by the API)
- Interactive TUI or progress bars

## Decisions

### stdlib `flag` over a CLI framework

No cobra/urfave dependency needed for a tool with ~6 flags. Keeps the binary small and consistent with the rest of the codebase.

**Alternative considered**: `github.com/spf13/cobra`. Rejected — adds a dependency and auto-generated help text for a tool this simple adds more complexity than value.

### Concurrency model: semaphore-bounded goroutine pool

For bulk mode, spawn one goroutine per txid bounded by a semaphore of size `--concurrency` (default 10). Results are collected via a shared error counter with atomic ops.

**Alternative considered**: Worker pool with a channel of txids. Equivalent complexity; semaphore approach is simpler for this fan-out pattern.

### Input formats

- Single: `--txid <hash>` (64-char hex, validated client-side before sending)
- Bulk: `--file <path>` — one txid per line, blank lines and `#` comments skipped, validated before any requests are sent (fail fast on bad input)
- stdin pipe: if `--file -` is specified, read from stdin (enables `cat txids.txt | watch --file - ...`)

### Output

- Default: one line per txid — `OK <txid>` or `FAIL <txid>: <reason>`
- `--quiet`: suppress per-txid output, print only final summary `registered N/M`
- `--verbose`: additionally log HTTP request/response details

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | All registrations succeeded |
| 1 | One or more registrations failed |
| 2 | Bad flags / input validation error (before any requests) |

## Risks / Trade-offs

- [No retry logic] A single transient HTTP 500 causes exit 1. **Mitigation**: The API is idempotent — re-running the command is safe. A `--retries` flag can be added later.
- [Memory for large files] Loading all txids before starting adds latency for very large files. **Mitigation**: Streaming line-by-line from the file keeps memory O(1); validation pass is a first scan.
