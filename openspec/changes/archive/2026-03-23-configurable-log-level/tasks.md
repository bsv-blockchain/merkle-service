## 1. Config

- [x] 1.1 Add `LogLevel` field to `Config` struct in `internal/config/config.go`
- [x] 1.2 Register default (`info`) and env var binding (`LOG_LEVEL`) in config.go
- [x] 1.3 Add `ParseLogLevel(string) slog.Level` helper that maps `debug`/`info`/`warn`/`error` to slog levels (case-insensitive, defaults to info on invalid input)
- [x] 1.4 Add `logLevel` entry to `config.yaml` with comment
- [x] 1.5 Add config test for default log level and env override

## 2. Logger Construction

- [x] 2.1 Add `NewLogger(level slog.Level) *slog.Logger` helper in `internal/service/base.go` that creates a JSON slog.Logger with the given level
- [x] 2.2 Update `BaseService.InitBase` to preserve an existing logger if already set
- [x] 2.3 Update `cmd/merkle-service/main.go` to create logger from config after `config.Load()` and pass to services
- [x] 2.4 Update `cmd/subtree-worker/main.go` to create logger from config
- [x] 2.5 Update `cmd/block-processor/main.go` to create logger from config
- [x] 2.6 Update `cmd/callback-delivery/main.go` to create logger from config
- [x] 2.7 Update other `cmd/` entrypoints (subtree-fetcher, p2p-client, api-server) to create logger from config

## 3. Verification

- [x] 3.1 Verify `go build ./...` succeeds
- [x] 3.2 Run unit tests and fix any failures
- [x] 3.3 Test with `LOG_LEVEL=debug` env var to confirm debug output appears
