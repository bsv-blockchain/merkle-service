## Why

The log level is hardcoded to `INFO` in `BaseService.InitBase` and all `cmd/` entrypoints use `slog.Default()` (which is also INFO). There is no way to enable debug-level logging at runtime without code changes. Debug logs are critical for troubleshooting production issues and during development.

## What Changes

- Add a `logLevel` field to the top-level config (`config.yaml` / env var `LOG_LEVEL`).
- Parse the log level string (`debug`, `info`, `warn`, `error`) into `slog.Level`.
- Configure `slog.Default()` and `BaseService.InitBase` to use the configured level.
- All `cmd/` entrypoints use the configured logger instead of `slog.Default()`.

## Capabilities

### New Capabilities
- `configurable-log-level`: Adds a config-driven log level setting that controls the global slog level across all services.

### Modified Capabilities
_(none — this is purely additive infrastructure)_

## Impact

- **Config**: New `logLevel` field (default: `info`). Env override: `LOG_LEVEL`.
- **All services**: Log output changes when level is set to `debug` or `warn`/`error`.
- **No breaking changes**: Default behavior unchanged (INFO level).
