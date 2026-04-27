## Context

All services use `slog` for structured logging. `BaseService.InitBase` creates a JSON handler hardcoded to `slog.LevelInfo`. The `cmd/` entrypoints call `slog.Default()` before config is loaded, so the logger used during init/startup is also INFO. There is no config path for log level.

## Goals / Non-Goals

**Goals:**
- Add a single `logLevel` config field that controls log verbosity across all services.
- Support standard levels: `debug`, `info`, `warn`, `error`.
- Default to `info` for backwards compatibility.

**Non-Goals:**
- Per-service log levels (all services share the same level).
- Log format configuration (JSON handler stays as-is).
- Log output destination configuration (stdout stays as-is).

## Decisions

### 1. Config field at top level

**Decision**: Add `logLevel` as a top-level config field (not nested under a section), since it applies globally.

**Rationale**: Log level is a cross-cutting concern, not specific to any service. Top-level placement matches the existing `mode` field pattern.

### 2. Parse level in config.Load(), create logger in entrypoints

**Decision**: Add a `ParseLogLevel(string) slog.Level` helper in the config package. Each `cmd/` entrypoint creates the logger after loading config.

**Rationale**: Keeps config parsing pure (no side effects) and lets entrypoints own logger construction. `BaseService.InitBase` is updated to accept an optional `*slog.Logger` so services use the pre-configured logger.

### 3. Use slog.LevelVar for runtime compatibility

**Decision**: Use `slog.LevelVar` so the level could be changed at runtime in the future, though this change only sets it at startup.

**Rationale**: Minor cost, future-friendly. `slog.HandlerOptions.Level` accepts the `slog.Leveler` interface which `*slog.LevelVar` satisfies.

## Risks / Trade-offs

- **[Invalid level string]** → Default to `info` and log a warning. Don't fail startup over a bad log level.
