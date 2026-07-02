## MODIFIED Requirements

### Requirement: All services use configured log level
Every service (API, P2P, subtree processor, block processor, subtree worker, subtree fetcher, callback delivery) SHALL use the configured log level, and the `BaseService.InitBase` logger and all `cmd/` entrypoint loggers SHALL respect the configured level. subtree-fetcher and callback-delivery previously constructed their loggers with a hardcoded `slog.LevelInfo` instead of `config.ParseLogLevel(cfg.LogLevel)`, silently ignoring `LOG_LEVEL`/`logLevel` for those two services; this requirement is restated to explicitly enumerate every affected service and close that gap.

#### Scenario: Service logger respects configured level
- **WHEN** `logLevel` is set to `warn`
- **THEN** `BaseService.InitBase` SHALL create a logger with minimum level `warn`
- **AND** debug and info messages SHALL not appear in output

#### Scenario: subtree-fetcher honors LOG_LEVEL
- **WHEN** `LOG_LEVEL=debug` is set and `cmd/subtree-fetcher` starts
- **THEN** its logger SHALL be constructed at `debug` level and debug-level messages SHALL appear in output

#### Scenario: callback-delivery honors LOG_LEVEL
- **WHEN** `LOG_LEVEL=debug` is set and `cmd/callback-delivery` starts
- **THEN** its logger SHALL be constructed at `debug` level and debug-level messages SHALL appear in output
