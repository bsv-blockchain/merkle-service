## ADDED Requirements

### Requirement: Configurable log level via config
The system SHALL support a `logLevel` configuration field that controls the minimum log level for all services. Valid values SHALL be `debug`, `info`, `warn`, and `error` (case-insensitive). The default SHALL be `info`.

#### Scenario: Default log level
- **WHEN** no `logLevel` is configured
- **THEN** the system SHALL log at `info` level and above

#### Scenario: Debug level enabled
- **WHEN** `logLevel` is set to `debug`
- **THEN** the system SHALL log at `debug` level and above, including all debug-level messages from services

#### Scenario: Error level only
- **WHEN** `logLevel` is set to `error`
- **THEN** the system SHALL only log `error` level messages

#### Scenario: Invalid log level
- **WHEN** `logLevel` is set to an unrecognized value
- **THEN** the system SHALL default to `info` level and log a warning about the invalid value

### Requirement: Log level configurable via environment variable
The `logLevel` config field SHALL be overridable via the `LOG_LEVEL` environment variable, following the same priority rules as other config fields (env > yaml > default).

#### Scenario: Environment variable overrides config file
- **WHEN** config file sets `logLevel: info` AND env var `LOG_LEVEL=debug` is set
- **THEN** the system SHALL use `debug` level

### Requirement: All services use configured log level
Every service (API, P2P, subtree processor, block processor, subtree worker, callback delivery) SHALL use the configured log level. The `BaseService.InitBase` logger and all `cmd/` entrypoint loggers SHALL respect the configured level.

#### Scenario: Service logger respects configured level
- **WHEN** `logLevel` is set to `warn`
- **THEN** `BaseService.InitBase` SHALL create a logger with minimum level `warn`
- **AND** debug and info messages SHALL not appear in output
