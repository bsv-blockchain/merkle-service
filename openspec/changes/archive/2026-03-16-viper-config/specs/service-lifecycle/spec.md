## MODIFIED Requirements

### Requirement: Configuration management
The system SHALL load configuration using Viper, supporting YAML config files and environment variable overrides. The `Load()` function signature (`func Load() (*Config, error)`) and the `Config` struct SHALL remain unchanged.

> Traces to: requirements.md — "Follow the same daemon/service pattern that Teranode uses"

#### Scenario: Load with defaults only
- **WHEN** no config file exists and no environment variables are set
- **THEN** the system returns a `Config` populated with built-in defaults
- **THEN** all default values match the previous implementation exactly

#### Scenario: Load from YAML file
- **WHEN** a YAML config file exists at the default path or `CONFIG_FILE` env var path
- **THEN** the system reads and applies YAML values over defaults
- **THEN** unspecified YAML keys retain their default values

#### Scenario: Environment variable override
- **WHEN** an environment variable is set (e.g., `AEROSPIKE_HOST=custom-host`)
- **THEN** the corresponding config field is overridden regardless of YAML file content
- **THEN** the priority order is: env vars > YAML file > defaults

#### Scenario: Nested key env var mapping
- **WHEN** a nested config key like `aerospike.host` has a corresponding env var `AEROSPIKE_HOST`
- **THEN** Viper maps the env var to the correct nested config field using underscore-to-dot key replacement

#### Scenario: Slice config from env var
- **WHEN** a comma-separated env var is set (e.g., `KAFKA_BROKERS=host1:9092,host2:9092`)
- **THEN** the corresponding config field is populated as a string slice

#### Scenario: Boolean config from env var
- **WHEN** a boolean env var is set (e.g., `P2P_ENABLE_NAT=true`)
- **THEN** the corresponding config field is set to the boolean value

#### Scenario: Invalid YAML file
- **WHEN** the YAML config file contains invalid syntax
- **THEN** the `Load()` function returns an error
