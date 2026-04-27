## ADDED Requirements

### Requirement: Daemon lifecycle interface
Every service in the Merkle Service system SHALL implement the daemon lifecycle interface with Init, Start, Stop, and Health methods, matching the Teranode service pattern.

> Traces to: requirements.md — "Follow the same daemon/service pattern that Teranode uses"

#### Scenario: Service initialization
- **WHEN** a service is initialized via Init
- **THEN** the service loads its configuration, establishes connections to dependencies (Aerospike, Kafka, P2P), and prepares internal state
- **THEN** the service does not begin processing until Start is called

#### Scenario: Service start
- **WHEN** a service is started via Start
- **THEN** the service begins its main processing loop (e.g., consuming Kafka, listening for HTTP requests, subscribing to P2P topics)

#### Scenario: Graceful shutdown
- **WHEN** a service receives a Stop signal (SIGTERM or SIGINT)
- **THEN** the service completes in-flight work (current Kafka batch, active HTTP requests)
- **THEN** the service closes connections to dependencies
- **THEN** the service exits cleanly without data loss

#### Scenario: Health check
- **WHEN** a health check is requested via the Health method
- **THEN** the service reports its current status including dependency connectivity (Aerospike, Kafka, P2P as applicable)

### Requirement: All-in-one deployment mode
The system SHALL support running all services in a single process using a composition root that initializes and starts each service. This matches Teranode's all-in-one execution mode.

> Traces to: requirements.md — "Build with a microservice architecture in mind, but support running all-in-one like Teranode"

#### Scenario: Start all services in one process
- **WHEN** the system is started in all-in-one mode
- **THEN** the composition root initializes all services (API server, P2P client, subtree processor, block processor, callback delivery)
- **THEN** all services start and run concurrently within the same process
- **THEN** a shutdown signal stops all services gracefully in reverse dependency order

### Requirement: Microservice deployment mode
The system SHALL support running each service as a standalone process with its own entry point under `cmd/`. Each service binary SHALL be independently deployable.

> Traces to: requirements.md — "Build with a microservice architecture in mind, but support running all-in-one like Teranode"

#### Scenario: Start individual service
- **WHEN** the operator runs a service-specific binary (e.g., `cmd/api-server`)
- **THEN** only that service initializes and starts
- **THEN** the service connects to shared infrastructure (Aerospike, Kafka) independently

### Requirement: Configuration management
The system SHALL load configuration from environment variables and/or configuration files, supporting both all-in-one and per-service configuration. Configuration SHALL include mode selection (all-in-one vs. microservice).

> Traces to: requirements.md — "Follow the same daemon/service pattern that Teranode uses"

#### Scenario: Mode selection via configuration
- **WHEN** the operator sets the mode to "all-in-one" via environment variable or config
- **THEN** the system starts all services in a single process

#### Scenario: Per-service configuration
- **WHEN** a service is started in microservice mode
- **THEN** the service reads only its own configuration section plus shared infrastructure config (Aerospike, Kafka endpoints)
