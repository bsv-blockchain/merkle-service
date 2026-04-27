## ADDED Requirements

### Requirement: Kubernetes Deployment manifests for all services
The system SHALL provide Kubernetes Deployment manifests for each service component: block processor, subtree worker, callback delivery, API server, and P2P client. Each Deployment SHALL use the same container image with different CMD entrypoints.

#### Scenario: Subtree worker Deployment scales horizontally
- **WHEN** the subtree worker Deployment replica count is increased
- **THEN** new pods join the Kafka consumer group and receive partitions of the subtree-work topic via rebalancing

#### Scenario: Delivery service Deployment scales horizontally
- **WHEN** the delivery service Deployment replica count is increased
- **THEN** new pods join the callback Kafka consumer group and receive partitions of the stumps topic via rebalancing

#### Scenario: Block processor runs as coordinator
- **WHEN** the block processor Deployment is running
- **THEN** it consumes block announcements and publishes SubtreeWorkMessages without performing subtree processing itself

### Requirement: Shared ConfigMap for service configuration
The system SHALL provide a Kubernetes ConfigMap containing the shared `config.yaml` mounted into all service pods. Environment variable overrides SHALL be supported for per-service settings.

#### Scenario: ConfigMap mounted in all pods
- **WHEN** any service pod starts
- **THEN** it reads configuration from the mounted ConfigMap at the CONFIG_FILE path

#### Scenario: Environment variables override ConfigMap
- **WHEN** a pod has environment variables set (e.g., CALLBACK_STUMP_CACHE_MODE)
- **THEN** the environment variable takes precedence over the ConfigMap value

### Requirement: Subtree worker entrypoint
The system SHALL provide a `cmd/subtree-worker/main.go` entrypoint for the Kafka-based subtree work consumer, distinct from the existing `cmd/subtree-processor` (which handles P2P subtree announcements).

#### Scenario: Subtree worker starts independently
- **WHEN** the subtree-worker binary is executed
- **THEN** it initializes a SubtreeWorkerService consuming from the subtree-work topic with an AerospikeStumpCache

#### Scenario: Subtree worker shares consumer group
- **WHEN** multiple subtree-worker pods are running with the same consumer group
- **THEN** Kafka distributes subtree-work partitions across all pods
