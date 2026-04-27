## MODIFIED Requirements

### Requirement: Fetch and store subtree on announcement
The subtree-fetcher service SHALL fetch full subtree data from the DataHub URL in the announcement message and store the raw binary data in the subtree blob store. Multiple subtree-fetcher replicas MAY run concurrently in the same Kafka consumer group; the maximum useful replica count equals the partition count of the `subtree` Kafka topic.

#### Scenario: Subtree announced and stored
- **WHEN** a subtree announcement is received with a DataHubURL
- **THEN** the processor fetches the binary subtree data and stores it in the blob store keyed by subtree hash

#### Scenario: DataHub fetch fails
- **WHEN** a subtree announcement is received but the DataHub fetch fails after retries
- **THEN** the processor logs an error and skips processing the subtree

#### Scenario: Multiple fetcher replicas share work
- **WHEN** N subtree-fetcher replicas are running in the same Kafka consumer group AND the `subtree` topic has at least N partitions
- **THEN** each replica processes a distinct subset of subtree announcements with no duplicates

## ADDED Requirements

### Requirement: Subtree-fetcher horizontal scaling via Kafka partitions
The subtree-fetcher deployment SHALL scale horizontally by adding replicas up to the partition count of the `subtree` Kafka topic. Blob store writes are idempotent; if two replicas process the same subtree hash, the second write is safe. Subtree workers that read the blob store SHALL fall back to DataHub if the blob store entry is absent.

#### Scenario: Scale to N replicas
- **WHEN** the `subtree` topic has P partitions and N ≤ P subtree-fetcher replicas are deployed
- **THEN** all replicas are active and each processes a distinct partition assignment

#### Scenario: Blob store miss on worker
- **WHEN** a subtree-worker reads the blob store for a subtree hash that was not yet fetched by any subtree-fetcher replica
- **THEN** the subtree-worker fetches the subtree directly from DataHub as a fallback
