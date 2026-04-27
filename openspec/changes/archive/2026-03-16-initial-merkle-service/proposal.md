## Why

Arcade instances need to receive merkle proofs (STUMPs) for transactions they broadcast through Teranode so they can construct full BUMPs (BRC-0074) for SPV verification. Currently no service exists to bridge the gap between Teranode's P2P subtree/block announcements and Arcade's callback-based proof delivery model. This is a foundational infrastructure component required for BSV's teranode-scale block processing.

## What Changes

- Introduce a new Go service (Merkle Service) that sits between Teranode and Arcade
- REST API for transaction registration with callback URLs (multiple callbacks per txid)
- P2P client that subscribes to Teranode's libp2p subtree and block topics
- Kafka-based async pipeline for subtree processing, block processing, and STUMP delivery
- Aerospike-backed registration store with batched operations and CDT support
- SEEN_ON_NETWORK callbacks when registered transactions appear in subtrees, with seen-count tracking and configurable threshold for SEEN_MULTIPLE_NODES status
- STUMP construction (BRC-0074 BUMP format scoped to subtree merkle paths) on block confirmation
- Callback delivery with retry logic for STUMP and status notifications
- All-in-one deployment mode (single binary) and microservice mode (per-service binaries), following Teranode patterns

## Capabilities

### New Capabilities

- `transaction-registration`: REST API `/watch` endpoint for registering txid-to-callback-URL mappings in Aerospike, supporting multiple callback URLs per txid, with validation and idempotent registration
- `p2p-ingestion`: libp2p client connecting to Teranode P2P network, subscribing to subtree and block topics, and publishing received data to Kafka
- `subtree-processing`: Kafka subtree consumer that stores subtrees, checks transaction registrations, emits SEEN_ON_NETWORK callbacks, and tracks seen-count per txid with txmetacache deduplication
- `block-processing`: Parallel subtree processing per block, STUMP construction (BRC-0074), grouping proofs by callback URL, Kafka publish, TTL updates, and subtree store cleanup
- `callback-delivery`: Kafka stumps consumer that delivers STUMP and status payloads to registered callback URLs with retry via Kafka re-enqueue
- `service-lifecycle`: Go daemon pattern (Init/Start/Stop/Health) matching Teranode, supporting all-in-one and microservice deployment modes with graceful shutdown and health endpoints
- `data-management`: Aerospike schema design for txid-to-callback-set storage and seen counters with batched operations; Teranode blob.Store for subtree storage with delete-at-height (DAH); Teranode txmetacache for registration deduplication

### Modified Capabilities

(none -- this is a greenfield service)

## Impact

- **New codebase**: Entire Go module created from scratch with `cmd/`, `internal/`, and `pkg/` structure
- **External dependencies**: Aerospike Go client, Kafka client (Sarama or confluent-kafka-go), libp2p, BRC-0074 BUMP library, Teranode `stores/blob` package, Teranode `stores/txmetacache` package
- **Infrastructure**: Requires Aerospike cluster (registrations + seen counters), blob.Store backend (file/HTTP/S3 for subtree storage), Kafka cluster, and connectivity to Teranode P2P network
- **APIs**: New REST endpoint (`POST /watch`) consumed by Arcade; outbound HTTP callbacks to registered URLs
- **Downstream**: Arcade must integrate with the `/watch` API and implement callback handlers for SEEN_ON_NETWORK and MINED (STUMP) payloads
- **Coinbase boundary**: Merkle Service provides STUMPs only; Arcade is responsible for combining with coinbase BEEF to construct full BUMPs
