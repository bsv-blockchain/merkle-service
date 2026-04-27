## 1. Project Scaffolding

- [x] 1.1 Initialize Go module (`go.mod`) with module path and add core dependencies (Aerospike Go client, Sarama/confluent-kafka-go, libp2p, chi/gorilla for HTTP, Teranode `stores/blob`, Teranode `stores/txmetacache`)
- [x] 1.2 Create directory structure: `cmd/`, `internal/service/`, `internal/api/`, `internal/p2p/`, `internal/subtree/`, `internal/block/`, `internal/callback/`, `internal/store/`, `internal/kafka/`, `internal/stump/`, `internal/config/`, `pkg/`
- [x] 1.3 Create Docker Compose file with Aerospike, Kafka (+ Zookeeper), and a stub Teranode P2P node for local development
- [x] 1.4 Create Makefile with targets: `build`, `test`, `lint`, `docker-up`, `docker-down`, `run`
- [x] 1.5 Define the daemon `Service` interface in `internal/service/service.go` with `Init(cfg)`, `Start(ctx)`, `Stop()`, `Health()` methods matching Teranode pattern
- [x] 1.6 Implement base service struct with common lifecycle logic (context management, graceful shutdown via signal handling, logger setup)
- [x] 1.7 Create `internal/config/config.go` with configuration struct and loader supporting environment variables and config file (YAML), including mode selection (all-in-one vs. microservice)

## 2. Data Layer — Aerospike Client & Registration Store

- [x] 2.1 Create Aerospike client wrapper in `internal/store/aerospike.go` with connection pool, retry policy (configurable backoff), and health check method
- [x] 2.2 Implement registration store in `internal/store/registration.go`: `Add(txid, callbackURL)` using CDT set append, `Get(txid) → []callbackURL`, `BatchGet([]txid) → map[txid][]callbackURL`
- [x] 2.3 Implement `UpdateTTL(txid, ttl)` and `BatchUpdateTTL([]txid, ttl)` for post-mine 30-minute TTL updates
- [x] 2.4 Implement seen-counter store in `internal/store/seen_counter.go`: `Increment(txid) → newCount` using Aerospike `operate` with atomic `Add`, configurable threshold for SEEN_MULTIPLE_NODES
- [x] 2.5 Integrate Teranode `stores/blob` package as subtree store in `internal/store/subtree_store.go`: configure `blob.Store` via URL-scheme factory (file:// for dev, http:// for production), wrap with `ConcurrentBlob` for deduplication, implement `StoreSubtree(id, data, blockHeight)` using `Set`/`SetFromReader` with `WithDeleteAtHeight`, `GetSubtree(id)` using `Get`/`GetIoReader`, `DeleteSubtree(id)` using `Del`, and `SetCurrentBlockHeight(height)` forwarding
- [x] 2.6 Write unit tests for registration store (add single, add multiple callbacks, idempotent add, batch get, TTL update)
- [x] 2.7 Write unit tests for seen-counter store (increment, threshold detection, atomic behavior)
- [x] 2.8 Write unit tests for subtree store (store/get with DAH, expiry after block height advance, concurrent access via ConcurrentBlob, streaming I/O for large subtrees)

## 3. Kafka Infrastructure

- [x] 3.1 Create Kafka producer wrapper in `internal/kafka/producer.go` with configurable topic, partitioner, and retry settings
- [x] 3.2 Create Kafka consumer wrapper in `internal/kafka/consumer.go` with consumer group support, offset management, and graceful shutdown
- [x] 3.3 Define message schemas in `internal/kafka/messages.go`: `SubtreeMessage` (subtree ID, txids, merkle data), `BlockMessage` (block hash, header, subtree refs), `StumpsMessage` (callback URL, STUMP data, status type, retry count, delay)
- [x] 3.4 Implement partition key strategies: subtree ID for subtree topic, block hash for block topic, callback URL hash for stumps topic
- [x] 3.5 Write integration tests for Kafka producer/consumer round-trip with Docker Compose Kafka

## 4. API Server

- [x] 4.1 Implement HTTP server in `internal/api/server.go` with the `Service` interface (Init/Start/Stop/Health), route registration, and middleware (logging, request ID)
- [x] 4.2 Implement `POST /watch` handler in `internal/api/handlers.go`: parse request body (txid + callback URL), validate inputs (64-char hex txid, valid HTTP/HTTPS URL)
- [x] 4.3 Wire `/watch` handler to registration store `Add` call, return appropriate HTTP responses (200 success, 400 validation errors, 500 internal errors)
- [x] 4.4 Implement `GET /health` handler returning JSON status with Aerospike connectivity check
- [x] 4.5 Write unit tests for request validation (missing txid, invalid txid format, missing callback URL, invalid URL)
- [x] 4.6 Write integration tests for `/watch` endpoint with real Aerospike (register, duplicate register, multiple callbacks per txid)

## 5. P2P Client

- [x] 5.1 Implement libp2p client in `internal/p2p/client.go` with the `Service` interface: DHT-based peer discovery, configurable bootstrap peers, reconnection with exponential backoff
- [x] 5.2 Implement subtree topic subscription: subscribe to Teranode subtree P2P topic, deserialize subtree messages, publish to Kafka subtree topic via producer wrapper
- [x] 5.3 Implement block topic subscription: subscribe to Teranode block P2P topic, deserialize block messages, publish to Kafka block topic via producer wrapper
- [x] 5.4 Add error handling for Kafka publish failures (retry with backoff, error metrics logging)
- [x] 5.5 Write unit tests for P2P client (message deserialization, Kafka publish wiring with mock producer)

## 6. Subtree Processor

- [x] 6.1 Implement subtree processor in `internal/subtree/processor.go` with the `Service` interface: Kafka consumer for subtree topic, consumer group configuration
- [x] 6.2 Implement subtree storage logic: extract subtree data from Kafka message, store in blob.Store with DAH (delete-at-height) and configurable mode (realtime vs. deferred)
- [x] 6.3 Implement registration check: extract all txids from subtree, perform batched Aerospike registration lookup, collect registered txid → callback URL mappings
- [x] 6.4 Implement SEEN_ON_NETWORK emission: for each registered txid, publish SEEN_ON_NETWORK message to Kafka stumps topic with txid, callback URLs, and subtree reference
- [x] 6.5 Implement seen-count tracking: increment Aerospike counter per registered txid, emit SEEN_MULTIPLE_NODES when threshold is reached, suppress duplicate emissions for counts above threshold
- [x] 6.6 Integrate Teranode `stores/txmetacache` package for registration deduplication: configure `ImprovedCache` with appropriate `maxBytes` and bucket type, wrap registration lookups to check `GetMetaCached` before Aerospike, populate cache on miss via `SetCache`/`SetCacheMulti`, use batch operations for high-throughput subtree processing
- [x] 6.7 Write unit tests for subtree processor (registration check, SEEN_ON_NETWORK emission, seen-count threshold logic, txmetacache hit/miss)
- [x] 6.8 Write integration tests for subtree processor with Kafka and Aerospike (end-to-end subtree consumption → callback emission)

## 7. Block Processing

- [x] 7.1 Implement block processor in `internal/block/processor.go` with the `Service` interface: Kafka consumer for block topic, consumer group configuration
- [x] 7.2 Implement bounded worker pool for parallel block-subtree processing using `errgroup` with configurable concurrency limit
- [x] 7.3 Implement block-subtree processor in `internal/block/subtree_processor.go`: retrieve subtree from blob.Store (via ConcurrentBlob wrapper), extract txids, batch check registrations
- [x] 7.4 Implement STUMP construction in `internal/stump/builder.go`: build BRC-0074 BUMP-format merkle paths scoped to subtree, group registered txids by callback URL, merge paths per callback
- [x] 7.5 Implement STUMP publishing: for each callback URL group, publish StumpsMessage to Kafka stumps topic partitioned by callback URL hash
- [x] 7.6 Implement post-block TTL updates: after all subtrees processed, batch update registered txid TTLs to 30 minutes via registration store
- [x] 7.7 Implement subtree store cleanup: after all block-subtree processors complete, call `blob.Store.Del` for all subtree records in the processed block (DAH serves as fallback if explicit delete fails)
- [x] 7.8 Implement coinbase placeholder handling for subtree 0: treat coinbase entry as opaque, construct STUMPs for other registered txids without modifying coinbase
- [x] 7.9 Write unit tests for STUMP builder (single tx path, merged multi-tx path, BRC-0074 format validation)
- [x] 7.10 Write unit tests for block processor (parallel dispatch, TTL updates, subtree cleanup, coinbase handling)
- [x] 7.11 Write integration tests for block processor with Kafka and Aerospike (block consumption → STUMP publication)

## 8. Callback Delivery

- [x] 8.1 Implement callback delivery service in `internal/callback/delivery.go` with the `Service` interface: Kafka consumer for stumps topic, consumer group configuration
- [x] 8.2 Implement HTTP callback client: POST to callback URL with STUMP payload (MINED status) or status notification (SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES), configurable timeout
- [x] 8.3 Implement retry logic: on non-2xx or connection error, increment retry counter, re-enqueue to Kafka stumps topic with linear backoff delay
- [x] 8.4 Implement delay checking: skip messages whose backoff delay has not elapsed (re-enqueue for later)
- [x] 8.5 Implement dead letter handling: after max retries (configurable, default 5), publish to `stumps-dlq` topic, log permanent failure
- [x] 8.6 Write unit tests for callback delivery (successful delivery, retry on failure, delay checking, dead letter after max retries)
- [x] 8.7 Write integration tests for callback delivery with Kafka and mock HTTP server

## 9. All-in-One Mode & Entry Points

- [x] 9.1 Implement composition root in `cmd/merkle-service/main.go`: instantiate all services, wire shared dependencies (Aerospike client, blob.Store, txmetacache, Kafka producer/consumers), start all services, handle graceful shutdown
- [x] 9.2 Create per-service entry points: `cmd/api-server/main.go`, `cmd/p2p-client/main.go`, `cmd/subtree-processor/main.go`, `cmd/block-processor/main.go`, `cmd/callback-delivery/main.go`
- [x] 9.3 Implement CLI flag and environment variable parsing for mode selection, service-specific config, and shared infra config (Aerospike endpoints, Kafka brokers, P2P bootstrap peers)
- [x] 9.4 Write test for all-in-one startup and shutdown (all services initialize, start, and stop without errors)

## 10. End-to-End Testing & Documentation

- [x] 10.1 Write e2e test: register tx via `/watch` → publish subtree to Kafka → verify SEEN_ON_NETWORK callback delivered to mock server
- [x] 10.2 Write e2e test: register tx → publish subtree → publish block → verify MINED callback with STUMP delivered to mock server
- [x] 10.3 Write e2e test: register tx with multiple callbacks → verify all callback URLs receive notifications
- [x] 10.4 Write e2e test: seen-count tracking → register tx → publish multiple subtrees → verify SEEN_MULTIPLE_NODES callback at threshold
- [x] 10.5 Document API endpoint (`POST /watch`, `GET /health`) with request/response examples
- [x] 10.6 Document deployment guide: Docker Compose for local dev, environment variables reference, all-in-one vs. microservice mode configuration
