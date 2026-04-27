## 1. StumpCache Interface

- [x] 1.1 Define `StumpCache` interface in `internal/store/stump_cache.go` with `Put(subtreeHash, blockHash string, data []byte) error`, `Get(subtreeHash, blockHash string) ([]byte, bool, error)`, `Close() error`
- [x] 1.2 Implement `MemoryStumpCache` using `sync.Map` with TTL sweep goroutine (move existing `sync.Map` logic behind the interface)
- [x] 1.3 Add unit tests for `MemoryStumpCache`: put/get, TTL expiry, concurrent access, close stops sweep

## 2. Aerospike STUMP Cache

- [x] 2.1 Create `internal/store/stump_cache_aerospike.go` with `AerospikeStumpCache` struct: Aerospike client, set name, TTL, local LRU cache
- [x] 2.2 Implement `Put()`: write STUMP binary to Aerospike set with key `subtreeHash:blockHash` and configured TTL
- [x] 2.3 Implement `Get()`: check local LRU first; on miss, read from Aerospike, populate LRU, return data
- [x] 2.4 Implement `Close()`: no-op (Aerospike client lifecycle managed externally)
- [x] 2.5 Add `hashicorp/golang-lru/v2` dependency for the local LRU cache (default capacity 1024)
- [x] 2.6 Add unit tests with mock Aerospike client: put/get through Aerospike, LRU hit avoids Aerospike read, LRU eviction on capacity

## 3. Configuration

- [x] 3.1 Add `StumpCacheMode string` (default `memory`) to `config.CallbackConfig` — values: `memory`, `aerospike`
- [x] 3.2 Add `StumpCacheSet string` (default `stump_cache`) to `config.AerospikeConfig`
- [x] 3.3 Add `StumpCacheLRUSize int` (default 1024) to `config.CallbackConfig`
- [x] 3.4 Add viper defaults, env var bindings (`CALLBACK_STUMP_CACHE_MODE`, `AEROSPIKE_STUMP_CACHE_SET`, `CALLBACK_STUMP_CACHE_LRU_SIZE`), and `config.yaml` entries

## 4. Cache Factory Wiring

- [x] 4.1 Create `store.NewStumpCache(mode string, asClient, cfg) StumpCache` factory function that returns `MemoryStumpCache` or `AerospikeStumpCache` based on mode
- [x] 4.2 Update `cmd/merkle-service/main.go` (all-in-one) to create `StumpCache` via factory and inject into subtree worker and delivery services
- [x] 4.3 Update `cmd/callback-delivery/main.go` to create `AerospikeStumpCache` and inject into `DeliveryService`
- [x] 4.4 Update `DeliveryService` and `SubtreeWorkerService` to accept `StumpCache` interface instead of concrete type

## 5. Subtree Worker Entrypoint

- [x] 5.1 Create `cmd/subtree-worker/main.go` that initializes `SubtreeWorkerService` with Aerospike STUMP cache, Kafka consumers/producers, registration store, subtree store, URL registry, and subtree counter store
- [x] 5.2 Use `MODE` env var or config to determine if running as standalone service
- [x] 5.3 Add `Dockerfile` stage or build target for `subtree-worker` binary

## 6. Kubernetes Manifests

- [x] 6.1 Create `deploy/k8s/namespace.yaml` with `merkle-service` namespace
- [x] 6.2 Create `deploy/k8s/configmap.yaml` with shared `config.yaml` (stumpCacheMode: aerospike, Kafka/Aerospike service addresses)
- [x] 6.3 Create `deploy/k8s/block-processor.yaml` — Deployment with 1 replica, CMD `./block-processor`, env overrides
- [x] 6.4 Create `deploy/k8s/subtree-worker.yaml` — Deployment with 16 replicas (scalable to 256), CMD `./subtree-worker`, resource requests/limits
- [x] 6.5 Create `deploy/k8s/callback-delivery.yaml` — Deployment with 4 replicas, CMD `./callback-delivery`, env overrides
- [x] 6.6 Create `deploy/k8s/api-server.yaml` — Deployment + Service with 2 replicas, CMD `./api-server`, port 8080
- [x] 6.7 Create `deploy/k8s/p2p-client.yaml` — Deployment with 1 replica, CMD `./p2p-client`
- [x] 6.8 Add comments in each manifest documenting scaling considerations (partition count limits, resource tuning)

## 7. Integration Testing

- [x] 7.1 Update scale test `newTestDeliveryService` and wiring to accept `StumpCache` interface
- [x] 7.2 Add integration test verifying Aerospike STUMP cache: subtree worker writes, delivery service reads (requires Aerospike, skip if unavailable)
- [x] 7.3 Verify existing scale test (`TestScaleMega`) still passes with memory cache mode
- [x] 7.4 Run full test suite (`go test ./...`) and fix any regressions

## 8. Documentation

- [x] 8.1 Add `deploy/k8s/README.md` documenting: prerequisites (Kafka, Aerospike), how to deploy, scaling guidance (partition count = max subtree workers), environment variable reference
- [x] 8.2 Update `config.yaml` comments to document `stumpCacheMode` and related settings
