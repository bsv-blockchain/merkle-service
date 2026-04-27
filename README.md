# Merkle Service

A high-throughput service that delivers BSV Blockchain merkle proofs to subscribers as transactions propagate the network and land in mined blocks. Built to interoperate with [Teranode](https://github.com/bsv-blockchain/teranode) and [Arcade](https://github.com/bsv-blockchain/arcade).

Clients register a `(txid, callbackUrl)` pair. The service watches the BSV network over P2P, and as a transaction is observed in subtrees and then in a block, it POSTs status updates and a **STUMP** (Subtree Unified Merkle Path) back to the callback URL.

## Terminology

- **BUMP** — [Bitcoin Unified Merkle Path](https://github.com/bitcoin-sv/BRCs/blob/master/transactions/0074.md) for a transaction within a full block.
- **STUMP** — Subtree Unified Merkle Path. Same wire format as BUMP, but scoped to a single subtree. The service emits one STUMP per subtree (not per transaction), letting downstream consumers reconstruct the BUMP.

## Architecture

The service is a set of stages connected by Kafka topics, designed to scale horizontally for blocks containing millions of transactions.

```
                BSV Network (libp2p)
                       │
                       ▼
                  P2P Client ──► Kafka: subtree, block
                       │
       ┌───────────────┴────────────────┐
       ▼                                ▼
 Subtree Fetcher                  Block Processor
 (fan-out by sub-                 (fan-out subtrees
  scribing to subtree              of a mined block to
  topic, fetches blob,             subtree-work topic)
  emits SEEN callbacks)                  │
       │                                 ▼
       │                          Subtree Worker
       │                          (builds STUMP,
       │                           accumulates in
       │                           Aerospike,
       │                           flushes batched
       │                           callbacks)
       │                                 │
       └─────────► Kafka: callback ◄─────┘
                       │
                       ▼
              Callback Delivery
              (HTTP POST → subscriber URL,
               retries via DLQ)
```

### Stages

| Stage | Binary | Replicas | Role |
|-------|--------|----------|------|
| API server | `cmd/api-server` | unlimited | Accepts `POST /watch` registrations and serves `GET /health`. |
| P2P client | `cmd/p2p-client` | 1 (singleton) | Connects to the BSV P2P network; publishes subtree/block announcements to Kafka. |
| Subtree fetcher | `cmd/subtree-fetcher` | up to `subtree` topic partitions | Resolves subtree blobs, persists them, emits `SEEN_ON_NETWORK` callbacks. |
| Block processor | `cmd/block-processor` | 1 (active) | Fans out a mined block's subtrees to `subtree-work` for parallel processing. |
| Subtree worker | `cmd/subtree-worker` | up to `subtree-work` partitions | Builds STUMPs, accumulates per-callback batches in Aerospike, emits batched callbacks. |
| Callback delivery | `cmd/callback-delivery` | up to `callback` partitions | Hash-partitions HTTP delivery by callback URL; retries failures and DLQs the dead. |
| All-in-one | `cmd/merkle-service` | 1 | Runs every stage in a single process for development and small deployments. |

There is also `cmd/watch`, a small CLI for bulk-registering txids, and `tools/debug-dashboard`, a local web UI for inspecting in-flight state.

## Quick start

### Prerequisites

- Go (the version pinned in `go.mod`; currently 1.26)
- Docker / podman + compose (for local dependencies)

### Run locally (all-in-one)

```bash
make docker-up   # starts Aerospike, Zookeeper, Kafka
make run         # go run ./cmd/merkle-service
```

The API listens on `:8080`. Stop dependencies with `make docker-down`.

### Register a transaction

```bash
curl -X POST http://localhost:8080/watch \
  -H 'Content-Type: application/json' \
  -d '{
    "txid": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
    "callbackUrl": "https://example.com/callback"
  }'
```

Or use the bundled CLI for bulk registration:

```bash
go run ./cmd/watch \
  --url http://localhost:8080 \
  --callback https://example.com/callback \
  --file txids.txt
```

See [docs/api.md](docs/api.md) for the full API reference.

## Configuration

Configuration is loaded from (highest priority first):

1. Environment variables
2. YAML file (`config.yaml` by default; override with `CONFIG_FILE=/path/to/config.yaml`)
3. Built-in defaults

`config.yaml` in this repo lists every setting with its default value and the corresponding env var. Key knobs:

- `mode` — `all-in-one` or `microservice`.
- `store.backend` — `aerospike` (default, recommended for production) or `sql`.
- `kafka.brokers`, `aerospike.host`, `blobStore.url` — connection targets.
- `block.workerPoolSize`, `callback.deliveryWorkers` — concurrency tuning.

See [docs/deployment.md](docs/deployment.md) for the full env-var reference and [docs/sql-backend.md](docs/sql-backend.md) for running against PostgreSQL or SQLite instead of Aerospike.

## Building

```bash
make build    # builds every binary under cmd/
make test     # unit tests
make lint     # golangci-lint
```

The Dockerfile builds all binaries into a single Alpine runtime image. Choose which one runs by overriding the entrypoint, e.g. `--entrypoint api-server`.

## Deployment

### Docker Compose

`docker-compose.yml` brings up Aerospike, Zookeeper, and Kafka for local development. Aerospike data persists in the `aerospike-data` volume.

### Kubernetes

`deploy/k8s/` contains manifests for running the microservice topology on Kubernetes — one deployment per stage, plus a shared ConfigMap. See [deploy/k8s/README.md](deploy/k8s/README.md) for the deployment order, partition-count guidance, and horizontal-scaling notes (validated to ~92k txids/sec with two delivery instances).

## Storage backends

| Backend | Use for | Notes |
|---------|---------|-------|
| Aerospike | Production | Default. Native record TTL, batched ops, scales to teranode-grade workloads. |
| PostgreSQL | Environments without Aerospike | Schema migrations embedded in the binary; TTL emulated by a sweeper goroutine. |
| SQLite | Tests, lightweight single-node | Same SQL backend; not for multi-writer workloads. |

Switch with `store.backend: sql` (and the `store.sql.*` block) in `config.yaml`. The CI suite runs every store against a real PostgreSQL container — `make test-e2e-postgres`.

## Testing

```bash
make test                # unit tests
make test-e2e-postgres   # PostgreSQL-backed e2e suite (requires Docker)
make scale-test          # synthetic large-block scale tests
make mega-scale-test     # 100 instances × 10k txids × 250 subtrees × 4k tx/subtree
```

`make lint-store-imports` enforces that nothing under `cmd/` imports a backend implementation directly — backends are pulled in only via blank imports for side-effect registration.

## Repository layout

```
cmd/                  service binaries (one per stage + all-in-one + watch CLI)
internal/
  api/                HTTP handlers for /watch and /health
  block/              block processor (fans subtrees out to workers)
  cache/              in-memory caches (txmetacache, dedup)
  callback/           callback delivery, batching, DLQ handling
  config/             YAML + env loading
  datahub/            Teranode HTTP client (subtree/block fallback)
  e2e/                cross-stage end-to-end tests
  kafka/              producer/consumer wrappers
  p2p/                Teranode-compatible libp2p client
  service/            all-in-one orchestrator
  store/              storage interfaces + aerospike & sql backends
  stump/              STUMP construction and parsing
  subtree/            subtree fetcher and worker logic
deploy/k8s/           Kubernetes manifests
docs/                 design / api / deployment / sql-backend docs
test/scale/           large-input scale-test fixtures and runners
tools/debug-dashboard local web UI for inspecting state
```

## Documentation

- [docs/design.md](docs/design.md) — Arcade integration design and transaction lifecycle (RECEIVED → SEEN → MINED → IMMUTABLE), including the per-subtree STUMP and compound-BUMP design rationale.
- [docs/api.md](docs/api.md) — HTTP API reference.
- [docs/deployment.md](docs/deployment.md) — full configuration and env-var reference.
- [docs/sql-backend.md](docs/sql-backend.md) — running against PostgreSQL / SQLite.
- [deploy/k8s/README.md](deploy/k8s/README.md) — Kubernetes topology and scaling.
- [requirements.md](requirements.md) — original requirements and design intent.

## Related projects

- [Teranode](https://github.com/bsv-blockchain/teranode) — high-throughput BSV node this service interoperates with.
- [Arcade](https://github.com/bsv-blockchain/arcade) — transaction broadcast and lifecycle service that consumes merkle-service callbacks.
