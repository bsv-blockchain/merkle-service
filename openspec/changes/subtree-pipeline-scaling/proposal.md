## Why

The subtree pipeline has two distinct processing roles — fetching subtree data from P2P announcements and building STUMPs from block subtrees — but they are confusingly named (`subtree-processor` and `subtree-worker`), and the subtree-fetcher role has no Kubernetes deployment manifest, making it undeployable as a horizontal microservice. Both roles need to be independently scalable and properly documented.

## What Changes

- **Rename** `cmd/subtree-processor` binary to `subtree-fetcher` and rename the internal service struct/name from `subtree-processor` to `subtree-fetcher` to make the fetch-and-store role explicit
- **Add** `deploy/k8s/subtree-fetcher.yaml` — Kubernetes Deployment for the subtree-fetcher, scaling via partitions of the `subtree` Kafka topic
- **Add** `subtree` topic partition guidance to K8s docs: partition count determines max subtree-fetcher replicas
- **Update** `deploy/k8s/README.md` — add subtree-fetcher to the services table and architecture diagram, add scaling section for subtree-fetchers
- **Update** `deploy/k8s/configmap.yaml` — add `BLOB_STORE_URL` documentation noting that `s3://` or `gs://` URLs are required for shared blob store in K8s
- **Update** `cmd/merkle-service/main.go` all-in-one — rename `subtreeProcessor` to `subtreeFetcher`
- **Update** `deploy/k8s/README.md` deployment `kubectl apply` list to include `subtree-fetcher.yaml`
- No changes to `subtree-worker` (already scalable, already has manifest)

## Capabilities

### New Capabilities
- none

### Modified Capabilities
- `subtree-processing`: The requirement describes the process name and its fetch/store/emit role — update to reflect the rename to "subtree-fetcher" and document K8s horizontal scaling via subtree topic partitions

## Impact

- `cmd/subtree-processor/` — binary renamed to `subtree-fetcher` (binary name change in Dockerfile/build)
- `internal/subtree/processor.go` — service name changed from `subtree-processor` to `subtree-fetcher`
- `cmd/merkle-service/main.go` — variable rename only
- `deploy/k8s/` — new manifest + README updates
- **No breaking changes** to Kafka message formats, Aerospike schemas, or API contracts
- Consumers of the `subtree` topic via consumer group are unaffected
- **BREAKING (deploy)**: New manifest must be applied; the `subtree-fetcher` was previously not deployed in K8s at all
