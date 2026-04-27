## Context

The merkle-service subtree pipeline consists of two distinct processing stages that are currently ambiguously named:

1. **Subtree Fetcher** (currently `subtree-processor`): Consumes P2P subtree announcements from the `subtree` Kafka topic, fetches binary subtree data from DataHub, stores it in the blob store, checks registrations, and emits `SEEN_ON_NETWORK` / `SEEN_MULTIPLE_NODES` callbacks. Located at `internal/subtree/processor.go` and `cmd/subtree-processor/`.

2. **Subtree Worker** (already correctly named): Consumes `SubtreeWorkMessage` items from the `subtree-work` Kafka topic, reads subtree data from the blob store (with DataHub fallback), builds STUMPs, and emits `MINED` callbacks via the callback accumulator. Located at `internal/block/subtree_worker.go` and `cmd/subtree-worker/`.

The pipeline is complete in code but incomplete in deployment: the subtree-fetcher has no K8s manifest and does not appear in the README. The K8s deployment only covers `subtree-worker`.

**Current data flow:**
```
P2P Client → subtree topic → [subtree-fetcher] → blob store + SEEN callbacks
P2P Client → block topic  → [block-processor] → subtree-work topic
                                                       ↓
                                          [subtree-worker] → blob store (read) → MINED callbacks
```

## Goals / Non-Goals

**Goals:**
- Rename the `subtree-processor` binary and service name to `subtree-fetcher` throughout
- Add `deploy/k8s/subtree-fetcher.yaml` with scaling guidance
- Document `subtree` topic partition requirements for subtree-fetcher scaling
- Update K8s README to show the full pipeline including subtree-fetcher

**Non-Goals:**
- Changing the internal logic of either service (fetch behavior, SEEN callbacks, STUMP building)
- Splitting SEEN callback emission out of the subtree-fetcher into a separate service
- Migrating the blob store from file-based to S3/GCS (the DataHub fallback already handles multi-pod operation; shared blob store is an optimization for a future change)
- Adding new Kafka topics or changing message formats

## Decisions

### Rename scope: binary + service name only, not package path

Rename `cmd/subtree-processor/` binary output name to `subtree-fetcher`, and rename the internal service display name (`InitBase("subtree-processor")` → `InitBase("subtree-fetcher")`). The package path `internal/subtree/` and struct name `Processor` can stay — they describe the type accurately. The Go struct rename would require updating all call sites with no user-visible benefit.

**Alternative considered**: Rename the entire package to `internal/subtreefetcher`. Rejected — renaming an internal package requires updating all imports and provides no runtime benefit. The binary name and service name are what operators see.

### Blob store: rely on DataHub fallback for multi-pod correctness

When multiple subtree-fetcher pods run, each writes to its own local blob store (`file:///data/subtrees` by default). Subtree workers fall back to DataHub if the subtree isn't in the blob store. This already works correctly — the blob store is a performance cache, not a required shared state.

**For production K8s:** Operators should configure `blobStore.url` with an `s3://` or `gs://` URL to share cached subtree data. This is already supported by `store.NewBlobStoreFromURL`. The manifest and docs will note this requirement.

**Alternative considered**: Require shared blob store before deploying subtree-fetcher. Rejected — the system works correctly without it; DataHub fallback ensures correctness.

### Subtree topic partitions: document as scaling knob

The `subtree` topic currently has no partition guidance. Scaling subtree-fetchers to N replicas requires at least N partitions. Document this alongside the existing `subtree-work` topic partition guidance.

## Risks / Trade-offs

- [Binary rename] Any existing K8s deployments referencing `command: ["subtree-processor"]` will fail after the Docker image is rebuilt. **Mitigation**: Old binary name can be kept as a symlink or alias if needed; documented in migration plan.
- [Blob store not shared] Without a shared blob store (S3/GCS), subtree workers may hit DataHub more often when prefetch misses due to load balancing. **Mitigation**: DataHub fallback is already implemented; document S3 configuration in the new manifest.

## Migration Plan

1. Update Docker build to produce `subtree-fetcher` binary (alongside or replacing `subtree-processor`)
2. Apply new `subtree-fetcher.yaml` manifest
3. Ensure `subtree` Kafka topic has sufficient partitions: `kafka-topics.sh --alter --topic subtree --partitions 16`
4. Scale: `kubectl scale deployment subtree-fetcher --replicas=16`

**Rollback**: Delete `subtree-fetcher` deployment; no data is lost since blob store writes are idempotent and DataHub fallback handles gaps.
