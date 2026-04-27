## 1. Service Rename — Internal

- [x] 1.1 In `internal/subtree/processor.go`, change `p.InitBase("subtree-processor")` to `p.InitBase("subtree-fetcher")`
- [x] 1.2 In `internal/subtree/processor.go`, update `p.Logger.Info("starting subtree processor")` and `p.Logger.Info("subtree processor started/stopped/initialized")` log messages to say "subtree-fetcher" instead
- [x] 1.3 In `internal/subtree/processor_test.go`, update any service name assertions or log message checks referencing "subtree-processor" to "subtree-fetcher"

## 2. Service Rename — Entrypoint

- [x] 2.1 In `cmd/subtree-processor/main.go`, update the `base.InitBase("subtree-processor")` call to `base.InitBase("subtree-fetcher")`
- [x] 2.2 Rename the directory `cmd/subtree-processor/` to `cmd/subtree-fetcher/` (move the file, update build scripts/Makefile if needed)
- [x] 2.3 If a `Makefile` or `Dockerfile` builds the `subtree-processor` binary, update the build target/`go build` output name from `subtree-processor` to `subtree-fetcher`

## 3. All-in-One Main Update

- [x] 3.1 In `cmd/merkle-service/main.go`, rename the `subtreeProcessor` variable to `subtreeFetcher`
- [x] 3.2 Verify `cmd/merkle-service/main.go` builds cleanly after rename

## 4. K8s Manifest — subtree-fetcher.yaml

- [x] 4.1 Create `deploy/k8s/subtree-fetcher.yaml` with a `Deployment` named `subtree-fetcher`, `replicas: 4`, using `command: ["subtree-fetcher"]`, referencing the `merkle-service-config` configmap volume
- [x] 4.2 Add header comment to `subtree-fetcher.yaml` explaining: partition count of the `subtree` topic determines max replicas, each replica handles a distinct partition subset, blob store writes are idempotent
- [x] 4.3 Add resource requests/limits to `subtree-fetcher.yaml`: requests `cpu: 250m, memory: 256Mi`; limits `cpu: "1", memory: 512Mi` (fetch-only, low memory)
- [x] 4.4 Add `BLOB_STORE_URL` env var note as a comment in `subtree-fetcher.yaml`: for multi-pod deployments, configure `blobStore.url` with an `s3://` or `gs://` URL in the configmap

## 5. K8s Manifest — configmap.yaml

- [x] 5.1 In `deploy/k8s/configmap.yaml`, add a comment above the `blobStore` section noting that `file://` is for single-node only and `s3://`/`gs://` is required for shared blob store in multi-replica deployments

## 6. K8s README Updates

- [x] 6.1 In `deploy/k8s/README.md`, add `subtree-fetcher.yaml` to the `kubectl apply` deployment list
- [x] 6.2 Add `subtree-fetcher` row to the Services table: replicas=4, scaling limit=subtree topic partitions, notes about P2P fetch and SEEN callbacks
- [x] 6.3 Update the architecture diagram in `deploy/k8s/README.md` to show `Subtree Fetcher (4+ pods)` consuming from the `subtree` Kafka topic and writing to blob store
- [x] 6.4 Add a "Scaling Subtree Fetchers" section to `deploy/k8s/README.md` explaining how to increase `subtree` topic partitions and scale replicas, similar to the existing "Scaling Subtree Workers" section
- [x] 6.5 Update the `deploy/k8s/README.md` prerequisites to note the `subtree` topic partition count recommendation

## 7. Build and Validation

- [x] 7.1 Run `go build ./...` to confirm all packages compile after the rename
- [x] 7.2 Run `go test ./...` to confirm no regressions
