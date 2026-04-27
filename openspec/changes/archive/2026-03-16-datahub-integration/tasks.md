## 1. DataHub Client Package

- [x] 1.1 Create `internal/datahub/client.go` with `Client` struct holding `*http.Client` with configurable timeout
- [x] 1.2 Implement `FetchSubtreeRaw(ctx, dataHubURL, hash)` — GET `{dataHubURL}/subtree/{hash}`, return raw `[]byte` and error
- [x] 1.3 Implement `FetchSubtree(ctx, dataHubURL, hash)` — calls `FetchSubtreeRaw`, deserializes with `subtree.NewSubtreeFromBytes`, returns `*subtree.Subtree`
- [x] 1.4 Implement `FetchBlockMetadata(ctx, dataHubURL, hash)` — GET `{dataHubURL}/block/{hash}/json`, parse JSON response into `BlockMetadata` struct with Height, Header, Subtrees ([]string), TransactionCount
- [x] 1.5 Add retry logic with exponential backoff (max 3 retries, 500ms base) to HTTP requests
- [x] 1.6 Add `internal/datahub/client_test.go` — test FetchSubtree and FetchBlockMetadata with httptest mock servers (success, 404, timeout)

## 2. Add go-subtree Dependency

- [x] 2.1 Run `go get github.com/bsv-blockchain/go-subtree@v1.0.1` and `go mod tidy`
- [x] 2.2 Verify the dependency resolves and `go build ./...` succeeds

## 3. Subtree Processor — DataHub Fetch and Store

- [x] 3.1 Add DataHub client to subtree `Processor` struct and `Init` method
- [x] 3.2 In `handleMessage`, fetch binary subtree data from DataHub using the announcement's DataHubURL and hash
- [x] 3.3 Store the raw binary data in the subtree blob store via `subtreeStore.StoreSubtree(hash, data, 0)`
- [x] 3.4 Parse the binary data into `*subtree.Subtree` using `subtree.NewSubtreeFromBytes`

## 4. Subtree Processor — Registration Checking and Callbacks

- [x] 4.1 Extract TxIDs from `subtree.Nodes` (each `SubtreeNode.Hash` is a txid)
- [x] 4.2 Use `regCache.FilterUncached(txids)` to split into uncached txids and cached-registered txids
- [x] 4.3 Call `registrationStore.BatchGet(uncachedTxids)` for Aerospike lookup
- [x] 4.4 Update the registration cache: `SetMultiRegistered` for found txids, `SetMultiNotRegistered` for not-found
- [x] 4.5 For each registered txid, emit a `StumpsMessage` with `StatusSeenOnNetwork` to the stumps producer for each callback URL
- [x] 4.6 For each registered txid, call `seenCounterStore.Increment(txid)` and if `ThresholdReached`, emit `StumpsMessage` with `StatusSeenMultiNodes`

## 5. Block Processor — DataHub Fetch

- [x] 5.1 Add DataHub client to block `Processor` struct and `Init` method
- [x] 5.2 In `handleMessage`, fetch block metadata from DataHub using the announcement's DataHubURL and hash
- [x] 5.3 Extract the list of subtree hashes from the block metadata

## 6. Block Subtree Processor — Re-implementation

- [x] 6.1 Create `ProcessBlockSubtree(ctx, subtreeHash, blockHeight, blockHash, dataHubURL)` function in `internal/block/subtree_processor.go`
- [x] 6.2 Retrieve subtree data from blob store via `subtreeStore.GetSubtree(hash)`, falling back to DataHub fetch if not found
- [x] 6.3 Parse subtree data with `subtree.NewSubtreeFromBytes`
- [x] 6.4 Extract TxIDs, batch-lookup registrations via `registrationStore.BatchGet`
- [x] 6.5 Build full merkle tree using `subtree.BuildMerkleTreeStoreFromBytes(st.Nodes)`
- [x] 6.6 Map registered txids to their leaf indices in the subtree
- [x] 6.7 Call `stump.Build(blockHeight, merkleTree, registeredIndices)` to construct the STUMP
- [x] 6.8 Encode STUMP with `stump.Encode()` to get BRC-0074 binary
- [x] 6.9 Group txids by callback URL using `stump.GroupByCallback`
- [x] 6.10 For each callback URL group, emit a `StumpsMessage` with `StatusMined`, encoded STUMP data, and blockHash
- [x] 6.11 Batch update registration TTLs via `registrationStore.BatchUpdateTTL(txids, postMineTTLSec)`

## 7. Block Processor — Worker Pool Integration

- [x] 7.1 After fetching block metadata, iterate subtree hashes and dispatch to a bounded worker pool (size from `block.workerPoolSize` config)
- [x] 7.2 Each worker calls `ProcessBlockSubtree` for its assigned subtree hash
- [x] 7.3 Collect and log errors from workers, continue processing remaining subtrees on individual failures
- [x] 7.4 Update subtree store block height via `subtreeStore.SetCurrentBlockHeight(blockHeight)` after all subtrees processed (triggers DAH pruning)

## 8. DataHub Config

- [x] 8.1 Add `DataHub` config section to `config.go` with `TimeoutSec` (default 30) and `MaxRetries` (default 3)
- [x] 8.2 Add corresponding YAML config and env var bindings (`DATAHUB_TIMEOUT_SEC`, `DATAHUB_MAX_RETRIES`)
- [x] 8.3 Add `datahub` section to `config.yaml` with documented defaults

## 9. Tests

- [x] 9.1 Test subtree processor `handleMessage` — mock DataHub returns valid subtree, verify blob store write, verify SEEN_ON_NETWORK callback emitted for registered txid
- [x] 9.2 Test subtree processor — verify SEEN_MULTIPLE_NODES emitted when seen threshold reached
- [x] 9.3 Test `ProcessBlockSubtree` — mock subtree data, verify STUMP built and MINED callback emitted with encoded STUMP
- [x] 9.4 Test `ProcessBlockSubtree` — verify registration TTL update called
- [x] 9.5 Test block processor — mock DataHub returns block metadata with 3 subtrees, verify all 3 processed

## 10. Build Verification

- [x] 10.1 Run `go build ./...` to verify full project compiles
- [x] 10.2 Run `go vet ./...` to check for issues
- [x] 10.3 Run `go test ./...` to verify all tests pass
