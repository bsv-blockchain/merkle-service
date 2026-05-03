package block

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"

	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	"github.com/bsv-blockchain/merkle-service/internal/stump"
)

// RegCache is the registration deduplication cache abstraction used by
// block-time subtree processing. Mirrors the shape used by the subtree-fetcher
// so a future cross-process cache implementation can be substituted.
//
// Only positive lookups are cached. Negatives are not cached so that a
// /watch registration arriving after an earlier "not registered"
// observation is not masked until cache eviction (F-020).
type RegCache interface {
	FilterUncached(txids []string) (uncached, cachedRegistered []string)
	SetMultiRegistered(txids []string) error
}

// SubtreeResult holds the callback groups produced by processing a subtree.
// The caller uses this to publish STUMP callback messages.
type SubtreeResult struct {
	// CallbackGroups maps callbackURL → list of matched txids.
	CallbackGroups map[string][]string
	// CallbackTokens maps callbackURL → the bearer token registered for that
	// URL on /watch. The publisher attaches the token to each
	// CallbackTopicMessage so deliveries to arcade carry the
	// `Authorization: Bearer <token>` header arcade requires. URLs without a
	// configured token map to "" (no Authorization header) which preserves
	// pre-token-rollout behavior.
	CallbackTokens map[string]string
	// SubtreeHash is the hash of the processed subtree.
	SubtreeHash string
	// StumpData is the serialized STUMP binary (BRC-0074 format).
	StumpData []byte
}

// ProcessBlockSubtree processes a single subtree within a block: retrieves the
// subtree data, checks registrations, builds a STUMP, and returns callback groups.
// Returns (result, error) where result is nil if no registered txids were found.
//
// regCache, when non-nil, is consulted before hitting Aerospike — txids known
// to be unregistered (typically the bulk of mempool, populated during the
// SEEN_ON_NETWORK pass in subtree-fetcher) skip the BatchGet entirely.
//
// batchSem, when non-nil, gates the registration BatchGet so only N calls are
// in flight per process (avoids exhausting the Aerospike connection pool when
// a block fans out 14+ subtrees in parallel).
func ProcessBlockSubtree(
	ctx context.Context,
	subtreeHash string,
	blockHeight uint64,
	blockHash string,
	dataHubURL string,
	dhClient *datahub.Client,
	subtreeStore store.SubtreeStore,
	regStore store.RegistrationStore,
	regCache RegCache,
	batchSem chan struct{},
	postMineTTLSec int,
	logger *slog.Logger,
) (*SubtreeResult, error) {
	// 6.2: Retrieve subtree data from blob store, falling back to DataHub.
	rawData, err := subtreeStore.GetSubtree(subtreeHash)
	if err != nil || rawData == nil {
		logger.Debug("subtree not in blob store, fetching from DataHub",
			"subtreeHash", subtreeHash,
			"blockHash", blockHash,
		)
		rawData, err = dhClient.FetchSubtreeRaw(ctx, dataHubURL, subtreeHash)
		if err != nil {
			return nil, fmt.Errorf("fetching subtree %s from DataHub: %w", subtreeHash, err)
		}
		// Store for potential future use.
		if storeErr := subtreeStore.StoreSubtree(subtreeHash, rawData, blockHeight); storeErr != nil {
			logger.Warn("failed to store fetched subtree", "hash", subtreeHash, "error", storeErr)
		}
	}

	// 6.3: Parse raw binary data into nodes.
	// DataHub returns concatenated 32-byte hashes, not full go-subtree Serialize() format.
	nodes, err := datahub.ParseRawNodes(rawData)
	if err != nil {
		return nil, fmt.Errorf("parsing subtree %s: %w", subtreeHash, err)
	}

	if len(nodes) == 0 {
		return nil, nil
	}

	// 6.4: Extract TxIDs and batch-lookup registrations.
	txids := make([]string, len(nodes))
	txidToIndex := make(map[string]int, len(nodes))
	for i, node := range nodes {
		txid := node.Hash.String()
		txids[i] = txid
		txidToIndex[txid] = i
	}

	registrations, err := lookupRegistrations(ctx, txids, regStore, regCache, batchSem)
	if err != nil {
		return nil, fmt.Errorf("batch get registrations for subtree %s: %w", subtreeHash, err)
	}

	if len(registrations) == 0 {
		return nil, nil
	}

	// Reduce CallbackEntry tuples back into the (txid → urls) shape that
	// the STUMP grouping logic expects, while capturing the latest token
	// per URL on the side. If multiple txids have the same callbackURL with
	// different tokens (e.g. mid-rotation), the last token observed wins —
	// in practice every txid registered against a given URL went through
	// the same /watch payload, so they will agree.
	registrationsByTxID := make(map[string][]string, len(registrations))
	urlTokens := make(map[string]string)
	for txid, entries := range registrations {
		urls := make([]string, 0, len(entries))
		for _, e := range entries {
			urls = append(urls, e.URL)
			if _, ok := urlTokens[e.URL]; !ok || e.Token != "" {
				urlTokens[e.URL] = e.Token
			}
		}
		registrationsByTxID[txid] = urls
	}

	// 6.5: Build full merkle tree from subtree nodes.
	merkleTreeStore, err := subtreepkg.BuildMerkleTreeStoreFromBytes(nodes)
	if err != nil {
		return nil, fmt.Errorf("building merkle tree for subtree %s: %w", subtreeHash, err)
	}

	// Convert leaf hashes to [][]byte.
	leaves := make([][]byte, len(nodes))
	for i, node := range nodes {
		hashCopy := make([]byte, chainhash.HashSize)
		copy(hashCopy, node.Hash[:])
		leaves[i] = hashCopy
	}

	// Convert internal nodes (from BuildMerkleTreeStoreFromBytes) to [][]byte.
	internalNodes := make([][]byte, len(*merkleTreeStore))
	for i, h := range *merkleTreeStore {
		hashCopy := make([]byte, chainhash.HashSize)
		copy(hashCopy, h[:])
		internalNodes[i] = hashCopy
	}

	// 6.6: Map registered txids to their leaf indices in the subtree.
	registeredIndices := make(map[int]string)
	for txid := range registrations {
		if idx, ok := txidToIndex[txid]; ok {
			registeredIndices[idx] = txid
		}
	}

	// 6.7: Build STUMP.
	s := stump.Build(blockHeight, leaves, internalNodes, registeredIndices)
	if s == nil {
		return nil, nil
	}

	// 6.8: Encode STUMP to BRC-0074 binary.
	stumpData := s.Encode()

	// 6.9: Group txids by callback URL.
	callbackGroups := stump.GroupByCallback(registrationsByTxID)

	// 6.11: Batch update registration TTLs (skip if postMineTTLSec is 0).
	if postMineTTLSec > 0 {
		registeredTxids := make([]string, 0, len(registrations))
		for txid := range registrations {
			registeredTxids = append(registeredTxids, txid)
		}
		ttl := time.Duration(postMineTTLSec) * time.Second
		if err := regStore.BatchUpdateTTL(registeredTxids, ttl); err != nil {
			logger.Warn("failed to batch update TTLs (ensure Aerospike namespace has nsup-period configured)", "error", err)
		}
	}

	logger.Info("processed block subtree",
		"subtreeHash", subtreeHash,
		"blockHash", blockHash,
		"registeredTxids", len(registrations),
	)

	return &SubtreeResult{
		CallbackGroups: callbackGroups,
		CallbackTokens: urlTokens,
		SubtreeHash:    subtreeHash,
		StumpData:      stumpData,
	}, nil
}

// lookupRegistrations resolves txid → []CallbackEntry using the in-process
// registration cache (when set) to filter out the unregistered majority before
// issuing a single bounded BatchGet against Aerospike. The semaphore caps the
// number of concurrent BatchGets across all callers in the process.
func lookupRegistrations(
	ctx context.Context,
	txids []string,
	regStore store.RegistrationStore,
	regCache RegCache,
	batchSem chan struct{},
) (map[string][]store.CallbackEntry, error) {
	if len(txids) == 0 {
		return map[string][]store.CallbackEntry{}, nil
	}

	uncached := txids
	var cachedRegistered []string
	if regCache != nil {
		uncached, cachedRegistered = regCache.FilterUncached(txids)
	}

	// Build the union of txids we need URLs for: uncached (need to discover
	// registration status) + cachedRegistered (status known, but URL list still
	// lives in Aerospike). Combining into one BatchGet is cheaper than two.
	lookup := uncached
	if len(cachedRegistered) > 0 {
		lookup = make([]string, 0, len(uncached)+len(cachedRegistered))
		lookup = append(lookup, uncached...)
		lookup = append(lookup, cachedRegistered...)
	}

	if len(lookup) == 0 {
		return map[string][]store.CallbackEntry{}, nil
	}

	registered, err := batchGetWithSem(ctx, lookup, regStore, batchSem)
	if err != nil {
		return nil, err
	}

	// Update cache with discoveries from the uncached portion. Only
	// positive results are cached; negatives are intentionally not
	// cached so a later /watch registration is not hidden by a stale
	// negative entry (F-020).
	if regCache != nil && len(uncached) > 0 && len(registered) > 0 {
		foundTxids := make([]string, 0, len(registered))
		for _, txid := range uncached {
			if _, found := registered[txid]; found {
				foundTxids = append(foundTxids, txid)
			}
		}
		if len(foundTxids) > 0 {
			_ = regCache.SetMultiRegistered(foundTxids)
		}
	}

	return registered, nil
}

// batchGetWithSem runs RegistrationStore.BatchGet under the optional semaphore.
// Returns ctx.Err() if the context is canceled while waiting for a slot.
func batchGetWithSem(
	ctx context.Context,
	txids []string,
	regStore store.RegistrationStore,
	batchSem chan struct{},
) (map[string][]store.CallbackEntry, error) {
	if batchSem != nil {
		select {
		case batchSem <- struct{}{}:
			defer func() { <-batchSem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return regStore.BatchGet(txids)
}
