package block

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"

	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/store"
	"github.com/bsv-blockchain/merkle-service/internal/stump"
)

// SubtreeResult holds the callback groups produced by processing a subtree.
// The caller uses this to publish STUMP callback messages.
type SubtreeResult struct {
	// CallbackGroups maps callbackURL → list of matched txids.
	CallbackGroups map[string][]string
	// SubtreeHash is the hash of the processed subtree.
	SubtreeHash string
	// StumpData is the serialized STUMP binary (BRC-0074 format).
	StumpData []byte
}

// ProcessBlockSubtree processes a single subtree within a block: retrieves the
// subtree data, checks registrations, builds a STUMP, and returns callback groups.
// Returns (result, error) where result is nil if no registered txids were found.
func ProcessBlockSubtree(
	ctx context.Context,
	subtreeHash string,
	blockHeight uint64,
	blockHash string,
	dataHubURL string,
	dhClient *datahub.Client,
	subtreeStore store.SubtreeStore,
	regStore store.RegistrationStore,
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

	registrations, err := regStore.BatchGet(txids)
	if err != nil {
		return nil, fmt.Errorf("batch get registrations for subtree %s: %w", subtreeHash, err)
	}

	if len(registrations) == 0 {
		return nil, nil
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
	callbackGroups := stump.GroupByCallback(registrations)

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
		SubtreeHash:    subtreeHash,
		StumpData:      stumpData,
	}, nil
}
