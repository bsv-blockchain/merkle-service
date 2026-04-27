package store

import (
	"errors"
	"fmt"
	"log/slog"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

const accumEntriesBin = "entries"

// aerospikeCallbackAccumulator is the Aerospike-backed CallbackAccumulatorStore
// implementation. Subtree workers append entries as they process subtrees. When
// all subtrees for a block are done, the last worker reads and deletes the
// accumulated data, then publishes individual CallbackTopicMessages.
type aerospikeCallbackAccumulator struct {
	client      *AerospikeClient
	setName     string
	ttlSec      int
	maxRetries  int
	retryBaseMs int
	logger      *slog.Logger
}

var _ CallbackAccumulatorStore = (*aerospikeCallbackAccumulator)(nil)

func NewCallbackAccumulatorStore(client *AerospikeClient, setName string, ttlSec int, maxRetries int, retryBaseMs int, logger *slog.Logger) CallbackAccumulatorStore {
	return &aerospikeCallbackAccumulator{
		client:      client,
		setName:     setName,
		ttlSec:      ttlSec,
		maxRetries:  maxRetries,
		retryBaseMs: retryBaseMs,
		logger:      logger,
	}
}

// Append atomically appends callback data for a set of txids from a single subtree
// to the accumulation record for the given block. One entry per subtree per callback URL,
// keeping STUMP data stored only once per subtree (not duplicated per txid).
func (s *aerospikeCallbackAccumulator) Append(blockHash, callbackURL string, txids []string, subtreeIndex int, stumpData []byte) error {
	key, err := as.NewKey(s.client.Namespace(), s.setName, blockHash)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	// Store one entry per subtree: txid list + subtree index + STUMP data.
	entry := map[string]interface{}{
		"u": callbackURL,
		"t": txids,
		"i": subtreeIndex,
		"s": stumpData,
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE
	wp.Expiration = uint32(s.ttlSec)

	_, err = s.client.Client().Operate(wp, key,
		as.ListAppendOp(accumEntriesBin, entry),
	)
	if err != nil {
		return fmt.Errorf("failed to append to accumulator: %w", err)
	}
	return nil
}

// ReadAndDelete reads all accumulated callback data for the given block and
// deletes the record atomically. Returns a map of callbackURL → AccumulatedCallback.
func (s *aerospikeCallbackAccumulator) ReadAndDelete(blockHash string) (map[string]*AccumulatedCallback, error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, blockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to create key: %w", err)
	}

	// Read the record first.
	record, err := s.client.Client().Get(nil, key, accumEntriesBin)
	if err != nil {
		var asErr *as.AerospikeError
		if errors.As(err, &asErr) && asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read accumulator: %w", err)
	}
	if record == nil {
		return nil, nil
	}

	// Delete the record.
	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	_, err = s.client.Client().Delete(wp, key)
	if err != nil {
		s.logger.Warn("failed to delete accumulator record after read",
			"blockHash", blockHash, "error", err)
	}

	// Parse the entries list.
	binVal := record.Bins[accumEntriesBin]
	if binVal == nil {
		return nil, nil
	}

	list, ok := binVal.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected bin type for accumulator entries: %T", binVal)
	}

	result := make(map[string]*AccumulatedCallback)
	for _, item := range list {
		entryMap, ok := item.(map[interface{}]interface{})
		if !ok {
			continue
		}

		url, _ := entryMap["u"].(string)
		if url == "" {
			continue
		}

		acc, exists := result[url]
		if !exists {
			acc = &AccumulatedCallback{}
			result[url] = acc
		}

		entry := AccumulatedCallbackEntry{}

		// Parse txid list.
		if txidList, ok := entryMap["t"].([]interface{}); ok {
			for _, v := range txidList {
				if s, ok := v.(string); ok {
					entry.TxIDs = append(entry.TxIDs, s)
				}
			}
		}

		if idx, ok := entryMap["i"].(int); ok {
			entry.SubtreeIndex = idx
		}
		if data, ok := entryMap["s"].([]byte); ok {
			entry.StumpData = data
		}

		acc.Entries = append(acc.Entries, entry)
	}

	return result, nil
}
