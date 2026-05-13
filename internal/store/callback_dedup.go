package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

const (
	dedupMarkerBin = "d"
)

// aerospikeCallbackDedup is the Aerospike-backed CallbackDedupStore implementation.
type aerospikeCallbackDedup struct {
	client      *AerospikeClient
	setName     string
	logger      *slog.Logger
	maxRetries  int
	retryBaseMs int
}

var _ CallbackDedupStore = (*aerospikeCallbackDedup)(nil)

func NewCallbackDedupStore(client *AerospikeClient, setName string, maxRetries, retryBaseMs int, logger *slog.Logger) CallbackDedupStore {
	return &aerospikeCallbackDedup{
		client:      client,
		setName:     setName,
		logger:      logger,
		maxRetries:  maxRetries,
		retryBaseMs: retryBaseMs,
	}
}

// dedupKey builds a deterministic Aerospike key from the callback parameters.
// Uses SHA-256 to keep key size bounded regardless of callbackURL length.
func dedupKey(txid, callbackURL, statusType string) string {
	h := sha256.Sum256([]byte(txid + ":" + callbackURL + ":" + statusType))
	return hex.EncodeToString(h[:])
}

// Exists checks if a callback delivery has already been recorded.
func (s *aerospikeCallbackDedup) Exists(txid, callbackURL, statusType string) (bool, error) {
	keyStr := dedupKey(txid, callbackURL, statusType)
	key, err := as.NewKey(s.client.Namespace(), s.setName, keyStr)
	if err != nil {
		return false, fmt.Errorf("failed to create dedup key: %w", err)
	}

	exists, err := s.client.Client().Exists(s.client.ReadPolicy(), key)
	if err != nil {
		return false, fmt.Errorf("failed to check dedup record: %w", err)
	}
	return exists, nil
}

// Delete removes a dedup entry. Used by /reprocess to clear stale dedup
// state left by a previous DLQ'd attempt so freshly-emitted callbacks
// are not skipped as duplicates. Deleting an absent key is a no-op
// (returns nil) — the Aerospike client returns existed=false in that
// case, which we treat as success since the intent is idempotent
// removal.
func (s *aerospikeCallbackDedup) Delete(txid, callbackURL, statusType string) error {
	keyStr := dedupKey(txid, callbackURL, statusType)
	key, err := as.NewKey(s.client.Namespace(), s.setName, keyStr)
	if err != nil {
		return fmt.Errorf("failed to create dedup key: %w", err)
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	if _, err := s.client.Client().Delete(wp, key); err != nil {
		return fmt.Errorf("failed to delete dedup record: %w", err)
	}
	return nil
}

// Record marks a callback delivery as completed with a TTL.
func (s *aerospikeCallbackDedup) Record(txid, callbackURL, statusType string, ttl time.Duration) error {
	keyStr := dedupKey(txid, callbackURL, statusType)
	key, err := as.NewKey(s.client.Namespace(), s.setName, keyStr)
	if err != nil {
		return fmt.Errorf("failed to create dedup key: %w", err)
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	if ttl > 0 {
		wp.Expiration = uint32(ttl.Seconds())
	}

	bins := as.BinMap{dedupMarkerBin: 1}
	if err := s.client.Client().Put(wp, key, bins); err != nil {
		// If TTL is rejected (namespace lacks nsup-period), retry without TTL.
		if err.Matches(astypes.FAIL_FORBIDDEN) && ttl > 0 {
			s.logger.Warn("callback dedup TTL rejected, writing without TTL (configure Aerospike nsup-period to enable TTL)",
				"txid", txid, "statusType", statusType)
			wp2 := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
			if err2 := s.client.Client().Put(wp2, key, bins); err2 != nil {
				return fmt.Errorf("failed to record dedup (without TTL): %w", err2)
			}
			return nil
		}
		return fmt.Errorf("failed to record dedup: %w", err)
	}
	return nil
}
