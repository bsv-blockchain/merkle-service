package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

// Claim atomically reserves the dedup slot for (txid, callbackURL, statusType).
// It uses Aerospike's CREATE_ONLY record-exists action so concurrent workers
// cannot both observe "not exists" and double-deliver: at most one writer
// successfully creates the record, the rest receive KEY_EXISTS_ERROR which we
// translate to claimed=false.
//
// Returns:
//   - (true, nil)  — this caller created the record and SHOULD deliver.
//   - (false, nil) — record already existed (duplicate); caller MUST skip.
//   - (_, err)     — backend error; caller should treat as transient and retry.
func (s *aerospikeCallbackDedup) Claim(txid, callbackURL, statusType string, ttl time.Duration) (bool, error) {
	keyStr := dedupKey(txid, callbackURL, statusType)
	key, err := as.NewKey(s.client.Namespace(), s.setName, keyStr)
	if err != nil {
		return false, fmt.Errorf("failed to create dedup key: %w", err)
	}

	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.CREATE_ONLY
	if ttl > 0 {
		wp.Expiration = uint32(ttl.Seconds())
	}

	bins := as.BinMap{dedupMarkerBin: 1}
	putErr := s.client.Client().Put(wp, key, bins)
	if putErr == nil {
		return true, nil
	}

	// CREATE_ONLY collision = a prior (or concurrent) writer claimed it first.
	// This is the dedup-hit signal, not an error.
	var asErr as.Error
	if errors.As(putErr, &asErr) && asErr.Matches(astypes.KEY_EXISTS_ERROR) {
		return false, nil
	}

	// If TTL is rejected (namespace lacks nsup-period), retry without TTL —
	// preserves the prior behavior of degrading gracefully on misconfigured
	// namespaces. Still CREATE_ONLY so the atomic-claim guarantee holds.
	if errors.As(putErr, &asErr) && asErr.Matches(astypes.FAIL_FORBIDDEN) && ttl > 0 {
		s.logger.Warn("callback dedup TTL rejected, writing without TTL (configure Aerospike nsup-period to enable TTL)",
			"txid", txid, "statusType", statusType)
		wp2 := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
		wp2.RecordExistsAction = as.CREATE_ONLY
		if err2 := s.client.Client().Put(wp2, key, bins); err2 != nil {
			var asErr2 as.Error
			if errors.As(err2, &asErr2) && asErr2.Matches(astypes.KEY_EXISTS_ERROR) {
				return false, nil
			}
			return false, fmt.Errorf("failed to claim dedup (without TTL): %w", err2)
		}
		return true, nil
	}

	return false, fmt.Errorf("failed to claim dedup: %w", putErr)
}
