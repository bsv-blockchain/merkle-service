package store

import (
	"fmt"
	"log/slog"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const subtreeAttrPeerBin = "peer"

type aerospikeSubtreeAttributionStore struct {
	client      *AerospikeClient
	setName     string
	ttlSec      int
	logger      *slog.Logger
	maxRetries  int
	retryBaseMs int
}

var _ SubtreeAttributionStore = (*aerospikeSubtreeAttributionStore)(nil)

func NewSubtreeAttributionStore(client *AerospikeClient, setName string, ttlSec, maxRetries, retryBaseMs int, logger *slog.Logger) SubtreeAttributionStore {
	if ttlSec <= 0 {
		ttlSec = 86400
	}
	return &aerospikeSubtreeAttributionStore{
		client: client, setName: setName, ttlSec: ttlSec, logger: logger,
		maxRetries: maxRetries, retryBaseMs: retryBaseMs,
	}
}

func (s *aerospikeSubtreeAttributionStore) TryAttribute(subtreeHash, peerID string) (string, bool, error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, subtreeHash)
	if err != nil {
		return "", false, err
	}
	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.CREATE_ONLY
	wp.Expiration = uint32(s.ttlSec) //nolint:gosec
	err = s.client.Client().Put(wp, key, as.BinMap{subtreeAttrPeerBin: peerID})
	if err == nil {
		return peerID, true, nil
	}
	if !isKeyExistsError(err) {
		return "", false, fmt.Errorf("create subtree attribution: %w", err)
	}
	rec, err := s.client.Client().Get(s.client.ReadPolicy(), key, subtreeAttrPeerBin)
	if err != nil {
		return "", false, fmt.Errorf("read subtree attribution: %w", err)
	}
	stored, _ := rec.Bins[subtreeAttrPeerBin].(string)
	return stored, false, nil
}
