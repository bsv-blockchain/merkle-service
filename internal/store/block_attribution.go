package store

import (
	"errors"
	"fmt"
	"log/slog"

	as "github.com/aerospike/aerospike-client-go/v8"
	astypes "github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	blockAttrPrevBin   = "prev"
	blockAttrHeightBin = "height"
	blockAttrPeerBin   = "peer"
	blockAttrIndexKey  = "_index"
	blockAttrHashesBin = "hashes"
)

type aerospikeBlockAttributionStore struct {
	client      *AerospikeClient
	setName     string
	logger      *slog.Logger
	maxRetries  int
	retryBaseMs int
}

var _ BlockAttributionStore = (*aerospikeBlockAttributionStore)(nil)

func NewBlockAttributionStore(client *AerospikeClient, setName string, maxRetries, retryBaseMs int, logger *slog.Logger) BlockAttributionStore {
	return &aerospikeBlockAttributionStore{
		client: client, setName: setName, logger: logger,
		maxRetries: maxRetries, retryBaseMs: retryBaseMs,
	}
}

func (s *aerospikeBlockAttributionStore) TryAttribute(hash, prevHash, peerID string, height uint32) (string, bool, error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, hash)
	if err != nil {
		return "", false, err
	}
	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.CREATE_ONLY
	bins := as.BinMap{
		blockAttrPrevBin:   prevHash,
		blockAttrHeightBin: int(height),
		blockAttrPeerBin:   peerID,
	}
	err = s.client.Client().Put(wp, key, bins)
	if err == nil {
		_ = s.addToIndex(hash)
		return peerID, true, nil
	}
	if !isKeyExistsError(err) {
		return "", false, fmt.Errorf("create block attribution: %w", err)
	}
	rp := s.client.ReadPolicy()
	rec, err := s.client.Client().Get(rp, key, blockAttrPeerBin)
	if err != nil {
		return "", false, fmt.Errorf("read block attribution: %w", err)
	}
	stored, _ := rec.Bins[blockAttrPeerBin].(string)
	return stored, false, nil
}

func (s *aerospikeBlockAttributionStore) ListAll() ([]BlockAttribution, error) {
	hashes, err := s.listIndex()
	if err != nil {
		return nil, err
	}
	out := make([]BlockAttribution, 0, len(hashes))
	rp := s.client.ReadPolicy()
	for _, h := range hashes {
		key, err := as.NewKey(s.client.Namespace(), s.setName, h)
		if err != nil {
			continue
		}
		rec, err := s.client.Client().Get(rp, key)
		if err != nil {
			var asErr as.Error
			if errors.As(err, &asErr) && asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
				continue
			}
			return nil, err
		}
		if rec == nil {
			continue
		}
		attr := BlockAttribution{Hash: h}
		if v, ok := rec.Bins[blockAttrPrevBin].(string); ok {
			attr.PrevHash = v
		}
		if v, ok := rec.Bins[blockAttrPeerBin].(string); ok {
			attr.PeerID = v
		}
		// Heights are chain heights and always fit in uint32 for BSV.
		h := asInt(rec.Bins[blockAttrHeightBin])
		if h < 0 {
			h = 0
		}
		if h > int(^uint32(0)) {
			h = int(^uint32(0))
		}
		attr.Height = uint32(h)
		out = append(out, attr)
	}
	return out, nil
}

func (s *aerospikeBlockAttributionStore) DeleteHashes(hashes []string) error {
	for _, h := range hashes {
		key, err := as.NewKey(s.client.Namespace(), s.setName, h)
		if err != nil {
			continue
		}
		_, _ = s.client.Client().Delete(s.client.WritePolicy(s.maxRetries, s.retryBaseMs), key)
		_ = s.removeFromIndex(h)
	}
	return nil
}

func (s *aerospikeBlockAttributionStore) addToIndex(hash string) error {
	key, err := as.NewKey(s.client.Namespace(), s.setName, blockAttrIndexKey)
	if err != nil {
		return err
	}
	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	wp.RecordExistsAction = as.UPDATE
	listPolicy := as.NewListPolicy(as.ListOrderUnordered, as.ListWriteFlagsAddUnique|as.ListWriteFlagsNoFail)
	_, err = s.client.Client().Operate(wp, key, as.ListAppendWithPolicyOp(listPolicy, blockAttrHashesBin, hash))
	return err
}

func (s *aerospikeBlockAttributionStore) removeFromIndex(hash string) error {
	key, err := as.NewKey(s.client.Namespace(), s.setName, blockAttrIndexKey)
	if err != nil {
		return err
	}
	wp := s.client.WritePolicy(s.maxRetries, s.retryBaseMs)
	_, err = s.client.Client().Operate(wp, key, as.ListRemoveByValueOp(blockAttrHashesBin, hash, as.ListReturnTypeNone))
	return err
}

func (s *aerospikeBlockAttributionStore) listIndex() ([]string, error) {
	key, err := as.NewKey(s.client.Namespace(), s.setName, blockAttrIndexKey)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.Client().Get(s.client.ReadPolicy(), key, blockAttrHashesBin)
	if err != nil {
		var asErr as.Error
		if errors.As(err, &asErr) && asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
			return nil, nil
		}
		return nil, err
	}
	if rec == nil || rec.Bins[blockAttrHashesBin] == nil {
		return nil, nil
	}
	raw, ok := rec.Bins[blockAttrHashesBin].([]interface{})
	if !ok {
		return nil, nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

func isKeyExistsError(err error) bool {
	if err == nil {
		return false
	}
	var asErr as.Error
	return errors.As(err, &asErr) && asErr.Matches(astypes.KEY_EXISTS_ERROR)
}
