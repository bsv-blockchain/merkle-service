package store

import (
	"io"
	"time"
)

// RegistrationStore maps a txid to the set of callback URLs registered for it.
// Add is set-insert: duplicate (txid, url) pairs are silently deduplicated.
type RegistrationStore interface {
	Add(txid string, callbackURL string) error
	Get(txid string) ([]string, error)
	BatchGet(txids []string) (map[string][]string, error)
	UpdateTTL(txid string, ttl time.Duration) error
	BatchUpdateTTL(txids []string, ttl time.Duration) error
}

// StumpStore provides content-addressed STUMP payload storage with
// delete-at-height pruning. Put returns a ref that Get/Delete resolve.
type StumpStore interface {
	Put(data []byte, blockHeight uint64) (string, error)
	Get(ref string) ([]byte, error)
	Delete(ref string) error
}

// SubtreeStore provides subtree payload storage with delete-at-height pruning.
type SubtreeStore interface {
	StoreSubtree(id string, data []byte, blockHeight uint64) error
	StoreSubtreeFromReader(id string, r io.Reader, size int64, blockHeight uint64) error
	GetSubtree(id string) ([]byte, error)
	GetSubtreeReader(id string) (io.ReadCloser, error)
	DeleteSubtree(id string) error
	SetCurrentBlockHeight(height uint64)
}

// CallbackDedupStore tracks whether a (txid, url, statusType) combination has
// already been delivered so retries don't double-fire callbacks.
type CallbackDedupStore interface {
	Exists(txid, callbackURL, statusType string) (bool, error)
	Record(txid, callbackURL, statusType string, ttl time.Duration) error
}

// CallbackURLRegistry enumerates every known callback URL. Add is set-insert.
type CallbackURLRegistry interface {
	Add(callbackURL string) error
	GetAll() ([]string, error)
}

// CallbackAccumulatorStore aggregates per-block, per-URL callback data across
// subtrees, then hands it off atomically for dispatch via ReadAndDelete.
type CallbackAccumulatorStore interface {
	Append(blockHash, callbackURL string, txids []string, subtreeIndex int, stumpData []byte) error
	ReadAndDelete(blockHash string) (map[string]*AccumulatedCallback, error)
}

// SeenCounterStore tracks how many distinct subtrees have reported a txid.
// Increment fires ThresholdReached exactly once — when the unique count first
// reaches the configured threshold.
type SeenCounterStore interface {
	Increment(txid string, subtreeID string) (*IncrementResult, error)
	Threshold() int
}

// SubtreeCounterStore coordinates BLOCK_PROCESSED emission: Init sets the
// expected subtree count for a block, Decrement atomically counts one subtree
// as done and returns the remaining count (caller fires BLOCK_PROCESSED at 0).
type SubtreeCounterStore interface {
	Init(blockHash string, count int) error
	Decrement(blockHash string) (remaining int, err error)
}

// BackendHealth reports whether the underlying backend (Aerospike cluster, SQL
// connection pool) is reachable. Used by the API /health endpoint.
type BackendHealth interface {
	Healthy() bool
}
