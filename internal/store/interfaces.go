package store

import (
	"io"
	"time"
)

// CallbackEntry is a (URL, token) tuple returned by the registration stores.
// Token is "" for legacy registrations that predate the per-callback bearer
// token (arcade /watch payloads without callbackToken). Deliveries should
// only attach an Authorization header when Token is non-empty so empty-token
// rollouts preserve today's no-auth behavior.
type CallbackEntry struct {
	URL   string
	Token string
}

// RegistrationStore maps a txid to the set of callback (URL, token) entries
// registered for it. Add is set-insert keyed on URL: re-registering the same
// (txid, url) pair refreshes the token and is otherwise a no-op.
type RegistrationStore interface {
	Add(txid, callbackURL, callbackToken string) error
	Get(txid string) ([]CallbackEntry, error)
	BatchGet(txids []string) (map[string][]CallbackEntry, error)
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
//
// Claim atomically reserves a dedup slot. It returns claimed=true iff this
// caller is the first to record the (txid, url, statusType) tuple — meaning
// the caller is responsible for delivering the callback. claimed=false
// indicates the tuple was already recorded by a prior delivery (or a
// concurrent racer that won the claim) and the caller MUST skip delivery to
// avoid double-firing. Combining the prior Exists+Record pair into a single
// atomic operation closes the read-modify-write race that allowed two
// concurrent workers to both observe "not exists" and double-deliver.
type CallbackDedupStore interface {
	Claim(txid, callbackURL, statusType string, ttl time.Duration) (claimed bool, err error)
}

// CallbackURLRegistry enumerates every known callback URL alongside its
// per-URL bearer token. Add is set-insert keyed on URL — re-registering an
// existing URL refreshes its token and last-seen timestamp.
type CallbackURLRegistry interface {
	Add(callbackURL, callbackToken string) error
	GetAll() ([]CallbackEntry, error)
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
	Increment(txid, subtreeID string) (*IncrementResult, error)
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
