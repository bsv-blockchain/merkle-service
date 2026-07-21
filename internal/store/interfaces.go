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
	// ScheduleDelete schedules an already-stored subtree for pruning at
	// blockHeight + dahOffset without rewriting its bytes — the block-time
	// complement to StoreSubtree's announcement-time write (which cannot set
	// a DAH because the height is unknown then). blockHeight 0 is a no-op.
	ScheduleDelete(id string, blockHeight uint64) error
	SetCurrentBlockHeight(height uint64)
}

// CallbackDedupStore tracks whether a (txid, url, statusType) combination has
// already been delivered so retries don't double-fire callbacks.
type CallbackDedupStore interface {
	Exists(txid, callbackURL, statusType string) (bool, error)
	Record(txid, callbackURL, statusType string, ttl time.Duration) error
	// Delete removes a single dedup entry. No-op when the entry is
	// absent — implementations return nil. /reprocess uses this to clear
	// stale dedup state left behind by a prior DLQ'd attempt so the
	// freshly-emitted callbacks are not skipped as duplicates
	// (bsv-blockchain/merkle-service#122).
	Delete(txid, callbackURL, statusType string) error
}

// CallbackURLRegistry enumerates every known callback URL alongside its
// per-URL bearer token. Add is set-insert keyed on URL — re-registering an
// existing URL refreshes its token and last-seen timestamp.
type CallbackURLRegistry interface {
	Add(callbackURL, callbackToken string) error
	GetAll() ([]CallbackEntry, error)
	// RecordFailure increments the per-URL failure counter after a callback to
	// this URL was DLQ'd. When the counter reaches threshold the URL is
	// disabled so GetAll stops returning it (a subsequent Add re-enables it).
	// Returns whether the URL is now disabled. A non-positive threshold or an
	// unknown URL is a no-op.
	RecordFailure(callbackURL string, threshold int) (disabled bool, err error)
}

// DataHubRegistry remembers every DataHub URL the block processor has
// successfully fetched block metadata from. The /reprocess endpoint reads
// this set (combined with operator-configured fallbacks) to find a DataHub
// that can serve a past block when the API caller doesn't know which
// DataHubs are live on the network. Add upserts and refreshes a per-URL TTL
// so dead URLs eventually drop off.
type DataHubRegistry interface {
	Add(dataHubURL string) error
	GetAll() ([]string, error)
}

// CallbackAccumulatorStore aggregates per-block, per-URL callback data across
// subtrees, then hands it off atomically for dispatch via ReadAndDelete.
type CallbackAccumulatorStore interface {
	Append(blockHash, callbackURL string, txids []string, subtreeIndex int, stumpData []byte) error
	ReadAndDelete(blockHash string) (map[string]*AccumulatedCallback, error)
}

// SeenCounterStore tracks weighted confidence that mining nodes have seen a
// txid. Each peerID is recorded at most once per txid, contributing its
// current node weight (blocks in the last-W tip window) to the score.
// ThresholdReached fires exactly once when the score first reaches the
// configured threshold (F-045).
//
// BatchAddPeer is the subtree-fetcher hot path: one peer observation applied
// to many registered txids. It returns a result per txid that succeeded plus
// the first error (F-058 partial-success: emit callbacks for successes,
// redeliver on error; operations are idempotent under re-runs).
//
// The former unique-subtree Increment/BatchIncrement API is replaced by this
// peer-weighted model.
type SeenCounterStore interface {
	AddPeer(txid, peerID string, weight int) (*IncrementResult, error)
	BatchAddPeer(txids []string, peerID string, weight int) (map[string]*IncrementResult, error)
	// BatchDelete removes the counters for txids. Called at mine time: a
	// counter tracks pre-mine propagation, so once its txid is in a block
	// the record is dead weight — this is the event-driven cleanup that
	// keeps the set from growing forever. Missing txids are not an error
	// (idempotent, safe on work-item redelivery).
	BatchDelete(txids []string) error
	Threshold() int
}

// SubtreeAttributionStore records the first peer to announce each subtree hash.
type SubtreeAttributionStore interface {
	// TryAttribute returns the stored peer for subtreeHash. first is true only
	// when this call won the first-seen race and inserted peerID.
	TryAttribute(subtreeHash, peerID string) (attributedPeer string, first bool, err error)
}

// BlockAttributionStore persists first-seen block→peer attributions for the
// node registry (shared across k8s replicas).
type BlockAttributionStore interface {
	TryAttribute(hash, prevHash, peerID string, height uint32) (attributedPeer string, first bool, err error)
	ListAll() ([]BlockAttribution, error)
	DeleteHashes(hashes []string) error
}

// BlockAttribution is a persisted first-seen block announcement.
type BlockAttribution struct {
	Hash     string
	PrevHash string
	Height   uint32
	PeerID   string
}

// BlockProcessedData is the canonical block-level data the producer attaches
// to a BLOCK_PROCESSED callback so downstream consumers can build and validate
// a compound BUMP without fetching the block from a teranode datahub.
//
// It is stamped onto the per-block subtree-counter record at Init time (when
// the block processor has the data in hand) and read back when the counter
// drains to zero and BLOCK_PROCESSED fires. All hashes are display-order hex
// (matching BlockHash / the chainhash.String() convention used elsewhere on
// the wire); SubtreeHashes are the canonical (coinbase-placeholder-based)
// subtree roots exactly as teranode stores them — consumers correct index 0
// using CoinbaseBUMP. CoinbaseBUMP is the hex-encoded BRC-74 merkle path of the
// coinbase transaction up to the block merkle root; it is empty when the
// producer could not build it (the consumer then falls back to a datahub).
type BlockProcessedData struct {
	MerkleRoot    string   `json:"merkleRoot,omitempty"`
	SubtreeCount  int      `json:"subtreeCount"`
	SubtreeHashes []string `json:"subtreeHashes,omitempty"`
	CoinbaseBUMP  string   `json:"coinbaseBump,omitempty"`
}

// SubtreeCounterStore coordinates BLOCK_PROCESSED emission: Init sets the
// expected subtree count for a block (and stashes the BlockProcessedData to
// surface on the callback), Decrement atomically counts one subtree as done
// and returns the remaining count (caller fires BLOCK_PROCESSED at 0). When the
// returned remaining count is <= 0, Decrement also returns the stashed
// BlockProcessedData so the caller can populate the callback without an extra
// round trip; it is nil otherwise (and may be nil even at zero if Init was
// called without data or the record was rewritten).
type SubtreeCounterStore interface {
	Init(blockHash string, count int, data *BlockProcessedData) error
	Decrement(blockHash string) (remaining int, data *BlockProcessedData, err error)
}

// ExpectedStumpStore records, per (block, callbackURL), the set of subtree
// indices that produced a STUMP for that URL. BLOCK_PROCESSED carries this set
// so the receiver knows exactly which STUMPs to expect and can detect a missing
// one (STUMPs are sparse — only subtrees with a tracked tx produce one — so the
// receiver cannot otherwise tell a legitimately-absent STUMP from a lost one).
//
// Adds are idempotent per (block, URL, index): a re-driven subtree work item
// never double-counts. The record's TTL is re-stamped on every add and is sized
// to outlive the block's processing, mirroring the subtree counter.
type ExpectedStumpStore interface {
	// AddSubtreeIndex records that subtreeIndex produced a STUMP for each URL in
	// callbackURLs, within blockHash. Must be durable before the block's subtree
	// counter drains to zero so the set is complete when BLOCK_PROCESSED is read.
	AddSubtreeIndex(blockHash string, subtreeIndex int, callbackURLs []string) error
	// GetSubtreeIndices returns the recorded subtree indices for (block, URL) in
	// ascending order, or an empty slice if none were recorded.
	GetSubtreeIndices(blockHash, callbackURL string) ([]int, error)
}

// BackendHealth reports whether the underlying backend (Aerospike cluster, SQL
// connection pool) is reachable. Used by the API /health endpoint.
type BackendHealth interface {
	Healthy() bool
}
