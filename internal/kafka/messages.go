package kafka

import (
	"encoding/json"
	"time"
)

// CallbackType represents the type of callback message, matching Arcade's CallbackType.
type CallbackType string

const (
	CallbackSeenOnNetwork     CallbackType = "SEEN_ON_NETWORK"
	CallbackSeenMultipleNodes CallbackType = "SEEN_MULTIPLE_NODES"
	CallbackStump             CallbackType = "STUMP"
	CallbackBlockProcessed    CallbackType = "BLOCK_PROCESSED"
)

// SubtreeMessage represents a subtree announcement received from P2P.
// AttemptCount is incremented by subtree-fetcher when re-publishing the message
// for retry; on reaching SubtreeConfig.MaxAttempts the message is routed to
// the subtree-dlq topic instead of being re-driven again.
type SubtreeMessage struct {
	Hash         string `json:"hash"`
	DataHubURL   string `json:"dataHubUrl"`
	PeerID       string `json:"peerId"`
	ClientName   string `json:"clientName"`
	AttemptCount int    `json:"attemptCount,omitempty"`

	// AnnouncedAtUnixMs is the wall-clock time (Unix milliseconds) at which
	// the P2P client observed the announcement and published this message.
	// The subtree-fetcher uses it to classify a DataHub 404: teranode's
	// asset cache only retains subtree data for a bounded window (~2h), so
	// a 404 on a message that sat in Kafka past
	// datahub.peerhealth.stale404GraceSec is our own consumer lag, not a
	// lying peer, and must not poison the peer-health breaker. Zero or
	// missing (messages produced before this field existed, retries of
	// such messages) means "age unknown" and is treated as fresh.
	AnnouncedAtUnixMs int64 `json:"announcedAtUnixMs,omitempty"`
}

// BlockMessage represents a block announcement received from P2P, or an
// on-demand reprocess request originated by the API.
//
// Override* fields are set only on reprocess. When OverrideCallbackURL is
// non-empty the block-processor and downstream subtree-worker scope all
// callback delivery to that one URL/token instead of the global
// CallbackURLRegistry: STUMPs are filtered to txids the override URL is
// registered for, BLOCK_PROCESSED fires only at the override URL, and the
// per-block subtree counter uses key (BlockHash|OverrideCallbackURL) so a
// reprocess never collides with a live announcement of the same block.
//
// BypassDedup is also reprocess-only — the block-processor's dedup cache
// is skipped so a hash already seen via P2P can still be reprocessed on
// demand.
type BlockMessage struct {
	Hash                  string `json:"hash"`
	Height                uint32 `json:"height"`
	Header                string `json:"header"`
	Coinbase              string `json:"coinbase"`
	DataHubURL            string `json:"dataHubUrl"`
	PeerID                string `json:"peerId"`
	ClientName            string `json:"clientName"`
	OverrideCallbackURL   string `json:"overrideCallbackUrl,omitempty"`
	OverrideCallbackToken string `json:"overrideCallbackToken,omitempty"`
	BypassDedup           bool   `json:"bypassDedup,omitempty"`
	// ReprocessNonce is a per-request random token set only on reprocess. It is
	// mixed into the subtree-counter key (see block.SubtreeCounterKey) so two
	// concurrent /reprocess requests for the same (BlockHash, OverrideCallbackURL)
	// get independent counters and cannot reset each other's mid-flight. Empty on
	// live announcements, which keep the bare-BlockHash counter key.
	ReprocessNonce string `json:"reprocessNonce,omitempty"`
}

// CallbackTopicMessage is the message published to the callback Kafka topic.
// It wraps the Arcade CallbackMessage fields plus delivery metadata.
//
// CallbackToken is the optional bearer token that the delivery service
// attaches as `Authorization: Bearer <token>` on the outbound HTTP POST.
// Empty / missing means "send no Authorization header" — preserves today's
// behavior for any deployment that hasn't shipped arcade's matching
// /watch token-passing change.
type CallbackTopicMessage struct {
	CallbackURL   string       `json:"callbackUrl"`
	CallbackToken string       `json:"callbackToken,omitempty"`
	Type          CallbackType `json:"type"`
	TxID          string       `json:"txid,omitempty"`
	TxIDs         []string     `json:"txids,omitempty"`
	BlockHash     string       `json:"blockHash,omitempty"`
	// SubtreeHash identifies the subtree this callback originated from. Used
	// for Kafka partitioning so callbacks for one subtree co-locate on one
	// partition (preserving STUMP / SEEN_* / SEEN_MULTIPLE_NODES ordering)
	// while different subtrees spread across the topic. Required for
	// SEEN_ON_NETWORK and SEEN_MULTIPLE_NODES (which don't carry BlockHash);
	// optional for STUMP (where BlockHash + SubtreeIndex already identify
	// the scope) and unset for BLOCK_PROCESSED (block-scoped). Unknown to
	// the receiver — it's a producer-side hint and is not part of the
	// arcade CallbackMessage contract.
	SubtreeHash  string    `json:"subtreeHash,omitempty"`
	SubtreeIndex int       `json:"subtreeIndex,omitempty"`
	StumpRef     string    `json:"stumpRef,omitempty"`
	RetryCount   int       `json:"retryCount,omitempty"`
	NextRetryAt  time.Time `json:"nextRetryAt,omitempty"`

	// BypassDedup marks a callback that must be delivered even if the
	// delivery-side dedup set already holds its key — set only on
	// /reprocess-originated STUMP and BLOCK_PROCESSED callbacks (see
	// bsv-blockchain/merkle-service#208). A /reprocess is an explicit
	// "re-emit this block's results" request, so deduping its callbacks
	// against the original delivery silently defeats the caller's reorg
	// recovery. When true the delivery service skips the dedup existence
	// check but STILL records the key on success, so normal (non-reprocess)
	// duplicates for the same block continue to be suppressed afterward.
	//
	// Additive + omitempty: older consumers ignore it, and live-announcement
	// callbacks leave it false so their dedup behavior is unchanged. It is
	// derived producer-side from OverrideCallbackURL != "" (the established
	// "this is a reprocess" signal) and is not part of the arcade
	// CallbackMessage contract.
	BypassDedup bool `json:"bypassDedup,omitempty"`

	// BLOCK_PROCESSED enrichment (merkle-service issues #123/#124/#125 + the
	// coinbase path). These let a consumer build and validate a compound BUMP
	// without fetching the block from a teranode datahub. All additive and
	// omitempty, so older consumers ignore them. Only populated for
	// BLOCK_PROCESSED messages.
	//
	//   - MerkleRoot:   canonical block header merkle root (display-order hex).
	//   - SubtreeCount: canonical number of subtrees in the block. A pointer so
	//     a coinbase-only block's legitimate 0 is emitted (omitempty drops a nil
	//     pointer, not a *0); only set for BLOCK_PROCESSED, nil for other types.
	//   - SubtreeHashes: canonical (coinbase-placeholder-based) subtree roots,
	//     display-order hex, in subtree-index order — exactly as teranode
	//     stores them; the consumer corrects index 0 using CoinbaseBUMP.
	//   - CoinbaseBUMP: hex-encoded BRC-74 merkle path of the coinbase tx up to
	//     the block merkle root. Empty when the producer couldn't build it (the
	//     consumer then falls back to a datahub).
	MerkleRoot    string   `json:"merkleRoot,omitempty"`
	SubtreeCount  *int     `json:"subtreeCount,omitempty"`
	SubtreeHashes []string `json:"subtreeHashes,omitempty"`
	CoinbaseBUMP  string   `json:"coinbaseBump,omitempty"`

	// ExpectedSubtreeIndices is the set of subtree indices (ascending) that
	// produced a STUMP for this callback URL in this block. Only set on
	// BLOCK_PROCESSED. It lets the receiver detect a missing STUMP — STUMPs are
	// sparse (only subtrees with a tracked tx produce one), so without it a lost
	// STUMP is indistinguishable from a legitimately-absent one and its txs are
	// silently never marked MINED. Additive and omitempty: older consumers
	// ignore it.
	ExpectedSubtreeIndices []int `json:"expectedSubtreeIndices,omitempty"`
}

// PartitionKey returns the Kafka partition key for this callback message.
// Scope-based: callbacks for the same (subtree, URL) or (block, URL) co-locate,
// while different subtrees and blocks spread across partitions.
//
// Hashing on CallbackURL alone (the previous strategy) collapsed every
// callback for a deployment with one registered URL onto a single partition,
// leaving N-1 callback-delivery pods idle while one drowned (F-059). This
// scope-based key spreads the work across partitions while preserving
// per-(scope, URL) ordering downstream consumers rely on.
//
// Choice of scope by message Type:
//   - STUMP / SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES: subtree-scoped, so the
//     key is SubtreeHash (preferred) or BlockHash:SubtreeIndex as a fallback
//     for messages produced before SubtreeHash was added.
//   - BLOCK_PROCESSED: block-scoped — one message per (block, URL).
//   - Anything else (or missing scope): falls back to CallbackURL so the
//     producer never returns an empty key (Kafka requires non-empty for
//     hash partitioning).
func (m *CallbackTopicMessage) PartitionKey() string {
	var scope string
	switch {
	case m.SubtreeHash != "":
		scope = m.SubtreeHash
	case m.Type == CallbackBlockProcessed && m.BlockHash != "":
		scope = m.BlockHash
	case m.BlockHash != "":
		// STUMP messages produced before SubtreeHash was added still have
		// BlockHash + SubtreeIndex, which is sufficient to scatter.
		scope = m.BlockHash + "#" + itoa(m.SubtreeIndex)
	default:
		scope = m.CallbackURL
	}
	if scope == m.CallbackURL {
		return scope
	}
	return scope + "|" + m.CallbackURL
}

// itoa avoids importing strconv just for one int-to-string. Inlined to
// keep messages.go's import surface unchanged.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func (m *SubtreeMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func DecodeSubtreeMessage(data []byte) (*SubtreeMessage, error) {
	var msg SubtreeMessage
	err := json.Unmarshal(data, &msg)
	return &msg, err
}

func (m *BlockMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func DecodeBlockMessage(data []byte) (*BlockMessage, error) {
	var msg BlockMessage
	err := json.Unmarshal(data, &msg)
	return &msg, err
}

// SubtreeWorkMessage represents a subtree processing work item dispatched by the block processor.
// AttemptCount is incremented by subtree-worker when re-publishing the message
// for retry; on reaching BlockConfig.MaxAttempts the message is routed to the
// subtree-work-dlq topic instead of being re-driven again. The original counter
// is decremented exactly once per subtree (on success or DLQ), so retries
// don't cause BLOCK_PROCESSED to fire prematurely.
type SubtreeWorkMessage struct {
	BlockHash    string `json:"blockHash"`
	BlockHeight  uint32 `json:"blockHeight"`
	SubtreeHash  string `json:"subtreeHash"`
	SubtreeIndex int    `json:"subtreeIndex"`
	DataHubURL   string `json:"dataHubUrl"`
	AttemptCount int    `json:"attemptCount,omitempty"`
	// OverrideCallbackURL/Token are propagated from a /reprocess-originated
	// BlockMessage. When set, the subtree worker scopes STUMP delivery to
	// only this URL (and uses this token), and on the last subtree emits
	// BLOCK_PROCESSED only to this URL — the global broadcast registry is
	// not consulted.
	OverrideCallbackURL   string `json:"overrideCallbackUrl,omitempty"`
	OverrideCallbackToken string `json:"overrideCallbackToken,omitempty"`
	// ReprocessNonce is propagated from the originating reprocess BlockMessage so
	// the subtree worker derives the same nonce-scoped subtree-counter key the
	// block-processor used to Init the counter. Empty on live announcements.
	ReprocessNonce string `json:"reprocessNonce,omitempty"`
}

func (m *SubtreeWorkMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func DecodeSubtreeWorkMessage(data []byte) (*SubtreeWorkMessage, error) {
	var msg SubtreeWorkMessage
	err := json.Unmarshal(data, &msg)
	return &msg, err
}

func (m *CallbackTopicMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func DecodeCallbackTopicMessage(data []byte) (*CallbackTopicMessage, error) {
	var msg CallbackTopicMessage
	err := json.Unmarshal(data, &msg)
	return &msg, err
}
