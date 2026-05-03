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
}

// BlockMessage represents a block announcement received from P2P.
type BlockMessage struct {
	Hash       string `json:"hash"`
	Height     uint32 `json:"height"`
	Header     string `json:"header"`
	Coinbase   string `json:"coinbase"`
	DataHubURL string `json:"dataHubUrl"`
	PeerID     string `json:"peerId"`
	ClientName string `json:"clientName"`
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
	SubtreeIndex  int          `json:"subtreeIndex,omitempty"`
	StumpRef      string       `json:"stumpRef,omitempty"`
	RetryCount    int          `json:"retryCount,omitempty"`
	NextRetryAt   time.Time    `json:"nextRetryAt,omitempty"`
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
