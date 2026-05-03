package kafka

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
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

// hashHexLen is the expected length of a hex-encoded 32-byte block/subtree hash.
const hashHexLen = 64

// ErrInvalidMessage is returned by the Decode... helpers when a Kafka message
// successfully unmarshals as JSON but fails post-decode validation (missing
// required fields, malformed hashes, malformed URLs, unknown callback type,
// negative retry counter). Callers should treat ErrInvalidMessage as a
// poison-pill condition: re-driving the same bytes will never succeed, so the
// consumer should log + ack rather than stalling the partition. Wrap with
// errors.Is to detect.
var ErrInvalidMessage = errors.New("kafka: invalid message")

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

// Validate enforces the post-decode invariants that the JSON layer cannot
// express on its own (non-empty required fields, well-formed hash hex,
// well-formed URL, non-negative attempt counter). All errors are wrapped in
// ErrInvalidMessage so callers can recognize poison pills.
func (m *SubtreeMessage) Validate() error {
	if err := validateHash("hash", m.Hash); err != nil {
		return err
	}
	if err := validateRequiredURL("dataHubUrl", m.DataHubURL); err != nil {
		return err
	}
	if m.AttemptCount < 0 {
		return fmt.Errorf("%w: field %q must be >= 0 (got %d)", ErrInvalidMessage, "attemptCount", m.AttemptCount)
	}
	return nil
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

// Validate enforces the post-decode invariants for a BlockMessage.
func (m *BlockMessage) Validate() error {
	if err := validateHash("hash", m.Hash); err != nil {
		return err
	}
	if err := validateRequiredURL("dataHubUrl", m.DataHubURL); err != nil {
		return err
	}
	return nil
}

// CallbackTopicMessage is the message published to the callback Kafka topic.
// It wraps the Arcade CallbackMessage fields plus delivery metadata.
type CallbackTopicMessage struct {
	CallbackURL  string       `json:"callbackUrl"`
	Type         CallbackType `json:"type"`
	TxID         string       `json:"txid,omitempty"`
	TxIDs        []string     `json:"txids,omitempty"`
	BlockHash    string       `json:"blockHash,omitempty"`
	SubtreeIndex int          `json:"subtreeIndex,omitempty"`
	StumpRef     string       `json:"stumpRef,omitempty"`
	RetryCount   int          `json:"retryCount,omitempty"`
	NextRetryAt  time.Time    `json:"nextRetryAt,omitempty"`
}

// Validate enforces the post-decode invariants for a CallbackTopicMessage.
// The required fields differ per callback Type (e.g. STUMP carries a blockHash
// and stumpRef, BLOCK_PROCESSED carries only a blockHash, SEEN_* carries
// either a single TxID or a TxIDs batch). Unknown types are rejected outright.
func (m *CallbackTopicMessage) Validate() error {
	if err := validateRequiredURL("callbackUrl", m.CallbackURL); err != nil {
		return err
	}
	if !isKnownCallbackType(m.Type) {
		return fmt.Errorf("%w: field %q has unknown value %q", ErrInvalidMessage, "type", m.Type)
	}
	if m.RetryCount < 0 {
		return fmt.Errorf("%w: field %q must be >= 0 (got %d)", ErrInvalidMessage, "retryCount", m.RetryCount)
	}

	switch m.Type {
	case CallbackSeenOnNetwork, CallbackSeenMultipleNodes:
		// Either a single TxID or a non-empty TxIDs batch must be present.
		if m.TxID == "" && len(m.TxIDs) == 0 {
			return fmt.Errorf("%w: field %q requires either %q or non-empty %q",
				ErrInvalidMessage, "type", "txid", "txids")
		}
	case CallbackStump:
		if err := validateHash("blockHash", m.BlockHash); err != nil {
			return err
		}
		if m.StumpRef == "" {
			return fmt.Errorf("%w: field %q is required for STUMP callback", ErrInvalidMessage, "stumpRef")
		}
	case CallbackBlockProcessed:
		if err := validateHash("blockHash", m.BlockHash); err != nil {
			return err
		}
	}
	return nil
}

func (m *SubtreeMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeSubtreeMessage parses and validates a subtree announcement. A wrapped
// ErrInvalidMessage indicates a poison-pill (consumers should log and ack);
// any other error is a JSON parse failure (also poison-pill in practice).
func DecodeSubtreeMessage(data []byte) (*SubtreeMessage, error) {
	var msg SubtreeMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (m *BlockMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeBlockMessage parses and validates a block announcement.
func DecodeBlockMessage(data []byte) (*BlockMessage, error) {
	var msg BlockMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return &msg, nil
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

// Validate enforces the post-decode invariants for a SubtreeWorkMessage.
func (m *SubtreeWorkMessage) Validate() error {
	if err := validateHash("blockHash", m.BlockHash); err != nil {
		return err
	}
	if err := validateHash("subtreeHash", m.SubtreeHash); err != nil {
		return err
	}
	if err := validateRequiredURL("dataHubUrl", m.DataHubURL); err != nil {
		return err
	}
	if m.SubtreeIndex < 0 {
		return fmt.Errorf("%w: field %q must be >= 0 (got %d)", ErrInvalidMessage, "subtreeIndex", m.SubtreeIndex)
	}
	if m.AttemptCount < 0 {
		return fmt.Errorf("%w: field %q must be >= 0 (got %d)", ErrInvalidMessage, "attemptCount", m.AttemptCount)
	}
	return nil
}

func (m *SubtreeWorkMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeSubtreeWorkMessage parses and validates a subtree work item.
func DecodeSubtreeWorkMessage(data []byte) (*SubtreeWorkMessage, error) {
	var msg SubtreeWorkMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (m *CallbackTopicMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeCallbackTopicMessage parses and validates a callback message.
func DecodeCallbackTopicMessage(data []byte) (*CallbackTopicMessage, error) {
	var msg CallbackTopicMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return &msg, nil
}

// validateHash enforces non-empty + 64-char lowercase/uppercase hex (i.e. a
// 32-byte hash, matching chainhash.HashSize). Returns an ErrInvalidMessage
// wrapped error.
func validateHash(field, value string) error {
	if value == "" {
		return fmt.Errorf("%w: field %q is required", ErrInvalidMessage, field)
	}
	if len(value) != hashHexLen {
		return fmt.Errorf("%w: field %q must be %d hex chars (got %d)",
			ErrInvalidMessage, field, hashHexLen, len(value))
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%w: field %q is not valid hex: %v", ErrInvalidMessage, field, err)
	}
	return nil
}

// validateRequiredURL enforces non-empty + parseable as a request URI with an
// http(s) scheme — peer-supplied URLs that don't parse cannot be safely fed to
// HTTP clients downstream. SSRF-class checks (private IPs, etc.) live in the
// dialer; this function only validates well-formedness.
func validateRequiredURL(field, value string) error {
	if value == "" {
		return fmt.Errorf("%w: field %q is required", ErrInvalidMessage, field)
	}
	u, err := url.ParseRequestURI(value)
	if err != nil {
		return fmt.Errorf("%w: field %q is not a valid URL: %v", ErrInvalidMessage, field, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: field %q must be http or https (got %q)",
			ErrInvalidMessage, field, u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: field %q must include a host", ErrInvalidMessage, field)
	}
	return nil
}

func isKnownCallbackType(t CallbackType) bool {
	switch t {
	case CallbackSeenOnNetwork, CallbackSeenMultipleNodes, CallbackStump, CallbackBlockProcessed:
		return true
	default:
		return false
	}
}
