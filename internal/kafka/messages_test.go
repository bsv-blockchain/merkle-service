package kafka

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// Stable, valid 64-char hex hashes used across tests.
const (
	validHash1 = "0000000000000000000000000000000000000000000000000000000000000001"
	validHash2 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	validHash3 = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
)

func TestSubtreeMessage_EncodeDecode(t *testing.T) {
	msg := &SubtreeMessage{
		Hash:       validHash1,
		DataHubURL: "https://datahub.example.com/subtree/123",
		PeerID:     "peer1",
		ClientName: "teranode-v1",
	}

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeSubtreeMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Hash != msg.Hash {
		t.Errorf("hash mismatch")
	}
	if decoded.DataHubURL != msg.DataHubURL {
		t.Errorf("dataHubUrl mismatch")
	}
	if decoded.PeerID != msg.PeerID {
		t.Errorf("peerId mismatch")
	}
	if decoded.ClientName != msg.ClientName {
		t.Errorf("clientName mismatch")
	}
}

func TestBlockMessage_EncodeDecode(t *testing.T) {
	msg := &BlockMessage{
		Hash:       validHash2,
		Height:     200,
		Header:     "0100000000000000",
		Coinbase:   "01000000010000",
		DataHubURL: "https://datahub.example.com/block/123",
		PeerID:     "peer2",
		ClientName: "teranode-v1",
	}

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeBlockMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Hash != msg.Hash {
		t.Errorf("hash mismatch")
	}
	if decoded.Height != 200 {
		t.Errorf("height mismatch")
	}
	if decoded.Header != msg.Header {
		t.Errorf("header mismatch")
	}
	if decoded.Coinbase != msg.Coinbase {
		t.Errorf("coinbase mismatch")
	}
	if decoded.DataHubURL != msg.DataHubURL {
		t.Errorf("dataHubUrl mismatch")
	}
}

func TestSubtreeWorkMessage_EncodeDecode(t *testing.T) {
	msg := &SubtreeWorkMessage{
		BlockHash:    validHash2,
		BlockHeight:  850000,
		SubtreeHash:  validHash3,
		SubtreeIndex: 2,
		DataHubURL:   "https://datahub.example.com/subtree/456",
	}

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeSubtreeWorkMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.BlockHash != msg.BlockHash {
		t.Errorf("blockHash mismatch: got %s", decoded.BlockHash)
	}
	if decoded.BlockHeight != msg.BlockHeight {
		t.Errorf("blockHeight mismatch: got %d", decoded.BlockHeight)
	}
	if decoded.SubtreeHash != msg.SubtreeHash {
		t.Errorf("subtreeHash mismatch: got %s", decoded.SubtreeHash)
	}
	if decoded.SubtreeIndex != 2 {
		t.Errorf("subtreeIndex mismatch: got %d", decoded.SubtreeIndex)
	}
	if decoded.DataHubURL != msg.DataHubURL {
		t.Errorf("dataHubUrl mismatch: got %s", decoded.DataHubURL)
	}
}

func TestCallbackTopicMessage_SeenOnNetwork(t *testing.T) {
	msg := &CallbackTopicMessage{
		CallbackURL: "https://example.com/cb",
		Type:        CallbackSeenOnNetwork,
		TxID:        "txid1",
		RetryCount:  2,
		NextRetryAt: time.Now().Add(30 * time.Second).Truncate(time.Millisecond),
	}

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeCallbackTopicMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.CallbackURL != msg.CallbackURL {
		t.Errorf("callback URL mismatch")
	}
	if decoded.Type != CallbackSeenOnNetwork {
		t.Errorf("type mismatch: got %s", decoded.Type)
	}
	if decoded.RetryCount != 2 {
		t.Errorf("retry count mismatch")
	}
}

func TestCallbackTopicMessage_Stump(t *testing.T) {
	stumpRef := "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	msg := &CallbackTopicMessage{
		CallbackURL:  "https://example.com/cb",
		Type:         CallbackStump,
		TxID:         "txid1",
		BlockHash:    validHash1,
		SubtreeIndex: 5,
		StumpRef:     stumpRef,
	}

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeCallbackTopicMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type != CallbackStump {
		t.Errorf("expected STUMP, got %s", decoded.Type)
	}
	if decoded.TxID != "txid1" {
		t.Errorf("txid mismatch: got %s", decoded.TxID)
	}
	if decoded.BlockHash != validHash1 {
		t.Errorf("blockHash mismatch: got %s", decoded.BlockHash)
	}
	if decoded.SubtreeIndex != 5 {
		t.Errorf("subtreeIndex mismatch: got %d", decoded.SubtreeIndex)
	}
	if decoded.StumpRef != stumpRef {
		t.Errorf("stumpRef mismatch: got %s", decoded.StumpRef)
	}
	// Verify the old "stump" JSON field is gone — ensures Kafka messages are
	// small (claim-check) rather than inlining the STUMP.
	if bytesContains(data, []byte(`"stump":`)) {
		t.Errorf("encoded message still contains raw stump field: %s", string(data))
	}
}

func bytesContains(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func TestCallbackTopicMessage_BatchedSeenOnNetwork(t *testing.T) {
	msg := &CallbackTopicMessage{
		CallbackURL: "https://example.com/cb",
		Type:        CallbackSeenOnNetwork,
		TxIDs:       []string{"txid1", "txid2", "txid3"},
	}

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeCallbackTopicMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type != CallbackSeenOnNetwork {
		t.Errorf("type mismatch: got %s", decoded.Type)
	}
	if len(decoded.TxIDs) != 3 {
		t.Fatalf("expected 3 TxIDs, got %d", len(decoded.TxIDs))
	}
	for i, expected := range []string{"txid1", "txid2", "txid3"} {
		if decoded.TxIDs[i] != expected {
			t.Errorf("TxIDs[%d]: expected %s, got %s", i, expected, decoded.TxIDs[i])
		}
	}
	if decoded.TxID != "" {
		t.Errorf("expected empty TxID for batched message, got %s", decoded.TxID)
	}
}

func TestCallbackTopicMessage_BlockProcessed(t *testing.T) {
	msg := &CallbackTopicMessage{
		CallbackURL: "https://arcade.example.com/callback",
		Type:        CallbackBlockProcessed,
		BlockHash:   validHash2,
	}

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeCallbackTopicMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Type != CallbackBlockProcessed {
		t.Errorf("expected BLOCK_PROCESSED, got %s", decoded.Type)
	}
	if decoded.BlockHash != msg.BlockHash {
		t.Errorf("blockHash mismatch: got %s", decoded.BlockHash)
	}
	if decoded.CallbackURL != msg.CallbackURL {
		t.Errorf("callbackURL mismatch: got %s", decoded.CallbackURL)
	}
	if decoded.TxID != "" {
		t.Errorf("expected empty txid, got %s", decoded.TxID)
	}
	if decoded.StumpRef != "" {
		t.Errorf("expected empty stumpRef, got %v", decoded.StumpRef)
	}
}

// --- Validation (F-032) -------------------------------------------------

// TestDecodeSubtreeMessage_Validation covers all the SubtreeMessage rejection
// paths so a poison-pill message can never flow through the decoder.
func TestDecodeSubtreeMessage_Validation(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		errField string
	}{
		{
			name:     "empty hash",
			raw:      `{"hash":"","dataHubUrl":"https://datahub.example.com/x"}`,
			errField: "hash",
		},
		{
			name:     "non-hex hash",
			raw:      `{"hash":"zzzz000000000000000000000000000000000000000000000000000000000000","dataHubUrl":"https://datahub.example.com/x"}`,
			errField: "hash",
		},
		{
			name:     "short hash",
			raw:      `{"hash":"deadbeef","dataHubUrl":"https://datahub.example.com/x"}`,
			errField: "hash",
		},
		{
			name:     "empty dataHubUrl",
			raw:      `{"hash":"` + validHash1 + `","dataHubUrl":""}`,
			errField: "dataHubUrl",
		},
		{
			name:     "non-http dataHubUrl",
			raw:      `{"hash":"` + validHash1 + `","dataHubUrl":"ftp://example.com/x"}`,
			errField: "dataHubUrl",
		},
		{
			name:     "garbage dataHubUrl",
			raw:      `{"hash":"` + validHash1 + `","dataHubUrl":"::not a url::"}`,
			errField: "dataHubUrl",
		},
		{
			name:     "negative attemptCount",
			raw:      `{"hash":"` + validHash1 + `","dataHubUrl":"https://example.com/x","attemptCount":-1}`,
			errField: "attemptCount",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeSubtreeMessage([]byte(tc.raw))
			assertInvalidMessageContains(t, err, tc.errField)
		})
	}
}

func TestDecodeBlockMessage_Validation(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		errField string
	}{
		{
			name:     "empty hash",
			raw:      `{"hash":"","dataHubUrl":"https://example.com/x"}`,
			errField: "hash",
		},
		{
			name:     "bad hex hash",
			raw:      `{"hash":"not-a-hash","dataHubUrl":"https://example.com/x"}`,
			errField: "hash",
		},
		{
			name:     "empty dataHubUrl",
			raw:      `{"hash":"` + validHash2 + `","dataHubUrl":""}`,
			errField: "dataHubUrl",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeBlockMessage([]byte(tc.raw))
			assertInvalidMessageContains(t, err, tc.errField)
		})
	}
}

func TestDecodeSubtreeWorkMessage_Validation(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		errField string
	}{
		{
			name:     "empty blockHash",
			raw:      `{"blockHash":"","subtreeHash":"` + validHash1 + `","dataHubUrl":"https://example.com/x"}`,
			errField: "blockHash",
		},
		{
			name:     "empty subtreeHash",
			raw:      `{"blockHash":"` + validHash2 + `","subtreeHash":"","dataHubUrl":"https://example.com/x"}`,
			errField: "subtreeHash",
		},
		{
			name:     "bad subtreeHash hex",
			raw:      `{"blockHash":"` + validHash2 + `","subtreeHash":"not-hex-not-hex-not-hex-not-hex-not-hex-not-hex-not-hex-not-hexx","dataHubUrl":"https://example.com/x"}`,
			errField: "subtreeHash",
		},
		{
			name:     "empty dataHubUrl",
			raw:      `{"blockHash":"` + validHash2 + `","subtreeHash":"` + validHash1 + `","dataHubUrl":""}`,
			errField: "dataHubUrl",
		},
		{
			name:     "negative subtreeIndex",
			raw:      `{"blockHash":"` + validHash2 + `","subtreeHash":"` + validHash1 + `","dataHubUrl":"https://example.com/x","subtreeIndex":-1}`,
			errField: "subtreeIndex",
		},
		{
			name:     "negative attemptCount",
			raw:      `{"blockHash":"` + validHash2 + `","subtreeHash":"` + validHash1 + `","dataHubUrl":"https://example.com/x","attemptCount":-5}`,
			errField: "attemptCount",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeSubtreeWorkMessage([]byte(tc.raw))
			assertInvalidMessageContains(t, err, tc.errField)
		})
	}
}

func TestDecodeCallbackTopicMessage_Validation(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		errField string
	}{
		{
			name:     "empty callbackUrl",
			raw:      `{"callbackUrl":"","type":"SEEN_ON_NETWORK","txid":"t1"}`,
			errField: "callbackUrl",
		},
		{
			name:     "non-http callbackUrl",
			raw:      `{"callbackUrl":"javascript:alert(1)","type":"SEEN_ON_NETWORK","txid":"t1"}`,
			errField: "callbackUrl",
		},
		{
			name:     "unknown type",
			raw:      `{"callbackUrl":"https://example.com/cb","type":"NOT_A_REAL_TYPE","txid":"t1"}`,
			errField: "type",
		},
		{
			name:     "empty type",
			raw:      `{"callbackUrl":"https://example.com/cb","type":"","txid":"t1"}`,
			errField: "type",
		},
		{
			name:     "negative retryCount",
			raw:      `{"callbackUrl":"https://example.com/cb","type":"SEEN_ON_NETWORK","txid":"t1","retryCount":-1}`,
			errField: "retryCount",
		},
		{
			name:     "SEEN without txid or txids",
			raw:      `{"callbackUrl":"https://example.com/cb","type":"SEEN_ON_NETWORK"}`,
			errField: "type",
		},
		{
			name:     "STUMP missing blockHash",
			raw:      `{"callbackUrl":"https://example.com/cb","type":"STUMP","stumpRef":"abc"}`,
			errField: "blockHash",
		},
		{
			name:     "STUMP missing stumpRef",
			raw:      `{"callbackUrl":"https://example.com/cb","type":"STUMP","blockHash":"` + validHash1 + `"}`,
			errField: "stumpRef",
		},
		{
			name:     "STUMP malformed blockHash",
			raw:      `{"callbackUrl":"https://example.com/cb","type":"STUMP","blockHash":"deadbeef","stumpRef":"abc"}`,
			errField: "blockHash",
		},
		{
			name:     "BLOCK_PROCESSED missing blockHash",
			raw:      `{"callbackUrl":"https://example.com/cb","type":"BLOCK_PROCESSED"}`,
			errField: "blockHash",
		},
		{
			name:     "BLOCK_PROCESSED malformed blockHash",
			raw:      `{"callbackUrl":"https://example.com/cb","type":"BLOCK_PROCESSED","blockHash":"deadbeef"}`,
			errField: "blockHash",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCallbackTopicMessage([]byte(tc.raw))
			assertInvalidMessageContains(t, err, tc.errField)
		})
	}
}

// TestDecodeCallbackTopicMessage_AcceptsBatchedSeen guards against regressing
// to a stricter validator that would reject the batched (TxIDs-only) form
// callers actually emit.
func TestDecodeCallbackTopicMessage_AcceptsBatchedSeen(t *testing.T) {
	raw := `{"callbackUrl":"https://example.com/cb","type":"SEEN_ON_NETWORK","txids":["t1","t2"]}`
	if _, err := DecodeCallbackTopicMessage([]byte(raw)); err != nil {
		t.Fatalf("expected batched SEEN to validate, got %v", err)
	}
}

// TestDecodeReturnsErrInvalidMessage verifies callers can switch on the
// sentinel via errors.Is so they can distinguish poison-pill from JSON-parse
// failures (both are poison in practice; the consumer paths log+ack either
// way).
func TestDecodeReturnsErrInvalidMessage(t *testing.T) {
	_, err := DecodeSubtreeMessage([]byte(`{"hash":"","dataHubUrl":""}`))
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("expected error to wrap ErrInvalidMessage, got %v", err)
	}
}

// TestDecodeRejectsMalformedJSON confirms json.Unmarshal errors also surface
// as a non-nil error from the decoder (the existing behavior must be
// preserved).
func TestDecodeRejectsMalformedJSON(t *testing.T) {
	if _, err := DecodeSubtreeMessage([]byte(`not-json`)); err == nil {
		t.Fatal("expected JSON unmarshal error, got nil")
	}
	if _, err := DecodeBlockMessage([]byte(`{`)); err == nil {
		t.Fatal("expected JSON unmarshal error, got nil")
	}
	if _, err := DecodeSubtreeWorkMessage([]byte(`[]`)); err == nil {
		t.Fatal("expected JSON unmarshal error, got nil")
	}
	if _, err := DecodeCallbackTopicMessage([]byte(`{"type":}`)); err == nil {
		t.Fatal("expected JSON unmarshal error, got nil")
	}
}

func assertInvalidMessageContains(t *testing.T, err error, field string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected validation error mentioning %q, got nil", field)
	}
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("expected error to wrap ErrInvalidMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), field) {
		t.Errorf("expected error to mention field %q, got %v", field, err)
	}
}
