package kafka

import (
	"testing"
	"time"
)

func TestSubtreeMessage_EncodeDecode(t *testing.T) {
	msg := &SubtreeMessage{
		Hash:       "subtree-hash-123",
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
		Hash:       "blockhash123",
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
		BlockHash:    "blockhash789",
		BlockHeight:  850000,
		SubtreeHash:  "subtree-hash-456",
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
		BlockHash:    "blockhash123",
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
	if decoded.BlockHash != "blockhash123" {
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
		BlockHash:   "000000000000000003a2d78e5f7c9012",
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
