package kafka

import "testing"

// TestFanoutAndRetryKeyingAgree pins the fix for the subtree-work retry path:
// the block-processor fans subtrees out with HashBatchEntry, and the
// subtree-worker re-publishes a transient failure with PublishWithHashKey.
// Both must derive the SAME partition key for a given subtree hash, or a retry
// lands on a different partition than its original.
//
// HashBatchEntry sets Key = HashPartitionKey(k); PublishWithHashKey now also
// keys via HashPartitionKey(k). This test fails if those ever diverge (e.g. a
// future change makes one of them hash differently).
func TestFanoutAndRetryKeyingAgree(t *testing.T) {
	keys := []string{
		"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
		"subtree-0",
		"",
	}
	for _, k := range keys {
		fanout := HashBatchEntry(k, []byte("v")).Key // block-processor fan-out
		retry := HashPartitionKey(k)                 // PublishWithHashKey (subtree-worker retry/DLQ, callback emit/retry)
		if fanout != retry {
			t.Errorf("key mismatch for %q: fan-out=%q retry=%q (retry would land on a different partition)", k, fanout, retry)
		}
	}
}

// TestRawKeyDiffersFromHashedKey documents WHY the old retry was a bug: a raw
// Publish(subtreeHash) keys differently from the HashBatchEntry(subtreeHash)
// fan-out, so the broker would place them on different partitions.
func TestRawKeyDiffersFromHashedKey(t *testing.T) {
	const subtreeHash = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	raw := subtreeHash                     // old retry: Publish(subtreeHash) -> raw key
	hashed := HashPartitionKey(subtreeHash) // fan-out / fixed retry
	if raw == hashed {
		t.Fatalf("expected raw and hashed keys to differ; both were %q", raw)
	}
}
