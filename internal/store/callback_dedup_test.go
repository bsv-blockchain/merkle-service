package store

import (
	"testing"
)

func TestDedupKey_Deterministic(t *testing.T) {
	k1 := dedupKey("txid1", "http://example.com/cb", "SEEN_ON_NETWORK")
	k2 := dedupKey("txid1", "http://example.com/cb", "SEEN_ON_NETWORK")

	if k1 != k2 {
		t.Errorf("same inputs should produce same key: %s != %s", k1, k2)
	}
}

func TestDedupKey_UniquePerStatus(t *testing.T) {
	k1 := dedupKey("txid1", "http://example.com/cb", "SEEN_ON_NETWORK")
	k2 := dedupKey("txid1", "http://example.com/cb", "MINED")

	if k1 == k2 {
		t.Error("different status types should produce different keys")
	}
}

func TestDedupKey_UniquePerCallback(t *testing.T) {
	k1 := dedupKey("txid1", "http://a.com/cb", "SEEN_ON_NETWORK")
	k2 := dedupKey("txid1", "http://b.com/cb", "SEEN_ON_NETWORK")

	if k1 == k2 {
		t.Error("different callback URLs should produce different keys")
	}
}

func TestDedupKey_UniquePerTxid(t *testing.T) {
	k1 := dedupKey("txid1", "http://example.com/cb", "SEEN_ON_NETWORK")
	k2 := dedupKey("txid2", "http://example.com/cb", "SEEN_ON_NETWORK")

	if k1 == k2 {
		t.Error("different txids should produce different keys")
	}
}

func TestDedupKey_FixedLength(t *testing.T) {
	k := dedupKey("txid1", "http://example.com/very/long/callback/url/path?param=value&other=thing", "SEEN_ON_NETWORK")
	// SHA-256 hex = 64 chars
	if len(k) != 64 {
		t.Errorf("expected 64-char hex key, got %d", len(k))
	}
}
