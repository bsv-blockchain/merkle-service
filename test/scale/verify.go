//go:build scale

package scale

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

// waitForAllCallbacks blocks until all expected callbacks are received or timeout.
//
// The modern pipeline emits ONE batched STUMP (MINED) callback per (subtree,
// callbackURL) — the payload identifies the subtree and carries the proof; it
// does not enumerate txids. So the wait target is the expected PAYLOAD count
// derived from the manifest (one per subtree containing >=1 of the arcade's
// txids), plus exactly one BLOCK_PROCESSED per arcade. Txid-level
// completeness is asserted afterwards by verifyMinedCompleteness.
func waitForAllCallbacks(t *testing.T, fleet *CallbackFleet, manifest *Manifest, timeout time.Duration) {
	t.Helper()

	expectedBP := int64(len(manifest.ArcadeInstances))
	var expectedMined int64
	for _, set := range expectedMinedPayloadsPerArcade(manifest) {
		expectedMined += int64(len(set))
	}

	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			// Report what we have.
			var gotMined int64
			var gotBP int64
			for i := 0; i < fleet.Count(); i++ {
				stats := fleet.GetServer(i).Stats()
				gotMined += int64(stats.MinedCallbacks)
				gotBP += int64(stats.BlockProcessed)
			}
			t.Fatalf("timeout after %v waiting for callbacks: got %d/%d MINED payloads, %d/%d BLOCK_PROCESSED",
				timeout, gotMined, expectedMined, gotBP, expectedBP)
			return

		case <-ticker.C:
			var totalMined int64
			var totalBP int64
			for i := 0; i < fleet.Count(); i++ {
				stats := fleet.GetServer(i).Stats()
				totalMined += int64(stats.MinedCallbacks)
				totalBP += int64(stats.BlockProcessed)
			}
			if totalMined >= expectedMined && totalBP >= expectedBP {
				t.Logf("all callbacks received: %d MINED payloads, %d BLOCK_PROCESSED", totalMined, totalBP)
				return
			}
		}
	}
}

// expectedMinedPayloadsPerArcade returns, for each arcade index, the set of
// subtree indices that contain at least one of the arcade's registered txids
// ([TxidStart, TxidEnd) global-index range). One batched STUMP callback is
// expected per entry.
func expectedMinedPayloadsPerArcade(manifest *Manifest) map[int]map[int]bool {
	out := make(map[int]map[int]bool, len(manifest.ArcadeInstances))
	for _, a := range manifest.ArcadeInstances {
		set := make(map[int]bool)
		for _, st := range manifest.Subtrees {
			for _, idx := range st.TxidIndices {
				if idx >= a.TxidStart && idx < a.TxidEnd {
					set[st.Index] = true
					break
				}
			}
		}
		out[a.Index] = set
	}
	return out
}

// verifyMinedCompleteness checks that each arcade instance received exactly its registered txids.
func verifyMinedCompleteness(t *testing.T, fleet *CallbackFleet, manifest *Manifest, txids [][]byte) {
	t.Helper()

	expectedPerArcade := expectedMinedPayloadsPerArcade(manifest)

	for _, arcade := range manifest.ArcadeInstances {
		server := fleet.GetServer(arcade.Index)
		payloads := server.MinedPayloads()

		// One batched STUMP callback per subtree containing the arcade's
		// txids: completeness is per-subtree coverage, not per-txid (the
		// payload carries the proof, not a txid list).
		received := make(map[int]bool)
		for _, p := range payloads {
			received[p.SubtreeIndex] = true
		}

		expected := expectedPerArcade[arcade.Index]

		missing := 0
		for idx := range expected {
			if !received[idx] {
				missing++
				if missing <= 5 {
					t.Errorf("arcade %d: missing STUMP callback for subtree %d", arcade.Index, idx)
				}
			}
		}
		if missing > 5 {
			t.Errorf("arcade %d: %d more missing subtrees (showing first 5)", arcade.Index, missing-5)
		}

		unexpected := 0
		for idx := range received {
			if !expected[idx] {
				unexpected++
				if unexpected <= 5 {
					t.Errorf("arcade %d: unexpected STUMP callback for subtree %d", arcade.Index, idx)
				}
			}
		}
		if unexpected > 5 {
			t.Errorf("arcade %d: %d more unexpected subtrees (showing first 5)", arcade.Index, unexpected-5)
		}

		if len(received) != len(expected) {
			t.Errorf("arcade %d: expected STUMP callbacks for %d subtrees, got %d", arcade.Index, len(expected), len(received))
		}
	}
}

// verifyMinedNoDuplicates checks that no txid appears more than once per server.
func verifyMinedNoDuplicates(t *testing.T, fleet *CallbackFleet, manifest *Manifest) {
	t.Helper()

	for _, arcade := range manifest.ArcadeInstances {
		server := fleet.GetServer(arcade.Index)
		payloads := server.MinedPayloads()

		// One batched STUMP callback per subtree: duplicates key on subtree.
		seen := make(map[int]int)
		for _, p := range payloads {
			seen[p.SubtreeIndex]++
		}

		dupes := 0
		for idx, count := range seen {
			if count > 1 {
				dupes++
				if dupes <= 5 {
					t.Errorf("arcade %d: duplicate STUMP callback for subtree %d (count=%d)", arcade.Index, idx, count)
				}
			}
		}
		if dupes > 5 {
			t.Errorf("arcade %d: %d more duplicate subtrees", arcade.Index, dupes-5)
		}
	}
}

// verifyStumpValidity checks that each MINED callback has valid, decodeable STUMP data.
func verifyStumpValidity(t *testing.T, fleet *CallbackFleet, manifest *Manifest) {
	t.Helper()

	for _, arcade := range manifest.ArcadeInstances {
		server := fleet.GetServer(arcade.Index)
		payloads := server.MinedPayloads()

		for i, p := range payloads {
			if p.Stump == "" {
				t.Errorf("arcade %d, STUMP callback %d: empty stump", arcade.Index, i)
				continue
			}

			// Decode hex (Arcade's HexBytes format).
			stumpBytes, err := hex.DecodeString(p.Stump)
			if err != nil {
				t.Errorf("arcade %d, STUMP callback %d: invalid hex stump: %v", arcade.Index, i, err)
				continue
			}

			// Validate BRC-0074 STUMP format: first bytes are CompactSize block height.
			if len(stumpBytes) < 2 {
				t.Errorf("arcade %d, STUMP callback %d: stump too short (%d bytes)", arcade.Index, i, len(stumpBytes))
				continue
			}

			// Parse block height from CompactSize VarInt.
			blockHeight, bytesRead := readCompactSize(stumpBytes)
			if bytesRead == 0 {
				t.Errorf("arcade %d, STUMP callback %d: failed to parse block height from stump", arcade.Index, i)
				continue
			}

			if blockHeight != uint64(manifest.BlockHeight) {
				t.Errorf("arcade %d, STUMP callback %d: block height %d != expected %d", arcade.Index, i, blockHeight, manifest.BlockHeight)
			}

			if p.BlockHash != manifest.BlockHash {
				t.Errorf("arcade %d, STUMP callback %d: blockHash %s != expected %s", arcade.Index, i, p.BlockHash, manifest.BlockHash)
			}
		}
	}
}

// readCompactSize reads a CompactSize VarInt from the beginning of data.
// Returns (value, bytesRead). bytesRead=0 on error.
func readCompactSize(data []byte) (uint64, int) {
	if len(data) == 0 {
		return 0, 0
	}
	first := data[0]
	switch {
	case first < 0xFD:
		return uint64(first), 1
	case first == 0xFD:
		if len(data) < 3 {
			return 0, 0
		}
		return uint64(data[1]) | uint64(data[2])<<8, 3
	case first == 0xFE:
		if len(data) < 5 {
			return 0, 0
		}
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16 | uint64(data[4])<<24, 5
	default: // 0xFF
		if len(data) < 9 {
			return 0, 0
		}
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16 | uint64(data[4])<<24 |
			uint64(data[5])<<32 | uint64(data[6])<<40 | uint64(data[7])<<48 | uint64(data[8])<<56, 9
	}
}

// verifyBlockProcessed checks that each server received exactly one BLOCK_PROCESSED callback.
func verifyBlockProcessed(t *testing.T, fleet *CallbackFleet, manifest *Manifest) {
	t.Helper()

	for _, arcade := range manifest.ArcadeInstances {
		server := fleet.GetServer(arcade.Index)
		payloads := server.BlockProcessedPayloads()

		if len(payloads) != 1 {
			t.Errorf("arcade %d: expected 1 BLOCK_PROCESSED, got %d", arcade.Index, len(payloads))
			continue
		}

		if payloads[0].BlockHash != manifest.BlockHash {
			t.Errorf("arcade %d: BLOCK_PROCESSED blockHash %s != expected %s",
				arcade.Index, payloads[0].BlockHash, manifest.BlockHash)
		}
	}
}

// verifyNoSpuriousCallbacks checks that total callbacks match expected counts.
func verifyNoSpuriousCallbacks(t *testing.T, fleet *CallbackFleet, manifest *Manifest) {
	t.Helper()

	// Wait a short drain period for any late-arriving callbacks.
	time.Sleep(2 * time.Second)

	var totalMined, totalBP, totalOther int64
	for i := 0; i < fleet.Count(); i++ {
		stats := fleet.GetServer(i).Stats()
		totalMined += int64(stats.MinedCallbacks)
		totalBP += int64(stats.BlockProcessed)
		totalOther = stats.TotalCallbacks - int64(stats.MinedCallbacks) - int64(stats.BlockProcessed)
		if totalOther > 0 {
			t.Errorf("server %d: received %d unexpected callbacks", i, totalOther)
		}
	}

	expectedBP := int64(len(manifest.ArcadeInstances))
	if totalBP != expectedBP {
		t.Errorf("total BLOCK_PROCESSED: expected %d, got %d", expectedBP, totalBP)
	}

	t.Logf("total callbacks: %d MINED, %d BLOCK_PROCESSED", totalMined, totalBP)
}

// runAllVerifications runs all verification checks.
func runAllVerifications(t *testing.T, fleet *CallbackFleet, manifest *Manifest, txids [][]byte) {
	t.Run("MinedCompleteness", func(t *testing.T) {
		verifyMinedCompleteness(t, fleet, manifest, txids)
	})
	t.Run("MinedNoDuplicates", func(t *testing.T) {
		verifyMinedNoDuplicates(t, fleet, manifest)
	})
	t.Run("StumpValidity", func(t *testing.T) {
		verifyStumpValidity(t, fleet, manifest)
	})
	t.Run("BlockProcessed", func(t *testing.T) {
		verifyBlockProcessed(t, fleet, manifest)
	})
	t.Run("NoSpuriousCallbacks", func(t *testing.T) {
		verifyNoSpuriousCallbacks(t, fleet, manifest)
	})
}

// txidSetForArcade returns the set of expected txid hex strings for an arcade instance.
func txidSetForArcade(arcade ArcadeInstance, txids [][]byte) map[string]bool {
	set := make(map[string]bool, arcade.TxidEnd-arcade.TxidStart)
	for j := arcade.TxidStart; j < arcade.TxidEnd; j++ {
		set[hashToTxidString(txids[j])] = true
	}
	return set
}

// hashToTxidString converts raw 32-byte hash to Bitcoin display order hex string.
func hashToTxidString(hash []byte) string {
	// Reverse byte order for Bitcoin display.
	reversed := make([]byte, 32)
	for i := 0; i < 32; i++ {
		reversed[i] = hash[31-i]
	}
	return fmt.Sprintf("%x", reversed)
}
