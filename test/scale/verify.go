//go:build scale

package scale

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

// waitForAllCallbacks blocks until all expected callbacks are received or timeout.
func waitForAllCallbacks(t *testing.T, fleet *CallbackFleet, manifest *Manifest, timeout time.Duration) {
	t.Helper()

	// Expected: each arcade instance gets some number of MINED callbacks (one per subtree
	// that contains its txids) and exactly 1 BLOCK_PROCESSED callback.
	// Total MINED txids across all servers should equal manifest.TotalTxids.
	// Total BLOCK_PROCESSED callbacks should equal len(manifest.ArcadeInstances).
	expectedBP := int64(len(manifest.ArcadeInstances))
	expectedMinedTxids := int64(manifest.TotalTxids)

	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			// Report what we have.
			var gotMinedTxids int64
			var gotBP int64
			for i := 0; i < fleet.Count(); i++ {
				stats := fleet.GetServer(i).Stats()
				gotMinedTxids += int64(stats.TotalTxids)
				gotBP += int64(stats.BlockProcessed)
			}
			t.Fatalf("timeout after %v waiting for callbacks: got %d/%d MINED txids, %d/%d BLOCK_PROCESSED",
				timeout, gotMinedTxids, expectedMinedTxids, gotBP, expectedBP)
			return

		case <-ticker.C:
			var totalMinedTxids int64
			var totalBP int64
			for i := 0; i < fleet.Count(); i++ {
				stats := fleet.GetServer(i).Stats()
				totalMinedTxids += int64(stats.TotalTxids)
				totalBP += int64(stats.BlockProcessed)
			}
			if totalMinedTxids >= expectedMinedTxids && totalBP >= expectedBP {
				t.Logf("all callbacks received: %d MINED txids, %d BLOCK_PROCESSED", totalMinedTxids, totalBP)
				return
			}
		}
	}
}

// verifyMinedCompleteness checks that each arcade instance received exactly its registered txids.
func verifyMinedCompleteness(t *testing.T, fleet *CallbackFleet, manifest *Manifest, txids [][]byte) {
	t.Helper()

	for _, arcade := range manifest.ArcadeInstances {
		server := fleet.GetServer(arcade.Index)
		payloads := server.MinedPayloads()

		// Collect all txids received by this server (one per STUMP callback).
		received := make(map[string]bool)
		for _, p := range payloads {
			if p.TxID != "" {
				received[p.TxID] = true
			}
		}

		// Build expected set from manifest.
		expected := txidSetForArcade(arcade, txids)

		// Check for missing txids.
		missing := 0
		for txid := range expected {
			if !received[txid] {
				missing++
				if missing <= 5 {
					t.Errorf("arcade %d: missing txid %s", arcade.Index, txid)
				}
			}
		}
		if missing > 5 {
			t.Errorf("arcade %d: %d more missing txids (showing first 5)", arcade.Index, missing-5)
		}

		// Check for unexpected txids.
		unexpected := 0
		for txid := range received {
			if !expected[txid] {
				unexpected++
				if unexpected <= 5 {
					t.Errorf("arcade %d: unexpected txid %s", arcade.Index, txid)
				}
			}
		}
		if unexpected > 5 {
			t.Errorf("arcade %d: %d more unexpected txids (showing first 5)", arcade.Index, unexpected-5)
		}

		if len(received) != len(expected) {
			t.Errorf("arcade %d: expected %d txids, got %d", arcade.Index, len(expected), len(received))
		}
	}
}

// verifyMinedNoDuplicates checks that no txid appears more than once per server.
func verifyMinedNoDuplicates(t *testing.T, fleet *CallbackFleet, manifest *Manifest) {
	t.Helper()

	for _, arcade := range manifest.ArcadeInstances {
		server := fleet.GetServer(arcade.Index)
		payloads := server.MinedPayloads()

		seen := make(map[string]int)
		for _, p := range payloads {
			if p.TxID != "" {
				seen[p.TxID]++
			}
		}

		dupes := 0
		for txid, count := range seen {
			if count > 1 {
				dupes++
				if dupes <= 5 {
					t.Errorf("arcade %d: duplicate txid %s (count=%d)", arcade.Index, txid, count)
				}
			}
		}
		if dupes > 5 {
			t.Errorf("arcade %d: %d more duplicate txids", arcade.Index, dupes-5)
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
