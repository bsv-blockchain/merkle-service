package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// generateAll produces all fixture files.
func generateAll(seed int64, instances, txidsPerInstance, subtreeCount, txidsPerSubtree int, outDir string) error {
	totalTxids := instances * txidsPerInstance

	// Step 1: Generate deterministic txid hashes.
	txids := generateTxids(seed, totalTxids)
	if err := writeTxids(filepath.Join(outDir, "txids.bin"), txids); err != nil {
		return fmt.Errorf("writing txids: %w", err)
	}
	fmt.Printf("  Wrote %d txids (%d bytes)\n", len(txids), len(txids)*32)

	// Step 2: Assign txids to subtrees and write subtree files.
	subtreeHashes, err := writeSubtrees(outDir, txids, subtreeCount, txidsPerSubtree)
	if err != nil {
		return fmt.Errorf("writing subtrees: %w", err)
	}
	fmt.Printf("  Wrote %d subtree files\n", len(subtreeHashes))

	// Step 3: Generate and write manifest.
	manifest := buildManifest(seed, instances, txidsPerInstance, subtreeCount, txidsPerSubtree, subtreeHashes)
	if err := writeManifest(filepath.Join(outDir, "manifest.json"), manifest); err != nil {
		return fmt.Errorf("writing manifest: %w", err)
	}
	fmt.Println("  Wrote manifest.json")

	// Step 4: Verify fixture integrity.
	if err := verifyFixtures(manifest, txids, txidsPerSubtree); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}
	fmt.Println("  Verification passed")

	return nil
}

// generateTxids produces n deterministic 32-byte hashes from SHA256(seed || index).
func generateTxids(seed int64, n int) [][]byte {
	txids := make([][]byte, n)
	var input [16]byte
	binary.BigEndian.PutUint64(input[:8], uint64(seed)) //nolint:gosec // seed is internal, not user-controlled
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(input[8:], uint64(i))
		hash := sha256.Sum256(input[:])
		txids[i] = hash[:]
	}
	return txids
}

// writeTxids writes concatenated 32-byte hashes to a file.
func writeTxids(path string, txids [][]byte) error {
	data := make([]byte, 0, len(txids)*32)
	for _, txid := range txids {
		data = append(data, txid...)
	}
	return os.WriteFile(path, data, 0o644) //nolint:gosec // 0644 intentional for shared fixture files
}

// writeSubtrees assigns txids to subtrees and writes each subtree file.
// Returns the SHA256 hash of each subtree's raw data (used as subtree ID).
func writeSubtrees(outDir string, txids [][]byte, subtreeCount, txidsPerSubtree int) ([]string, error) {
	subtreeDir := filepath.Join(outDir, "subtrees")
	if err := os.MkdirAll(subtreeDir, 0o755); err != nil { //nolint:gosec // 0755 intentional for shared fixture dirs
		return nil, err
	}

	// Group txids by subtree: txid at global index j goes to subtree j % subtreeCount.
	subtreeGroups := make([][]int, subtreeCount)
	for j := range txids {
		st := j % subtreeCount
		subtreeGroups[st] = append(subtreeGroups[st], j)
	}

	// Use 3-digit format for subtree filenames when there are more than 99 subtrees.
	fmtStr := "%02d.bin"
	if subtreeCount > 99 {
		fmtStr = "%03d.bin"
	}

	hashes := make([]string, subtreeCount)
	for i := 0; i < subtreeCount; i++ {
		if len(subtreeGroups[i]) != txidsPerSubtree {
			return nil, fmt.Errorf("subtree %d has %d txids, expected %d", i, len(subtreeGroups[i]), txidsPerSubtree)
		}

		// Build raw binary data: concatenated 32-byte hashes in subtree order.
		rawData := make([]byte, 0, txidsPerSubtree*32)
		for _, globalIdx := range subtreeGroups[i] {
			rawData = append(rawData, txids[globalIdx]...)
		}

		// Compute subtree hash (SHA256 of raw data).
		h := sha256.Sum256(rawData)
		hashes[i] = fmt.Sprintf("%x", h)

		// Write subtree file.
		filename := filepath.Join(subtreeDir, fmt.Sprintf(fmtStr, i))
		if err := os.WriteFile(filename, rawData, 0o644); err != nil { //nolint:gosec // 0644 intentional for shared fixture files
			return nil, fmt.Errorf("writing subtree %d: %w", i, err)
		}
	}

	return hashes, nil
}

// Manifest describes the full test fixture layout.
type Manifest struct {
	Seed            int64            `json:"seed"`
	BlockHash       string           `json:"blockHash"`
	BlockHeight     uint32           `json:"blockHeight"`
	TotalTxids      int              `json:"totalTxids"`
	ArcadeInstances []ArcadeInstance `json:"arcadeInstances"`
	Subtrees        []SubtreeInfo    `json:"subtrees"`
}

// ArcadeInstance describes one simulated Arcade.
type ArcadeInstance struct {
	Index       int    `json:"index"`
	Port        int    `json:"port"`
	CallbackURL string `json:"callbackUrl"`
	TxidStart   int    `json:"txidStart"` // inclusive global index
	TxidEnd     int    `json:"txidEnd"`   // exclusive global index
}

// SubtreeInfo describes one subtree in the test block.
type SubtreeInfo struct {
	Index       int    `json:"index"`
	Hash        string `json:"hash"`
	TxidIndices []int  `json:"txidIndices"` // global indices of txids in this subtree
}

func buildManifest(seed int64, instances, txidsPerInstance, subtreeCount, txidsPerSubtree int, subtreeHashes []string) *Manifest {
	blockHash := fmt.Sprintf("%064x", sha256.Sum256([]byte(fmt.Sprintf("scale-test-block-seed-%d", seed))))

	arcades := make([]ArcadeInstance, instances)
	for i := 0; i < instances; i++ {
		arcades[i] = ArcadeInstance{
			Index:       i,
			Port:        19000 + i,
			CallbackURL: fmt.Sprintf("http://127.0.0.1:%d/callback", 19000+i),
			TxidStart:   i * txidsPerInstance,
			TxidEnd:     (i + 1) * txidsPerInstance,
		}
	}

	subtrees := make([]SubtreeInfo, subtreeCount)
	for i := 0; i < subtreeCount; i++ {
		indices := make([]int, 0, txidsPerSubtree)
		for j := i; j < instances*txidsPerInstance; j += subtreeCount {
			indices = append(indices, j)
		}
		subtrees[i] = SubtreeInfo{
			Index:       i,
			Hash:        subtreeHashes[i],
			TxidIndices: indices,
		}
	}

	return &Manifest{
		Seed:            seed,
		BlockHash:       blockHash,
		BlockHeight:     800000,
		TotalTxids:      instances * txidsPerInstance,
		ArcadeInstances: arcades,
		Subtrees:        subtrees,
	}
}

func writeManifest(path string, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644) //nolint:gosec // 0644 intentional for shared fixture files
}

// verifyFixtures checks that all txids are assigned to exactly one subtree and one arcade instance.
func verifyFixtures(m *Manifest, txids [][]byte, txidsPerSubtree int) error {
	totalTxids := len(txids)

	// Check arcade coverage.
	arcadeCoverage := make(map[int]bool, totalTxids)
	for _, a := range m.ArcadeInstances {
		for j := a.TxidStart; j < a.TxidEnd; j++ {
			if arcadeCoverage[j] {
				return fmt.Errorf("txid index %d assigned to multiple arcade instances", j)
			}
			arcadeCoverage[j] = true
		}
	}
	if len(arcadeCoverage) != totalTxids {
		return fmt.Errorf("arcade coverage: expected %d txids, got %d", totalTxids, len(arcadeCoverage))
	}

	// Check subtree coverage.
	subtreeCoverage := make(map[int]bool, totalTxids)
	for _, st := range m.Subtrees {
		if len(st.TxidIndices) != txidsPerSubtree {
			return fmt.Errorf("subtree %d has %d txids, expected %d", st.Index, len(st.TxidIndices), txidsPerSubtree)
		}
		for _, j := range st.TxidIndices {
			if subtreeCoverage[j] {
				return fmt.Errorf("txid index %d assigned to multiple subtrees", j)
			}
			subtreeCoverage[j] = true
		}
	}
	if len(subtreeCoverage) != totalTxids {
		return fmt.Errorf("subtree coverage: expected %d txids, got %d", totalTxids, len(subtreeCoverage))
	}

	return nil
}
