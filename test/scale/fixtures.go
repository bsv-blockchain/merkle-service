//go:build scale

package scale

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	testdataDir = "testdata"
	basePort    = 19000
)

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
	TxidStart   int    `json:"txidStart"`
	TxidEnd     int    `json:"txidEnd"`
}

// SubtreeInfo describes one subtree in the test block.
type SubtreeInfo struct {
	Index       int    `json:"index"`
	Hash        string `json:"hash"`
	TxidIndices []int  `json:"txidIndices"`
}

// loadTxids reads the binary txid file and returns 32-byte hash slices.
func loadTxids(path string) ([][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading txids file: %w", err)
	}
	if len(data)%32 != 0 {
		return nil, fmt.Errorf("txids file size %d is not a multiple of 32", len(data))
	}
	count := len(data) / 32
	txids := make([][]byte, count)
	for i := 0; i < count; i++ {
		txids[i] = data[i*32 : (i+1)*32]
	}
	return txids, nil
}

// loadSubtree reads a single subtree binary file.
func loadSubtree(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading subtree file: %w", err)
	}
	if len(data)%32 != 0 {
		return nil, fmt.Errorf("subtree file size %d is not a multiple of 32", len(data))
	}
	return data, nil
}

// loadManifest reads and parses the manifest JSON file.
func loadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}
	return &m, nil
}

// loadAllFixtures loads the manifest, txids, and all subtree files from the given directory.
func loadAllFixtures(dir string) (*Manifest, [][]byte, map[string][]byte, error) {
	manifest, err := loadManifest(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, nil, nil, err
	}

	txids, err := loadTxids(filepath.Join(dir, "txids.bin"))
	if err != nil {
		return nil, nil, nil, err
	}

	// Use 3-digit format for subtree filenames when there are more than 99 subtrees.
	fmtStr := "%02d.bin"
	if len(manifest.Subtrees) > 99 {
		fmtStr = "%03d.bin"
	}

	subtreeData := make(map[string][]byte, len(manifest.Subtrees))
	for _, st := range manifest.Subtrees {
		path := filepath.Join(dir, "subtrees", fmt.Sprintf(fmtStr, st.Index))
		data, err := loadSubtree(path)
		if err != nil {
			return nil, nil, nil, err
		}
		subtreeData[st.Hash] = data
	}

	return manifest, txids, subtreeData, nil
}
