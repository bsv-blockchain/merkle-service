//go:build scale

package scale

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"

	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// injectBlock publishes a BlockMessage to the Kafka block topic.
func injectBlock(manifest *Manifest, blockTopic string, producer *kafka.Producer, dataHubURL string) error {
	msg := &kafka.BlockMessage{
		Hash:       manifest.BlockHash,
		Height:     manifest.BlockHeight,
		DataHubURL: dataHubURL,
	}
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encoding block message: %w", err)
	}
	if err := producer.Publish(manifest.BlockHash, data); err != nil {
		return fmt.Errorf("publishing block message: %w", err)
	}
	return nil
}

// buildBlockBinary builds a model.Block binary payload from the manifest subtrees.
func buildBlockBinary(manifest *Manifest) ([]byte, error) {
	header := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}
	subtrees := make([]*chainhash.Hash, len(manifest.Subtrees))
	for i, st := range manifest.Subtrees {
		h, err := chainhash.NewHashFromStr(st.Hash)
		if err != nil {
			return nil, fmt.Errorf("parsing subtree hash %s: %w", st.Hash, err)
		}
		subtrees[i] = h
	}
	block, err := model.NewBlock(header, nil, subtrees, 0, uint64(manifest.TotalTxids), manifest.BlockHeight, 0)
	if err != nil {
		return nil, fmt.Errorf("building block: %w", err)
	}
	return block.Bytes()
}

// startMockDataHub starts an HTTP server that serves block metadata and subtree data.
// It returns the server and its base URL.
func startMockDataHub(manifest *Manifest, subtreeData map[string][]byte) (*http.Server, string, error) {
	// Pre-build the binary block payload.
	blockBinary, err := buildBlockBinary(manifest)
	if err != nil {
		return nil, "", fmt.Errorf("building block binary: %w", err)
	}

	mux := http.NewServeMux()

	// Block metadata endpoint: /block/{hash}
	// Serves binary model.Block format (as the real DataHub does).
	mux.HandleFunc("/block/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(blockBinary)
	})

	// Subtree data endpoint: /subtree/{hash}
	mux.HandleFunc("/subtree/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 3 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		hash := parts[len(parts)-1]
		data, ok := subtreeData[hash]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})

	// Use port 0 for random available port.
	ln, err := listenOnFreePort()
	if err != nil {
		return nil, "", err
	}

	server := &http.Server{Handler: mux}
	go server.Serve(ln)

	return server, fmt.Sprintf("http://%s", ln.Addr().String()), nil
}

func listenOnFreePort() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}
