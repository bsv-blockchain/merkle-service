package datahub

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
)

// Client fetches subtree and block data from Teranode DataHub endpoints.
type Client struct {
	httpClient *http.Client
	maxRetries int
	logger     *slog.Logger
}

// NewClient creates a new DataHub client.
func NewClient(timeoutSec int, maxRetries int, logger *slog.Logger) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutSec) * time.Second,
		},
		maxRetries: maxRetries,
		logger:     logger,
	}
}

// BlockHeader holds the parsed block header from a DataHub response.
type BlockHeader struct {
	Version       uint32 `json:"version"`
	HashPrevBlock string `json:"hash_prev_block"`
	HashMerkleRoot string `json:"hash_merkle_root"`
	Timestamp     uint32 `json:"timestamp"`
	Bits          string `json:"bits"`
	Nonce         uint32 `json:"nonce"`
}

// BlockMetadata holds the parsed response from a DataHub block endpoint.
type BlockMetadata struct {
	Height           uint32       `json:"height"`
	Header           *BlockHeader `json:"header,omitempty"`
	Subtrees         []string     `json:"subtrees"`
	TransactionCount uint64       `json:"transaction_count"`
}

// FetchSubtreeRaw fetches raw binary subtree data from a DataHub endpoint.
func (c *Client) FetchSubtreeRaw(ctx context.Context, dataHubURL string, hash string) ([]byte, error) {
	url := fmt.Sprintf("%s/subtree/%s", dataHubURL, hash)
	return c.doGetWithRetry(ctx, url)
}

// FetchSubtree fetches and parses a subtree from a DataHub endpoint.
// The DataHub binary endpoint returns concatenated 32-byte txid hashes,
// not the full go-subtree Serialize() format.
func (c *Client) FetchSubtree(ctx context.Context, dataHubURL string, hash string) (*subtreepkg.Subtree, error) {
	raw, err := c.FetchSubtreeRaw(ctx, dataHubURL, hash)
	if err != nil {
		return nil, fmt.Errorf("fetching subtree %s: %w", hash, err)
	}

	nodes, err := ParseRawNodes(raw)
	if err != nil {
		return nil, fmt.Errorf("parsing subtree %s: %w", hash, err)
	}

	// Build a Subtree struct with the parsed nodes.
	st := &subtreepkg.Subtree{
		Nodes: nodes,
	}

	return st, nil
}

// ParseRawTxids parses DataHub binary subtree response (concatenated 32-byte hashes)
// into a slice of hex-encoded txid strings in Bitcoin display order (reversed bytes).
func ParseRawTxids(data []byte) ([]string, error) {
	if len(data)%chainhash.HashSize != 0 {
		return nil, fmt.Errorf("invalid subtree data length %d: not a multiple of %d", len(data), chainhash.HashSize)
	}
	count := len(data) / chainhash.HashSize
	txids := make([]string, count)
	for i := 0; i < count; i++ {
		var h chainhash.Hash
		copy(h[:], data[i*chainhash.HashSize:(i+1)*chainhash.HashSize])
		txids[i] = h.String()
	}
	return txids, nil
}

// ParseRawNodes parses DataHub binary subtree response (concatenated 32-byte hashes)
// into a slice of subtree Nodes (with zero fee/size since DataHub doesn't include those).
func ParseRawNodes(data []byte) ([]subtreepkg.Node, error) {
	if len(data)%chainhash.HashSize != 0 {
		return nil, fmt.Errorf("invalid subtree data length %d: not a multiple of %d", len(data), chainhash.HashSize)
	}
	count := len(data) / chainhash.HashSize
	nodes := make([]subtreepkg.Node, count)
	for i := 0; i < count; i++ {
		copy(nodes[i].Hash[:], data[i*chainhash.HashSize:(i+1)*chainhash.HashSize])
	}
	return nodes, nil
}

// ParseBinaryBlockMetadata decodes the Teranode DataHub binary block response
// using the full model.Block binary format.
func ParseBinaryBlockMetadata(data []byte) (*BlockMetadata, error) {
	block, err := model.NewBlockFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("parsing block binary: %w", err)
	}

	subtrees := make([]string, len(block.Subtrees))
	for i, h := range block.Subtrees {
		subtrees[i] = h.String()
	}

	return &BlockMetadata{
		Height:           block.Height,
		Subtrees:         subtrees,
		TransactionCount: block.TransactionCount,
	}, nil
}

// FetchBlockMetadata fetches block metadata (binary) from a DataHub endpoint.
func (c *Client) FetchBlockMetadata(ctx context.Context, dataHubURL string, hash string) (*BlockMetadata, error) {
	url := fmt.Sprintf("%s/block/%s", dataHubURL, hash)
	data, err := c.doGetWithRetry(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching block metadata %s: %w", hash, err)
	}

	meta, err := ParseBinaryBlockMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("parsing block metadata %s: %w", hash, err)
	}

	return meta, nil
}

// doGetWithRetry performs an HTTP GET with exponential backoff retry.
func (c *Client) doGetWithRetry(ctx context.Context, url string) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))*500) * time.Millisecond
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("creating request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			c.logger.Warn("DataHub request failed, retrying",
				"url", url,
				"attempt", attempt+1,
				"error", err,
			)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("not found: %s (HTTP 404)", url)
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, url, string(body))
			c.logger.Warn("DataHub returned error, retrying",
				"url", url,
				"status", resp.StatusCode,
				"attempt", attempt+1,
			)
			continue
		}

		if err != nil {
			lastErr = err
			continue
		}

		return body, nil
	}

	return nil, fmt.Errorf("DataHub request failed after %d attempts: %w", c.maxRetries+1, lastErr)
}
