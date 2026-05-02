package datahub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"

	"github.com/bsv-blockchain/merkle-service/internal/ssrfguard"
)

// Default per-endpoint response body caps. They are intentionally generous so
// healthy traffic is never rejected, but tight enough that a malicious or
// malfunctioning DataHub endpoint cannot exhaust process memory by streaming
// an unbounded body. Operators can override these via DataHubConfig (see
// internal/config). See finding F-027.
const (
	// DefaultMaxBlockBytes caps a single /block/<hash> JSON/binary response.
	// Block metadata is small (header + subtree hash list) — even a block with
	// thousands of subtrees is well under 1 MiB; 16 MiB is two orders of
	// magnitude of headroom.
	DefaultMaxBlockBytes int64 = 16 * 1024 * 1024 // 16 MiB

	// DefaultMaxSubtreeBytes caps a single /subtree/<hash> binary response.
	// A DataHub subtree is concatenated 32-byte hashes; Teranode subtrees can
	// have on the order of millions of leaves, so we allow up to 1 GiB
	// (~33.5M txids). Operators running with smaller subtree limits should
	// tune this down via DataHubConfig.MaxSubtreeBytes.
	DefaultMaxSubtreeBytes int64 = 1 * 1024 * 1024 * 1024 // 1 GiB

	// DefaultMaxGenericBytes is the fallback cap for any future endpoints that
	// don't have a tuned per-endpoint cap. 128 MiB is large enough for
	// reasonable payloads but still bounded.
	DefaultMaxGenericBytes int64 = 128 * 1024 * 1024 // 128 MiB
)

// Client fetches subtree and block data from Teranode DataHub endpoints.
//
// Response bodies are read through an io.LimitReader and Content-Length is
// checked before reading, so a hostile or malfunctioning DataHub cannot
// exhaust process memory by returning an unbounded response.
//
// Because dataHubURL is sourced from peer-controlled P2P announcements, the
// client also gates outbound requests through the shared ssrfguard predicate
// at two layers: a URL/DNS check at request time and a Dialer.Control hook at
// connect time. A malicious peer cannot redirect block/subtree fetches at
// loopback, link-local, RFC1918 or cloud-metadata IPs unless the operator
// has explicitly opted in via DataHubConfig.AllowPrivateIPs. See finding
// F-028.
type Client struct {
	httpClient *http.Client
	maxRetries int
	logger     *slog.Logger

	// Per-endpoint response body caps in bytes. Zero means use the
	// corresponding Default*. Set via NewClientWithCaps or SetCaps.
	maxBlockBytes   int64
	maxSubtreeBytes int64
	maxGenericBytes int64

	// allowPrivateIPs disables the SSRF predicate's private/loopback
	// /link-local check. The unspecified/multicast checks remain in
	// force regardless. Mirrors CallbackConfig.AllowPrivateIPs.
	allowPrivateIPs bool
}

// NewClient creates a new DataHub client with the default per-endpoint
// response body caps. The SSRF guard is enabled with allowPrivateIPs=true
// for parity with this constructor's historical test-friendly behavior
// (httptest binds to 127.0.0.1). Production code MUST use
// NewClientWithSSRFGuard so private destinations are blocked by default.
func NewClient(timeoutSec, maxRetries int, logger *slog.Logger) *Client {
	return NewClientWithSSRFGuard(timeoutSec, maxRetries, 0, 0, true, logger)
}

// NewClientWithCaps creates a new DataHub client with explicit per-endpoint
// response body caps. A cap of 0 selects the corresponding Default* value.
// Negative caps are clamped to 0 (i.e. fall back to the default) to avoid
// silently disabling the protection. The SSRF guard is enabled but with
// allowPrivateIPs=true so existing tests using httptest (127.0.0.1) keep
// working. Production code paths should call NewClientWithSSRFGuard with
// the operator's AllowPrivateIPs setting.
func NewClientWithCaps(timeoutSec, maxRetries int, maxBlockBytes, maxSubtreeBytes int64, logger *slog.Logger) *Client {
	return NewClientWithSSRFGuard(timeoutSec, maxRetries, maxBlockBytes, maxSubtreeBytes, true, logger)
}

// NewClientWithSSRFGuard creates a new DataHub client with explicit
// per-endpoint response body caps and an SSRF predicate applied at both
// request time (URL/DNS validation) and dial time (Dialer.Control). A cap
// of 0 selects the corresponding Default*; negative caps are clamped to
// 0. allowPrivateIPs=false (the production default) blocks
// loopback/link-local/RFC1918/cloud-metadata destinations even if a
// peer-supplied dataHubURL points there. Mitigates F-028.
func NewClientWithSSRFGuard(timeoutSec, maxRetries int, maxBlockBytes, maxSubtreeBytes int64, allowPrivateIPs bool, logger *slog.Logger) *Client {
	if maxBlockBytes <= 0 {
		maxBlockBytes = DefaultMaxBlockBytes
	}
	if maxSubtreeBytes <= 0 {
		maxSubtreeBytes = DefaultMaxSubtreeBytes
	}
	return &Client{
		httpClient:      newSSRFAwareHTTPClient(timeoutSec, allowPrivateIPs),
		maxRetries:      maxRetries,
		logger:          logger,
		maxBlockBytes:   maxBlockBytes,
		maxSubtreeBytes: maxSubtreeBytes,
		maxGenericBytes: DefaultMaxGenericBytes,
		allowPrivateIPs: allowPrivateIPs,
	}
}

// newSSRFAwareHTTPClient builds the underlying http.Client used by the
// DataHub client. A net.Dialer.Control hook calls
// ssrfguard.CheckDialAddress on every TCP dial so a peer that bypasses
// the request-time URL check (e.g. via DNS rebinding) is still rejected
// at connection time. The Control hook receives the resolved
// "ip:port" address from Go's resolver — there is no opportunity for a
// hostname to be substituted between resolution and dial.
func newSSRFAwareHTTPClient(timeoutSec int, allowPrivateIPs bool) *http.Client {
	transport := &http.Transport{
		IdleConnTimeout:    90 * time.Second,
		DisableCompression: false,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			Control: func(network, address string, _ syscall.RawConn) error {
				if network != "tcp" && network != "tcp4" && network != "tcp6" {
					return nil
				}
				return ssrfguard.CheckDialAddress(address, allowPrivateIPs)
			},
		}).DialContext,
	}
	return &http.Client{
		Timeout:   time.Duration(timeoutSec) * time.Second,
		Transport: transport,
	}
}

// BlockHeader holds the parsed block header from a DataHub response.
type BlockHeader struct {
	Version        uint32 `json:"version"`
	HashPrevBlock  string `json:"hash_prev_block"`
	HashMerkleRoot string `json:"hash_merkle_root"`
	Timestamp      uint32 `json:"timestamp"`
	Bits           string `json:"bits"`
	Nonce          uint32 `json:"nonce"`
}

// BlockMetadata holds the parsed response from a DataHub block endpoint.
type BlockMetadata struct {
	Height           uint32       `json:"height"`
	Header           *BlockHeader `json:"header,omitempty"`
	Subtrees         []string     `json:"subtrees"`
	TransactionCount uint64       `json:"transaction_count"`
}

// FetchSubtreeRaw fetches raw binary subtree data from a DataHub endpoint.
// dataHubURL is treated as untrusted (it flows from peer-controlled P2P
// announcements) and is validated against the SSRF predicate before any
// network I/O happens.
func (c *Client) FetchSubtreeRaw(ctx context.Context, dataHubURL, hash string) ([]byte, error) {
	if err := c.validateDataHubURL(dataHubURL); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/subtree/%s", dataHubURL, hash)
	return c.doGetWithRetry(ctx, url, c.maxSubtreeBytes)
}

// FetchSubtree fetches and parses a subtree from a DataHub endpoint.
// The DataHub binary endpoint returns concatenated 32-byte txid hashes,
// not the full go-subtree Serialize() format.
func (c *Client) FetchSubtree(ctx context.Context, dataHubURL, hash string) (*subtreepkg.Subtree, error) {
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
// dataHubURL is treated as untrusted (it flows from peer-controlled P2P
// announcements) and is validated against the SSRF predicate before any
// network I/O happens.
func (c *Client) FetchBlockMetadata(ctx context.Context, dataHubURL, hash string) (*BlockMetadata, error) {
	if err := c.validateDataHubURL(dataHubURL); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/block/%s", dataHubURL, hash)
	data, err := c.doGetWithRetry(ctx, url, c.maxBlockBytes)
	if err != nil {
		return nil, fmt.Errorf("fetching block metadata %s: %w", hash, err)
	}

	meta, err := ParseBinaryBlockMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("parsing block metadata %s: %w", hash, err)
	}

	return meta, nil
}

// validateDataHubURL applies the shared SSRF predicate to a peer-supplied
// DataHub base URL. It runs at request time (so the offending URL never
// reaches Go's HTTP client) and is reinforced by the Dialer.Control hook
// installed on the client transport. Errors are wrapped so callers can
// distinguish SSRF rejection from transport failures.
func (c *Client) validateDataHubURL(rawURL string) error {
	if err := ssrfguard.ValidateURL(rawURL, c.allowPrivateIPs, nil); err != nil {
		c.logger.Warn("rejecting DataHub URL by SSRF policy",
			"url", rawURL,
			"allowPrivateIPs", c.allowPrivateIPs,
			"error", err,
		)
		switch {
		case errors.Is(err, ssrfguard.ErrBlockedAddress):
			return fmt.Errorf("DataHub URL rejected by SSRF policy: %w", err)
		case errors.Is(err, ssrfguard.ErrInvalidURL):
			return fmt.Errorf("invalid DataHub URL: %w", err)
		default:
			return fmt.Errorf("DataHub URL validation failed: %w", err)
		}
	}
	return nil
}

// readCapped reads up to maxBytes from r and returns an error if the body is
// larger than the cap. It uses io.LimitReader with maxBytes+1 so it can
// distinguish "exactly at cap" (allowed) from "exceeded cap" (rejected).
// The error intentionally does not include any of the response content.
func readCapped(r io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxGenericBytes
	}
	body, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return body, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxBytes)
	}
	return body, nil
}

// doGetWithRetry performs an HTTP GET with exponential backoff retry. The
// response body is read through io.LimitReader so a malicious or
// malfunctioning DataHub cannot exhaust process memory by returning an
// unbounded body. Content-Length is checked before reading so advertised
// oversize responses are rejected without ever buffering them.
func (c *Client) doGetWithRetry(ctx context.Context, url string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = c.maxGenericBytes
		if maxBytes <= 0 {
			maxBytes = DefaultMaxGenericBytes
		}
	}

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

		// Reject advertised oversize responses before reading. resp.ContentLength
		// is -1 when the server omits the header or uses chunked encoding; in
		// that case we fall through to the LimitReader check below.
		if resp.ContentLength >= 0 && resp.ContentLength > maxBytes {
			// Drain a small amount to allow connection reuse, then close.
			_, _ = io.CopyN(io.Discard, resp.Body, 1024)
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("response Content-Length %d exceeds cap of %d bytes", resp.ContentLength, maxBytes)
			c.logger.Warn("DataHub returned oversize Content-Length, retrying",
				"url", url,
				"contentLength", resp.ContentLength,
				"cap", maxBytes,
				"attempt", attempt+1,
			)
			continue
		}

		body, readErr := readCapped(resp.Body, maxBytes)
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("not found: %s (HTTP 404)", url)
		}

		if resp.StatusCode != http.StatusOK {
			// Truncate the error body so a hostile server can't bloat our log
			// lines either; readCapped already bounded it to maxBytes.
			lastErr = fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, url, string(body))
			c.logger.Warn("DataHub returned error, retrying",
				"url", url,
				"status", resp.StatusCode,
				"attempt", attempt+1,
			)
			continue
		}

		if readErr != nil {
			lastErr = readErr
			c.logger.Warn("DataHub response body read failed, retrying",
				"url", url,
				"attempt", attempt+1,
				"error", readErr,
			)
			continue
		}

		return body, nil
	}

	return nil, fmt.Errorf("DataHub request failed after %d attempts: %w", c.maxRetries+1, lastErr)
}
