package datahub

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"sync"
	"syscall"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/bsv-blockchain/merkle-service/internal/logfields"
	"github.com/bsv-blockchain/merkle-service/internal/ssrfguard"
)

// ErrNotFound is returned wrapped by the fetch methods when the DataHub
// returned a 404 for the requested resource. Distinguishing 404 from other
// failures lets callers (notably the /reprocess flow probing multiple
// candidates) tell "every DataHub knows the block is missing" from "every
// DataHub failed for transport reasons" and choose the right HTTP status
// to surface back to the API caller.
var ErrNotFound = errors.New("datahub: not found")

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

	// peerHealth, when non-nil, is informed of every fetch outcome so
	// call sites (block-processor failover, /reprocess probe) can skip
	// hosts that are persistently failing. The client itself does not
	// short-circuit on unhealthy peers — selection stays at the call
	// site to preserve the "one URL in, one body out" contract.
	peerHealth *PeerHealth

	// lookupIP overrides the DNS resolver used by SSRF URL validation. nil
	// selects net.LookupIP (the production path); tests set a stub to count or
	// fake resolutions.
	lookupIP func(host string) ([]net.IP, error)

	// validatedURLs caches SUCCESSFUL SSRF validations of peer DataHub base URLs
	// for dataHubURLValidationTTL, so a high subtree-fetch rate to the same peer
	// does not re-run a synchronous DNS lookup (ssrfguard.ValidateURL ->
	// net.LookupIP) on every fetch. Success only: rejections are never cached, so
	// a transient resolver blip can't pin a peer as bad and a malicious URL keeps
	// failing every time. DNS-rebind safety is unaffected — the transport's
	// Dialer.Control hook re-validates the actually-resolved IP on every TCP
	// dial, uncached. Keyed by the exact base URL string (stable per peer).
	validatedURLs sync.Map // rawURL string -> expiry unix-nanos (int64)
}

// SetPeerHealth attaches a PeerHealth tracker. After this call, every
// Fetch* invocation records success or failure against the tracker before
// returning — unless the caller's ctx is already dead at record time
// (cancellation says nothing about the peer; see recordPeerOutcome) or the
// call site opted out via WithoutPeerRecording to classify and record the
// outcome itself. Passing nil disables tracking. Safe to call once at
// startup; not safe to call concurrently with in-flight requests.
func (c *Client) SetPeerHealth(p *PeerHealth) {
	c.peerHealth = p
}

// FetchOption customizes a single fetch call. Currently only used to opt a
// call site out of the client's internal peer-health recording.
type FetchOption func(*fetchOptions)

type fetchOptions struct {
	skipPeerRecording bool
}

// WithoutPeerRecording disables the client's internal peer-health recording
// for one fetch. Call sites that can classify outcomes better than the
// client — the subtree processor knows the announcement's age and can tell
// a stale-announcement 404 (our consumer lag) from a peer lying about fresh
// data — use this and record against Client.PeerHealth() themselves.
func WithoutPeerRecording() FetchOption {
	return func(o *fetchOptions) {
		o.skipPeerRecording = true
	}
}

// PeerHealth returns the attached tracker, or nil if none was set. Call
// sites use this to filter candidate peers before invoking Fetch*.
func (c *Client) PeerHealth() *PeerHealth {
	return c.peerHealth
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

// newSSRFAwareTransport builds the SSRF-guarded *http.Transport used by
// newSSRFAwareHTTPClient. A net.Dialer.Control hook calls
// ssrfguard.CheckDialAddress on every TCP dial so a peer that bypasses
// the request-time URL check (e.g. via DNS rebinding) is still rejected
// at connection time. The Control hook receives the resolved
// "ip:port" address from Go's resolver — there is no opportunity for a
// hostname to be substituted between resolution and dial.
//
// Split out from newSSRFAwareHTTPClient so tests can inspect the raw
// connection-pool tuning directly, without unwrapping the otelhttp.Transport
// that wraps it in the client.
func newSSRFAwareTransport(allowPrivateIPs bool) *http.Transport {
	return &http.Transport{
		IdleConnTimeout:    90 * time.Second,
		DisableCompression: false,
		// Keep keep-alive connections warm for reuse under block-time fan-out.
		// net/http's default MaxIdleConnsPerHost is 2, so concurrent subtree
		// fetches to the SAME DataHub peer would otherwise re-dial (and re-TLS)
		// on all but two of them — pure handshake overhead on the hot path.
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 64,
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
}

// newSSRFAwareHTTPClient builds the http.Client used by the DataHub client,
// wrapping newSSRFAwareTransport with otelhttp.NewTransport so every
// outbound DataHub request carries a client span with the active trace's
// traceparent header — closing the arcade->merkle->arcade trace across the
// merkle-service->DataHub hop. With telemetry disabled this uses the global
// no-op TracerProvider, so the wrap adds no real spans or allocations beyond
// the wrapper's own inert bookkeeping.
func newSSRFAwareHTTPClient(timeoutSec int, allowPrivateIPs bool) *http.Client {
	return &http.Client{
		Timeout:   time.Duration(timeoutSec) * time.Second,
		Transport: otelhttp.NewTransport(newSSRFAwareTransport(allowPrivateIPs)),
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

	// HeaderHex is the hex-encoded 80-byte block header and CoinbaseTxHex the
	// hex-encoded raw coinbase transaction, both taken from the fetched block
	// binary. They mirror the kafka.BlockMessage.Header/.Coinbase wire format
	// so consumers can use them interchangeably. The BlockMessage fields are
	// not reliably populated (teranode's block announcement never carries the
	// coinbase, and /reprocess-driven messages carry neither), whereas the
	// block binary — which we fetch anyway for the subtree list — always
	// carries both.
	HeaderHex     string `json:"header_hex,omitempty"`
	CoinbaseTxHex string `json:"coinbase_tx_hex,omitempty"`

	// CoinbaseBUMPHex is teranode's ready-made BRC-74 coinbase BUMP, carried
	// in the block binary's tail. When present and valid it is the preferred
	// source for the BLOCK_PROCESSED coinbase BUMP: it is authoritative (the
	// node computed it from the full block, including the final-subtree
	// height-lift) and it remains available even after every peer has pruned
	// the block's subtree data — reconstruction from subtree 0 cannot make
	// that guarantee.
	CoinbaseBUMPHex string `json:"coinbase_bump_hex,omitempty"`
}

// FetchSubtreeRaw fetches raw binary subtree data from a DataHub endpoint.
// dataHubURL is treated as untrusted (it flows from peer-controlled P2P
// announcements) and is validated against the SSRF predicate before any
// network I/O happens. Pass WithoutPeerRecording to suppress the client's
// internal peer-health recording for this call (the subtree processor does,
// so it can classify 404s by announcement age before recording).
func (c *Client) FetchSubtreeRaw(ctx context.Context, dataHubURL, hash string, opts ...FetchOption) ([]byte, error) {
	var o fetchOptions
	for _, opt := range opts {
		opt(&o)
	}
	if err := c.validateDataHubURL(dataHubURL); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/subtree/%s", dataHubURL, hash)
	data, err := c.doGetWithRetry(ctx, url, c.maxSubtreeBytes)
	if !o.skipPeerRecording {
		c.recordPeerOutcome(ctx, dataHubURL, err)
	}
	return data, err
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

	meta := &BlockMetadata{
		Height:           block.Height,
		Subtrees:         subtrees,
		TransactionCount: block.TransactionCount,
	}

	// Carry the header and coinbase through instead of discarding them: the
	// block binary is the only source that reliably has both (the P2P block
	// announcement omits the coinbase; /reprocess messages omit the header
	// too), and downstream BLOCK_PROCESSED enrichment needs them for the
	// merkle root and the coinbase BUMP.
	if block.Header != nil {
		meta.HeaderHex = hex.EncodeToString(block.Header.Bytes())
	}
	// A nil coinbase serializes as an input-less placeholder tx and parses
	// back as such, so gate on a real coinbase (exactly one input) rather
	// than non-nil alone — an empty placeholder must read as "absent", not
	// hash to a bogus txid downstream.
	if block.CoinbaseTx != nil && len(block.CoinbaseTx.Inputs) > 0 {
		meta.CoinbaseTxHex = hex.EncodeToString(block.CoinbaseTx.Bytes())
	}
	if len(block.CoinbaseBUMP) > 0 {
		meta.CoinbaseBUMPHex = hex.EncodeToString(block.CoinbaseBUMP)
	}

	return meta, nil
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
		c.recordPeerOutcome(ctx, dataHubURL, err)
		return nil, fmt.Errorf("fetching block metadata %s: %w", hash, err)
	}

	meta, err := ParseBinaryBlockMetadata(data)
	if err != nil {
		// Parse failure is a peer-side issue (malformed response): the
		// transport succeeded but the body the peer returned cannot be
		// trusted. Count it against the peer.
		c.recordPeerOutcome(ctx, dataHubURL, err)
		return nil, fmt.Errorf("parsing block metadata %s: %w", hash, err)
	}

	c.recordPeerOutcome(ctx, dataHubURL, nil)
	return meta, nil
}

// recordPeerOutcome forwards a fetch outcome to the attached PeerHealth
// tracker, if any.
//
// Cancellation-neutral: when the CALLER's ctx is already dead at record
// time, nothing is recorded — neither failure nor success. A pod shutdown,
// consumer rebalance, or partition loss aborting an in-flight fetch says
// nothing about the peer, and on dev-ovh-1 (2026-07-15) exactly those
// context.Canceled errors tripped the breaker on fresh pods within minutes
// of a rollout. The client's OWN HTTP timeout firing while the caller ctx
// is alive still records a failure: peer slowness is peer-attributable.
// The two cases are distinguished via ctx.Err(), never by error string.
//
// Otherwise a nil err counts as a success and resets the peer's
// consecutive-failure counter; a non-nil err counts as a failure regardless
// of category (404, 5xx, timeout, network, parse) so a peer that lies
// about the data it has is treated as unhealthy. A breaker-opening failure
// is WARN-logged here with the breaker parameters.
func (c *Client) recordPeerOutcome(ctx context.Context, dataHubURL string, err error) {
	if c.peerHealth == nil {
		return
	}
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		c.peerHealth.RecordSuccess(dataHubURL)
		return
	}
	if tripped := c.peerHealth.RecordFailure(dataHubURL); tripped {
		c.logger.Warn(
			"DataHub peer marked unhealthy: consecutive-failure threshold reached",
			logfields.DataHubURL(dataHubURL),
			"failureThreshold", c.peerHealth.Threshold(),
			"cooldown", c.peerHealth.Cooldown().String(),
			"error", err,
		)
	}
}

// dataHubURLValidationTTL bounds how long a successful SSRF validation of a
// peer DataHub base URL is reused before the DNS lookup is repeated. Short
// enough that a peer whose DNS legitimately changes is re-checked promptly;
// the dial-time Control hook enforces SSRF on every connection regardless.
const dataHubURLValidationTTL = 60 * time.Second

// validateDataHubURL applies the shared SSRF predicate to a peer-supplied
// DataHub base URL. It runs at request time (so the offending URL never
// reaches Go's HTTP client) and is reinforced by the Dialer.Control hook
// installed on the client transport. Errors are wrapped so callers can
// distinguish SSRF rejection from transport failures.
//
// A successful validation is cached for dataHubURLValidationTTL so a burst of
// fetches to the same peer does not repeat the synchronous DNS lookup. Only
// successes are cached (see validatedURLs); rejections always re-run.
func (c *Client) validateDataHubURL(rawURL string) error {
	if exp, ok := c.validatedURLs.Load(rawURL); ok {
		if time.Now().UnixNano() < exp.(int64) {
			return nil
		}
		c.validatedURLs.Delete(rawURL)
	}

	if err := ssrfguard.ValidateURL(rawURL, c.allowPrivateIPs, c.lookupIP); err != nil {
		c.logger.Warn(
			"rejecting DataHub URL by SSRF policy",
			logfields.DataHubURL(rawURL),
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
	c.validatedURLs.Store(rawURL, time.Now().Add(dataHubURLValidationTTL).UnixNano())
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
			c.logger.Warn(
				"DataHub request failed, retrying",
				logfields.DataHubURL(url),
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
			c.logger.Warn(
				"DataHub returned oversize Content-Length, retrying",
				logfields.DataHubURL(url),
				"contentLength", resp.ContentLength,
				"cap", maxBytes,
				"attempt", attempt+1,
			)
			continue
		}

		body, readErr := readCapped(resp.Body, maxBytes)
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("%w: %s (HTTP 404)", ErrNotFound, url)
		}

		// Non-retryable 4xx (e.g. 401 Unauthorized, 403 Forbidden,
		// 422 Unprocessable) signal a client-side problem the peer
		// cannot resolve by us retrying. Return immediately so the
		// caller fails over to another peer instead of burning the
		// retry budget against an auth-rejecting endpoint. 408 (Request
		// Timeout) and 429 (Too Many Requests) are the standard "try
		// again later" 4xx codes and stay on the retry path below.
		if isNonRetryable4xx(resp.StatusCode) {
			return nil, fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, url, string(body))
		}

		if resp.StatusCode != http.StatusOK {
			// Truncate the error body so a hostile server can't bloat our log
			// lines either; readCapped already bounded it to maxBytes.
			lastErr = fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, url, string(body))
			c.logger.Warn(
				"DataHub returned error, retrying",
				logfields.DataHubURL(url),
				"status", resp.StatusCode,
				"attempt", attempt+1,
			)
			continue
		}

		if readErr != nil {
			lastErr = readErr
			c.logger.Warn(
				"DataHub response body read failed, retrying",
				logfields.DataHubURL(url),
				"attempt", attempt+1,
				"error", readErr,
			)
			continue
		}

		return body, nil
	}

	return nil, fmt.Errorf("DataHub request failed after %d attempts: %w", c.maxRetries+1, lastErr)
}

// isNonRetryable4xx reports whether code is a 4xx response that we treat
// as permanent. 404 is handled by the dedicated ErrNotFound path above
// and intentionally not included here. 408 (Request Timeout) and 429
// (Too Many Requests) are the standard "try again later" 4xx codes and
// stay on the retry path.
func isNonRetryable4xx(code int) bool {
	if code < 400 || code >= 500 {
		return false
	}
	switch code {
	case http.StatusNotFound, http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	return true
}
