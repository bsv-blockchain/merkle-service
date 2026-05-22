package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
	"github.com/bsv-blockchain/merkle-service/internal/ssrfguard"
	"github.com/bsv-blockchain/merkle-service/internal/store"
)

var txidRegex = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

// maxCallbackTokenLen caps the bearer token we accept on /watch. Tokens are
// short shared secrets (typically 32–64 bytes); rejecting absurd values is
// cheap insurance against a buggy or hostile arcade deployment trying to
// store arbitrary blobs in our registration record.
const maxCallbackTokenLen = 4096

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboardHTML)
}

// WatchRequest represents the POST /watch request body.
//
// CallbackToken is the bearer token arcade expects on its callback endpoint
// (Authorization: Bearer <token>). The field is optional for backwards
// compatibility with arcade deployments that haven't yet shipped the matching
// token-passing change — empty token means "send no Authorization header".
type WatchRequest struct {
	TxID          string `json:"txid"`
	CallbackURL   string `json:"callbackUrl"`
	CallbackToken string `json:"callbackToken,omitempty"`
}

// WatchResponse represents the POST /watch response body.
type WatchResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// ErrorResponse represents an error response body.
type ErrorResponse struct {
	Error string `json:"error"`
}

// HealthResponse represents the GET /health response body.
type HealthResponse struct {
	Status  string            `json:"status"`
	Details map[string]string `json:"details,omitempty"`
}

func (s *Server) handleWatch(w http.ResponseWriter, r *http.Request) {
	var req WatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid request body"})
		return
	}

	// Validate txid
	if req.TxID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "txid is required"})
		return
	}
	if !txidRegex.MatchString(req.TxID) {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid txid format: must be a 64-character hex string"})
		return
	}

	// Validate callback URL. ValidateURL covers scheme/host syntax AND the
	// SSRF deny-list (loopback / link-local / RFC1918 / metadata endpoints
	// / 0.0.0.0 / multicast). The dial-time guard in
	// internal/callback.deliverCallback re-checks at connection time so a
	// URL that survives validation but later DNS-rebinds onto a private
	// IP is still refused.
	if req.CallbackURL == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "callbackUrl is required"})
		return
	}
	if err := ssrfguard.ValidateURL(req.CallbackURL, s.allowPrivateCallbackIPs, nil); err != nil {
		// Distinguish the two error classes so callers get a useful 400 message
		// without leaking internal lookup details.
		var msg string
		switch {
		case errors.Is(err, ssrfguard.ErrBlockedAddress):
			msg = "invalid callbackUrl: host is on the SSRF deny-list (private, loopback, link-local, or metadata-endpoint address)"
		default:
			msg = "invalid callbackUrl: must be a valid HTTP/HTTPS URL with a public host"
		}
		s.Logger.Warn(
			"rejected callback URL registration",
			"reason", err.Error(),
			"txid", req.TxID,
		)
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: msg})
		return
	}

	// Defensive cap on the optional bearer token. We don't validate the
	// content (it's an opaque shared secret between merkle-service and
	// arcade) but we refuse anything large enough to look like an attempt to
	// stuff a payload into the registration record.
	if len(req.CallbackToken) > maxCallbackTokenLen {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "invalid callbackToken: exceeds maximum length",
		})
		return
	}

	// Store registration
	addStart := time.Now()
	err := s.regStore.Add(req.TxID, req.CallbackURL, req.CallbackToken)
	metrics.ObserveDB(s.backendLabel(), metrics.StoreRegistration, metrics.OpAdd, addStart, err)
	if err != nil {
		// F-050: surface the per-txid callback cap as a 429 so the caller can
		// distinguish a quota error from a transient backend failure and back
		// off accordingly. The body still uses the standard ErrorResponse shape.
		if errors.Is(err, store.ErrMaxCallbacksPerTxIDExceeded) {
			s.Logger.Warn("registration rejected: per-txid callback cap exceeded",
				"txid", req.TxID, "callbackUrl", req.CallbackURL)
			writeJSON(w, http.StatusTooManyRequests, ErrorResponse{
				Error: "too many callback URLs registered for this txid",
			})
			return
		}
		s.Logger.Error("failed to add registration", "txid", req.TxID, "error", err)
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
		return
	}

	// Register callback URL in the broadcast registry. The token is stored
	// alongside so BLOCK_PROCESSED fan-out (which iterates urlRegistry rather
	// than the per-txid map) can attach the same Authorization header.
	if s.urlRegistry != nil {
		regStart := time.Now()
		regErr := s.urlRegistry.Add(req.CallbackURL, req.CallbackToken)
		metrics.ObserveDB(s.backendLabel(), metrics.StoreCallbackURLRegistry, metrics.OpAdd, regStart, regErr)
		if regErr != nil {
			s.Logger.Warn("failed to add callback URL to registry", "url", req.CallbackURL, "error", regErr)
		}
	}

	writeJSON(w, http.StatusOK, WatchResponse{
		Status:  "ok",
		Message: "registration successful",
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := s.Health()

	statusCode := http.StatusOK
	if health.Status != "healthy" {
		statusCode = http.StatusServiceUnavailable
	}

	writeJSON(w, statusCode, HealthResponse{
		Status:  health.Status,
		Details: health.Details,
	})
}

// LookupResponse represents the GET /api/lookup/{txid} response body.
type LookupResponse struct {
	TxID         string   `json:"txid"`
	CallbackUrls []string `json:"callbackUrls"`
}

func (s *Server) handleLookup(w http.ResponseWriter, r *http.Request) {
	txid := chi.URLParam(r, "txid")

	if !txidRegex.MatchString(txid) {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid txid format: must be a 64-character hex string"})
		return
	}

	if s.regStore == nil {
		writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{Error: "registration store not available"})
		return
	}

	getStart := time.Now()
	entries, err := s.regStore.Get(txid)
	metrics.ObserveDB(s.backendLabel(), metrics.StoreRegistration, metrics.OpGet, getStart, err)
	if err != nil {
		s.Logger.Error("failed to lookup registration", "txid", txid, "error", err)
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
		return
	}

	urls := make([]string, 0, len(entries))
	for _, e := range entries {
		urls = append(urls, e.URL)
	}

	writeJSON(w, http.StatusOK, LookupResponse{
		TxID:         txid,
		CallbackUrls: urls,
	})
}

// blockHashRegex is the same shape as txidRegex (64-hex) but named for
// clarity at the /reprocess call site.
var blockHashRegex = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

// reprocessProbeTimeout caps the per-DataHub block-metadata probe inside
// /reprocess so a single unhealthy DataHub can't hold the API request
// past the surrounding HTTP write timeout.
const reprocessProbeTimeout = 5 * time.Second

// ReprocessRequest represents the POST /reprocess request body.
//
// CallbackURL/Token are scoped to this single reprocess: STUMPs and
// BLOCK_PROCESSED for the requested block flow only to this URL/token,
// independent of whatever URLs are registered globally via /watch. Tokens
// are optional for back-compat with arcade deployments that haven't
// shipped the matching token-passing change.
type ReprocessRequest struct {
	BlockHash     string `json:"blockHash"`
	CallbackURL   string `json:"callbackUrl"`
	CallbackToken string `json:"callbackToken,omitempty"`
}

// ReprocessResponse represents the 202 ACK body for POST /reprocess.
type ReprocessResponse struct {
	Status     string `json:"status"`
	BlockHash  string `json:"blockHash"`
	DataHubURL string `json:"dataHubUrl"`
}

func (s *Server) handleReprocess(w http.ResponseWriter, r *http.Request) {
	if s.dataHubClient == nil || s.blockProducer == nil {
		writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Error: "reprocess endpoint not configured on this server",
		})
		return
	}

	var req ReprocessRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid request body"})
		return
	}

	if req.BlockHash == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "blockHash is required"})
		return
	}
	if !blockHashRegex.MatchString(req.BlockHash) {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid blockHash format: must be a 64-character hex string"})
		return
	}
	if req.CallbackURL == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "callbackUrl is required"})
		return
	}
	if err := ssrfguard.ValidateURL(req.CallbackURL, s.allowPrivateCallbackIPs, nil); err != nil {
		var msg string
		switch {
		case errors.Is(err, ssrfguard.ErrBlockedAddress):
			msg = "invalid callbackUrl: host is on the SSRF deny-list (private, loopback, link-local, or metadata-endpoint address)"
		default:
			msg = "invalid callbackUrl: must be a valid HTTP/HTTPS URL with a public host"
		}
		s.Logger.Warn(
			"rejected reprocess callback URL",
			"reason", err.Error(),
			"blockHash", req.BlockHash,
		)
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: msg})
		return
	}
	if len(req.CallbackToken) > maxCallbackTokenLen {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "invalid callbackToken: exceeds maximum length",
		})
		return
	}

	// Build candidate DataHub list: operator-configured fallbacks first
	// (operator-trusted), then dynamically-discovered URLs. Dedupe so a
	// URL listed in both is probed only once.
	candidates, fallbackCount := s.collectDataHubCandidates()
	if len(candidates) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Error: "no DataHub endpoints configured or discovered",
		})
		return
	}

	resolvedURL, height, subtreeHashes, status, probeErr := s.probeDataHubsForBlock(r.Context(), candidates, fallbackCount, req.BlockHash)
	if probeErr != nil {
		s.Logger.Warn(
			"reprocess: no DataHub served block",
			"blockHash", req.BlockHash,
			"candidates", len(candidates),
			"status", status,
			"error", probeErr,
		)
		writeJSON(w, status, ErrorResponse{Error: probeErr.Error()})
		return
	}

	// Clear stale callback-dedup entries for the (blockHash, callbackURL)
	// pair before publishing the reprocess block message. Without this,
	// any prior delivery attempt that exhausted its retry chain and
	// landed in DLQ would have recorded a dedup entry — and the
	// freshly-emitted STUMP/BLOCK_PROCESSED callbacks would be silently
	// skipped by callback-delivery's dedup gate (issue #122).
	//
	// Best-effort: a Delete error is logged but does not block the
	// publish. The dedup-record path in callback-delivery is itself
	// log-on-error, so a brief store outage during clear simply means
	// some entries may persist; the next reprocess request will retry.
	s.clearReprocessDedup(req.BlockHash, req.CallbackURL, subtreeHashes)

	blockMsg := kafka.BlockMessage{
		Hash:                  req.BlockHash,
		Height:                height,
		DataHubURL:            resolvedURL,
		OverrideCallbackURL:   req.CallbackURL,
		OverrideCallbackToken: req.CallbackToken,
		BypassDedup:           true,
	}
	encoded, err := blockMsg.Encode()
	if err != nil {
		s.Logger.Error("failed to encode reprocess block message",
			"blockHash", req.BlockHash, "error", err)
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
		return
	}
	// Partition key includes the override URL so a /reprocess never collides
	// with a live announcement of the same block on the same partition.
	partitionKey := req.BlockHash + "|" + req.CallbackURL
	if err := s.blockProducer.PublishWithHashKey(partitionKey, encoded); err != nil {
		s.Logger.Error("failed to publish reprocess block message",
			"blockHash", req.BlockHash, "error", err)
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to enqueue reprocess"})
		return
	}

	s.Logger.Info(
		"reprocess enqueued",
		"blockHash", req.BlockHash,
		"dataHubUrl", resolvedURL,
		"callbackUrl", req.CallbackURL,
	)
	writeJSON(w, http.StatusAccepted, ReprocessResponse{
		Status:     "queued",
		BlockHash:  req.BlockHash,
		DataHubURL: resolvedURL,
	})
}

// clearReprocessDedup removes any callback-dedup entries that would
// cause callback-delivery to skip the freshly-emitted STUMP and
// BLOCK_PROCESSED callbacks for this (blockHash, callbackURL). Mirrors
// the key derivation in callback-delivery's dedupKeyForMessage:
//   - one BLOCK_PROCESSED entry keyed on blockHash, callbackURL,
//     "BLOCK_PROCESSED".
//   - one STUMP entry per subtree index i in [0, len(subtreeHashes))
//     keyed on (blockHash + ":" + i), callbackURL, "STUMP".
//
// SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES are not re-emitted on the
// /reprocess path so we do not need to clear them.
//
// Failures are best-effort: log at warn and continue. A persistent
// store outage that leaves an entry behind will reappear on the next
// reprocess request and can be cleared then.
func (s *Server) clearReprocessDedup(blockHash, callbackURL string, subtreeHashes []string) {
	if s.dedupStore == nil {
		return
	}

	type entry struct {
		key      string
		typeName string
	}
	entries := make([]entry, 0, 1+len(subtreeHashes))
	entries = append(entries, entry{key: blockHash, typeName: string(kafka.CallbackBlockProcessed)})
	for i := range subtreeHashes {
		entries = append(entries, entry{
			key:      blockHash + ":" + strconv.Itoa(i),
			typeName: string(kafka.CallbackStump),
		})
	}

	cleared := 0
	for _, e := range entries {
		if err := s.dedupStore.Delete(e.key, callbackURL, e.typeName); err != nil {
			s.Logger.Warn(
				"reprocess: failed to clear callback dedup entry",
				"blockHash", blockHash,
				"callbackUrl", callbackURL,
				"type", e.typeName,
				"dedupTxidKey", e.key,
				"error", err,
			)
			continue
		}
		cleared++
	}

	s.Logger.Info(
		"reprocess: cleared callback dedup entries",
		"blockHash", blockHash,
		"callbackUrl", callbackURL,
		"clearedCount", cleared,
		"attemptedCount", len(entries),
		"subtreeCount", len(subtreeHashes),
	)
}

// collectDataHubCandidates returns the deduped, ordered list of DataHub
// URLs /reprocess should probe along with the count of operator-trusted
// fallback URLs occupying the prefix of the slice. Operator-configured
// fallbacks come first so they take precedence on tie and so the
// probe loop can recognize them as "always try" (peer-health filtering
// is bypassed for these). Discovered URLs are appended. A
// registry-lookup error is logged and treated as "no discovered URLs"
// — the configured fallback list still serves the request.
func (s *Server) collectDataHubCandidates() ([]string, int) {
	seen := make(map[string]struct{}, len(s.fallbackDataHubURLs))
	out := make([]string, 0, len(s.fallbackDataHubURLs))
	for _, u := range s.fallbackDataHubURLs {
		if u == "" {
			continue
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	fallbackCount := len(out)
	if s.dataHubRegistry != nil {
		discovered, err := s.dataHubRegistry.GetAll()
		if err != nil {
			s.Logger.Warn("reprocess: failed to read DataHub registry; falling back to configured URLs only",
				"error", err)
			return out, fallbackCount
		}
		for _, u := range discovered {
			if u == "" {
				continue
			}
			if _, ok := seen[u]; ok {
				continue
			}
			seen[u] = struct{}{}
			out = append(out, u)
		}
	}
	return out, fallbackCount
}

// probeDataHubsForBlock tries each candidate DataHub in order and returns
// the first one that successfully serves block metadata for blockHash, the
// block's height, and HTTP 202 (status echoes the API response shape:
// caller should write 202 on success).
//
// The first fallbackCount entries are operator-configured and are always
// probed — peer-health filtering is bypassed for them. Discovered peers
// (entries after fallbackCount) are skipped if currently marked unhealthy
// by the attached PeerHealth tracker.
//
// On exhausted candidates, returns:
//   - 404 if every probe returned ErrNotFound (block genuinely missing).
//   - 502 if every probe failed for any other reason (transport, timeout, 5xx).
//
// Mixed outcomes are bucketed as 502 since at least one DataHub couldn't
// confirm a 404, and the caller should treat the result as ambiguous and
// retry later. If every discovered peer was skipped as unhealthy and no
// operator fallbacks were configured, returns 502 — we cannot confirm a
// 404 without actually probing.
func (s *Server) probeDataHubsForBlock(parentCtx context.Context, candidates []string, fallbackCount int, blockHash string) (resolvedURL string, height uint32, subtreeHashes []string, status int, err error) {
	ph := s.dataHubClient.PeerHealth()
	allNotFound := true
	probed := 0
	for i, url := range candidates {
		// Operator-configured fallbacks (index < fallbackCount) are
		// always tried. Discovered peers are skipped while unhealthy.
		if i >= fallbackCount && ph != nil && !ph.IsHealthy(url) {
			s.Logger.Debug("reprocess: skipping unhealthy discovered DataHub peer",
				"dataHubUrl", url, "blockHash", blockHash)
			continue
		}

		ctx, cancel := context.WithTimeout(parentCtx, reprocessProbeTimeout)
		meta, ferr := s.dataHubClient.FetchBlockMetadata(ctx, url, blockHash)
		cancel()
		probed++
		if ferr == nil {
			return url, meta.Height, meta.Subtrees, http.StatusAccepted, nil
		}
		if !errors.Is(ferr, datahub.ErrNotFound) {
			allNotFound = false
		}
		s.Logger.Debug(
			"reprocess probe failed",
			"dataHubUrl", url,
			"blockHash", blockHash,
			"notFound", errors.Is(ferr, datahub.ErrNotFound),
			"error", ferr,
		)
	}
	if probed == 0 {
		// Every candidate was skipped as unhealthy and there were no
		// operator fallbacks to force-try. We cannot honestly claim
		// the block is missing without probing anyone.
		return "", 0, nil, http.StatusBadGateway, errors.New("no DataHub could serve the requested block")
	}
	if allNotFound {
		return "", 0, nil, http.StatusNotFound, errors.New("block not found on any known DataHub")
	}
	return "", 0, nil, http.StatusBadGateway, errors.New("no DataHub could serve the requested block")
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
