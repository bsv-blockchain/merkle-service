package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"

	"github.com/go-chi/chi/v5"

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
		s.Logger.Warn("rejected callback URL registration",
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
	if err := s.regStore.Add(req.TxID, req.CallbackURL, req.CallbackToken); err != nil {
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
		if err := s.urlRegistry.Add(req.CallbackURL, req.CallbackToken); err != nil {
			s.Logger.Warn("failed to add callback URL to registry", "url", req.CallbackURL, "error", err)
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

	entries, err := s.regStore.Get(txid)
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
