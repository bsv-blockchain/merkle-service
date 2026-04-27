package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"

	"github.com/go-chi/chi/v5"
)

var txidRegex = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(dashboardHTML)
}

// WatchRequest represents the POST /watch request body.
type WatchRequest struct {
	TxID        string `json:"txid"`
	CallbackURL string `json:"callbackUrl"`
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

	// Validate callback URL
	if req.CallbackURL == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "callbackUrl is required"})
		return
	}
	parsedURL, err := url.Parse(req.CallbackURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid callbackUrl: must be a valid HTTP/HTTPS URL"})
		return
	}

	// Store registration
	if err := s.regStore.Add(req.TxID, req.CallbackURL); err != nil {
		s.Logger.Error("failed to add registration", "txid", req.TxID, "error", err)
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
		return
	}

	// Register callback URL in the broadcast registry.
	if s.urlRegistry != nil {
		if err := s.urlRegistry.Add(req.CallbackURL); err != nil {
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

	urls, err := s.regStore.Get(txid)
	if err != nil {
		s.Logger.Error("failed to lookup registration", "txid", txid, "error", err)
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
		return
	}

	if urls == nil {
		urls = []string{}
	}

	writeJSON(w, http.StatusOK, LookupResponse{
		TxID:         txid,
		CallbackUrls: urls,
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
