package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/store"
)

var txidRegex = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

// Handlers holds dependencies for HTTP handlers.
type Handlers struct {
	callbackStore *CallbackStore
	txidTracker   *TxidTracker
	regStore      store.RegistrationStore
	templates     map[string]*template.Template
	merkleAPIURL  string
	callbackURL   string
	logger        *slog.Logger
}

// pageData is the common data passed to templates.
type pageData struct {
	Title         string
	TxidCount     int
	CallbackCount int
	CallbackURL   string
	Flash         string
	FlashError    string
}

func (h *Handlers) newPageData(title string) pageData {
	return pageData{
		Title:         title,
		TxidCount:     h.txidTracker.Count(),
		CallbackCount: h.callbackStore.Count(),
		CallbackURL:   h.callbackURL,
	}
}

// homeData is the data struct for all handlers that render home.html.
type homeData struct {
	pageData

	RecentCallbacks []CallbackEntry
	LookupTxid      string
	LookupURLs      []string
	LookupErr       string
}

func (h *Handlers) newHomeData() homeData {
	return homeData{
		pageData:        h.newPageData("Home"),
		RecentCallbacks: h.getRecentCallbacks(5),
	}
}

// handleHome renders the home page.
func (h *Handlers) handleHome(w http.ResponseWriter, r *http.Request) {
	h.render(w, "home.html", h.newHomeData())
}

func (h *Handlers) getRecentCallbacks(n int) []CallbackEntry {
	all := h.callbackStore.GetAll()
	if len(all) > n {
		return all[:n]
	}
	return all
}

// handleRegister handles txid registration via the merkle-service API.
func (h *Handlers) handleRegister(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB — debug tool, not a public endpoint
	txid := r.FormValue("txid")
	callbackURL := r.FormValue("callbackUrl")

	if !txidRegex.MatchString(txid) {
		data := h.newHomeData()
		data.FlashError = "Invalid txid: must be exactly 64 hex characters"
		h.render(w, "home.html", data)
		return
	}

	if callbackURL == "" {
		data := h.newHomeData()
		data.FlashError = "Callback URL is required"
		h.render(w, "home.html", data)
		return
	}

	// POST to merkle-service /watch
	payload := map[string]string{
		"txid":        txid,
		"callbackUrl": callbackURL,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		data := h.newHomeData()
		data.FlashError = fmt.Sprintf("Failed to encode request: %v", err)
		h.render(w, "home.html", data)
		return
	}

	resp, err := http.Post(h.merkleAPIURL+"/watch", "application/json", bytes.NewReader(body)) //nolint:noctx // debug tool, context not plumbed through form handler
	if err != nil {
		data := h.newHomeData()
		data.FlashError = fmt.Sprintf("Failed to reach merkle-service: %v", err)
		h.render(w, "home.html", data)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		data := h.newHomeData()
		data.FlashError = fmt.Sprintf("Merkle-service returned %d: %s", resp.StatusCode, string(respBody))
		h.render(w, "home.html", data)
		return
	}

	h.txidTracker.Add(txid, []string{callbackURL})
	h.logger.Info("registered txid", "txid", txid, "callbackUrl", callbackURL)

	data := h.newHomeData()
	data.Flash = fmt.Sprintf("Registered txid %s with callback URL %s", txid[:16]+"...", callbackURL)
	h.render(w, "home.html", data)
}

// handleLookup looks up a txid in Aerospike.
func (h *Handlers) handleLookup(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB — debug tool, not a public endpoint
	txid := r.FormValue("txid")

	if txid == "" {
		data := h.newHomeData()
		data.FlashError = "Please enter a txid to look up"
		h.render(w, "home.html", data)
		return
	}

	urls, err := h.regStore.Get(txid)

	data := h.newHomeData()
	data.LookupTxid = txid

	if err != nil {
		data.LookupErr = fmt.Sprintf("Error querying Aerospike: %v", err)
	} else if len(urls) == 0 {
		data.LookupErr = "No registrations found"
	} else {
		data.LookupURLs = urls
		// Track this txid if not already tracked.
		h.txidTracker.Add(txid, urls)
	}

	h.render(w, "home.html", data)
}

// handleRegistrations lists all tracked txids.
func (h *Handlers) handleRegistrations(w http.ResponseWriter, r *http.Request) {
	// Refresh callback URLs from Aerospike for all tracked txids.
	tracked := h.txidTracker.GetAll()
	for _, t := range tracked {
		urls, err := h.regStore.Get(t.Txid)
		if err == nil && len(urls) > 0 {
			h.txidTracker.UpdateCallbackURLs(t.Txid, urls)
		}
	}

	data := struct {
		pageData

		Txids []TrackedTxid
	}{
		pageData: h.newPageData("Registrations"),
		Txids:    h.txidTracker.GetAll(),
	}
	h.render(w, "registrations.html", data)
}

// handleCallbacks lists all received callbacks.
func (h *Handlers) handleCallbacks(w http.ResponseWriter, r *http.Request) {
	data := struct {
		pageData

		Callbacks []CallbackEntry
	}{
		pageData:  h.newPageData("Callbacks"),
		Callbacks: h.callbackStore.GetAll(),
	}
	h.render(w, "callbacks.html", data)
}

// handleCallbackReceive receives callbacks from the merkle-service.
func (h *Handlers) handleCallbackReceive(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		// Store raw even if not valid JSON.
		parsed = map[string]interface{}{"_raw": string(body)}
	}

	entry := CallbackEntry{
		Timestamp: time.Now(),
		Body:      parsed,
		RawJSON:   string(body),
	}
	h.callbackStore.Add(entry)

	h.logger.Info("received callback",
		"status", parsed["status"],
		"txid", parsed["txid"],
	)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleStump renders the STUMP visualizer page.
func (h *Handlers) handleStump(w http.ResponseWriter, r *http.Request) {
	h.render(w, "stump.html", h.newPageData("STUMP Visualizer"))
}

// handleCallbacksClear clears all stored callbacks.
func (h *Handlers) handleCallbacksClear(w http.ResponseWriter, r *http.Request) {
	h.callbackStore.Clear()
	http.Redirect(w, r, "/callbacks", http.StatusSeeOther)
}

func (h *Handlers) render(w http.ResponseWriter, name string, data interface{}) {
	tmpl, ok := h.templates[name]
	if !ok {
		h.logger.Error("template not found", "template", name)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "layout", data); err != nil {
		h.logger.Error("template render error", "template", name, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
