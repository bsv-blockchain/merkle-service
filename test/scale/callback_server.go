//go:build scale

package scale

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// callbackPayload mirrors the JSON body delivered by the callback service (Arcade's CallbackMessage).
type callbackPayload struct {
	Type         string   `json:"type"`
	TxID         string   `json:"txid,omitempty"`
	TxIDs        []string `json:"txids,omitempty"`
	BlockHash    string   `json:"blockHash,omitempty"`
	SubtreeIndex int      `json:"subtreeIndex,omitempty"`
	// Stump is kept as raw JSON: the ~1MB hex string would otherwise be
	// unquoted + copied on every POST inside the mock subscriber, charging
	// the pipeline under test for harness-side decode CPU (profiled at ~20%
	// of total samples). Verification decodes it lazily via StumpHex.
	Stump json.RawMessage `json:"stump,omitempty"`
}

// CallbackServer is an HTTP server that collects callback payloads for one Arcade instance.
type CallbackServer struct {
	port     int
	server   *http.Server
	listener net.Listener

	mu             sync.Mutex
	minedRaw       [][]byte // raw STUMP bodies; decoded lazily by MinedPayloads
	minedDecoded   []callbackPayload
	seenPayloads   []callbackPayload
	blockProcessed []callbackPayload
	rawBytes       int64
	firstCallback  time.Time
	lastCallback   time.Time
	totalCallbacks atomic.Int64
}

// NewCallbackServer creates a callback server on the specified port.
func NewCallbackServer(port int) *CallbackServer {
	cs := &CallbackServer{port: port}
	mux := http.NewServeMux()
	mux.HandleFunc("/callback", cs.handleCallback)
	cs.server = &http.Server{
		Handler: mux,
	}
	return cs
}

// payloadType sniffs the "type" field without parsing the body. The delivery
// service marshals callbackPayload with Type as the FIRST field and Go's
// encoding/json preserves struct order, so every body begins {"type":"...".
// Full json.Unmarshal here costs a complete validation scan of ~1MB STUMP
// bodies per POST (profiled at ~25% of ALL benchmark CPU, paid on the
// pipeline's latency path) — a real subscriber pays that on its own machine,
// so the mock must not charge it to the pipeline under test.
func payloadType(body []byte) string {
	const prefix = `{"type":"`
	if len(body) < len(prefix) || string(body[:len(prefix)]) != prefix {
		return ""
	}
	rest := body[len(prefix):]
	end := bytes.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return string(rest[:end])
}

func (cs *CallbackServer) handleCallback(w http.ResponseWriter, r *http.Request) {
	// Pre-size from Content-Length: io.ReadAll's grow-and-copy doubling on
	// ~1MB bodies showed up as measurable memmove/allocator churn inside the
	// throughput window.
	var body []byte
	var err error
	if r.ContentLength > 0 {
		body = make([]byte, r.ContentLength)
		_, err = io.ReadFull(r.Body, body)
	} else {
		body, err = io.ReadAll(r.Body)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	typ := payloadType(body)

	now := time.Now()
	cs.mu.Lock()
	if cs.firstCallback.IsZero() {
		cs.firstCallback = now
	}
	cs.lastCallback = now
	cs.rawBytes += int64(len(body))

	switch typ {
	case "STUMP":
		// Hot path stores the raw body only; MinedPayloads decodes lazily at
		// verification time, off the throughput clock.
		cs.minedRaw = append(cs.minedRaw, body)
	case "SEEN_ON_NETWORK", "SEEN_MULTIPLE_NODES":
		// SEEN payloads are small (txid-list chunks); decode inline so the
		// phase-A wait can sum txids live.
		var p callbackPayload
		_ = json.Unmarshal(body, &p)
		cs.seenPayloads = append(cs.seenPayloads, p)
	case "BLOCK_PROCESSED":
		var p callbackPayload
		_ = json.Unmarshal(body, &p)
		cs.blockProcessed = append(cs.blockProcessed, p)
	}
	cs.mu.Unlock()
	cs.totalCallbacks.Add(1)

	w.WriteHeader(http.StatusOK)
}

// StumpHex returns the payload's stump as a hex string, decoding the raw JSON
// string token lazily (verification-time only — never on the hot receive path).
func (p *callbackPayload) StumpHex() (string, error) {
	if len(p.Stump) == 0 {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(p.Stump, &s); err != nil {
		return "", err
	}
	return s, nil
}

// SeenCounts returns the number of SEEN-type payloads received and the total
// txids they enumerated (batched SEEN callbacks carry a txid list).
func (cs *CallbackServer) SeenCounts() (payloads, txids int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, p := range cs.seenPayloads {
		txids += len(p.TxIDs)
	}
	return len(cs.seenPayloads), txids
}

// Start begins listening.
func (cs *CallbackServer) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cs.port))
	if err != nil {
		return fmt.Errorf("listen on port %d: %w", cs.port, err)
	}
	cs.listener = ln
	go cs.server.Serve(ln)
	return nil
}

// Stop shuts down the server.
func (cs *CallbackServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cs.server.Shutdown(ctx)
}

// MinedPayloads returns all received MINED payloads, decoding the raw bodies
// captured on the hot path. Decoding happens here — at verification time —
// so the megabyte-scale JSON parse never executes inside the measured
// throughput window. Decoded results are cached.
func (cs *CallbackServer) MinedPayloads() []callbackPayload {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for i := len(cs.minedDecoded); i < len(cs.minedRaw); i++ {
		var p callbackPayload
		_ = json.Unmarshal(cs.minedRaw[i], &p)
		cs.minedDecoded = append(cs.minedDecoded, p)
	}
	result := make([]callbackPayload, len(cs.minedDecoded))
	copy(result, cs.minedDecoded)
	return result
}

// BlockProcessedPayloads returns a copy of all received BLOCK_PROCESSED payloads.
func (cs *CallbackServer) BlockProcessedPayloads() []callbackPayload {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	result := make([]callbackPayload, len(cs.blockProcessed))
	copy(result, cs.blockProcessed)
	return result
}

// Stats returns server statistics.
func (cs *CallbackServer) Stats() ServerStats {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	totalTxids := 0 // batched STUMP payloads carry no txid list; SEEN payloads do
	for _, p := range cs.seenPayloads {
		totalTxids += len(p.TxIDs)
	}
	return ServerStats{
		Port:           cs.port,
		MinedCallbacks: len(cs.minedRaw),
		BlockProcessed: len(cs.blockProcessed),
		TotalTxids:     totalTxids,
		TotalBytes:     cs.rawBytes,
		FirstCallback:  cs.firstCallback,
		LastCallback:   cs.lastCallback,
		TotalCallbacks: cs.totalCallbacks.Load(),
	}
}

// ServerStats holds statistics for one callback server.
type ServerStats struct {
	Port           int
	MinedCallbacks int
	BlockProcessed int
	TotalTxids     int
	TotalBytes     int64
	FirstCallback  time.Time
	LastCallback   time.Time
	TotalCallbacks int64
}

// CallbackFleet manages multiple callback servers.
type CallbackFleet struct {
	servers []*CallbackServer
}

// NewCallbackFleet creates a fleet of callback servers on sequential ports.
func NewCallbackFleet(basePort, count int) *CallbackFleet {
	servers := make([]*CallbackServer, count)
	for i := 0; i < count; i++ {
		servers[i] = NewCallbackServer(basePort + i)
	}
	return &CallbackFleet{servers: servers}
}

// StartAll starts all servers in the fleet.
func (f *CallbackFleet) StartAll() error {
	for i, s := range f.servers {
		if err := s.Start(); err != nil {
			// Clean up already-started servers.
			for j := 0; j < i; j++ {
				f.servers[j].Stop()
			}
			return fmt.Errorf("starting server %d: %w", i, err)
		}
	}
	return nil
}

// StopAll stops all servers in the fleet.
func (f *CallbackFleet) StopAll() {
	for _, s := range f.servers {
		s.Stop()
	}
}

// GetServer returns the server at the given index.
func (f *CallbackFleet) GetServer(index int) *CallbackServer {
	return f.servers[index]
}

// TotalCallbacks returns the sum of all callbacks across all servers.
func (f *CallbackFleet) TotalCallbacks() int64 {
	var total int64
	for _, s := range f.servers {
		total += s.totalCallbacks.Load()
	}
	return total
}

// Count returns the number of servers in the fleet.
func (f *CallbackFleet) Count() int {
	return len(f.servers)
}
