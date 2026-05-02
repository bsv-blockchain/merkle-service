//go:build scale

package scale

import (
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
	Type         string `json:"type"`
	TxID         string `json:"txid,omitempty"`
	BlockHash    string `json:"blockHash,omitempty"`
	SubtreeIndex int    `json:"subtreeIndex,omitempty"`
	Stump        string `json:"stump,omitempty"`
}

// CallbackServer is an HTTP server that collects callback payloads for one Arcade instance.
type CallbackServer struct {
	port     int
	server   *http.Server
	listener net.Listener

	mu             sync.Mutex
	minedPayloads  []callbackPayload
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

func (cs *CallbackServer) handleCallback(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var p callbackPayload
	_ = json.Unmarshal(body, &p)

	now := time.Now()
	cs.mu.Lock()
	if cs.firstCallback.IsZero() {
		cs.firstCallback = now
	}
	cs.lastCallback = now
	cs.rawBytes += int64(len(body))

	switch p.Type {
	case "STUMP":
		cs.minedPayloads = append(cs.minedPayloads, p)
	case "BLOCK_PROCESSED":
		cs.blockProcessed = append(cs.blockProcessed, p)
	}
	cs.mu.Unlock()
	cs.totalCallbacks.Add(1)

	w.WriteHeader(http.StatusOK)
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

// MinedPayloads returns a copy of all received MINED payloads.
func (cs *CallbackServer) MinedPayloads() []callbackPayload {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	result := make([]callbackPayload, len(cs.minedPayloads))
	copy(result, cs.minedPayloads)
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
	totalTxids := len(cs.minedPayloads) // one txid per STUMP callback
	return ServerStats{
		Port:           cs.port,
		MinedCallbacks: len(cs.minedPayloads),
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
