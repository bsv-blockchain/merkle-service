package subtree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/datahub"
	"github.com/bsv-blockchain/merkle-service/internal/kafka"
)

// newPeerHealthTestProcessor builds a Processor whose DataHub client has a
// PeerHealth tracker attached, wired with mock retry/DLQ producers so
// handleMessage's failure paths are drivable end to end. The stale-404
// grace is fixed at 3600s (the shipped default); grace variation is covered
// by TestIsStaleAnnouncement and TestStale404Grace_Defaulting.
func newPeerHealthTestProcessor(t *testing.T, threshold int) (*Processor, *datahub.PeerHealth, *mockSyncProducer, *mockSyncProducer) {
	const graceSec = 3600
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := datahub.NewClient(5, 0, logger)
	ph := datahub.NewPeerHealth(threshold, 10*time.Minute)
	client.SetPeerHealth(ph)

	retryMock := &mockSyncProducer{}
	dlqMock := &mockSyncProducer{}
	p := &Processor{
		cfg: &config.Config{
			Subtree: config.SubtreeConfig{
				MaxAttempts:        3,
				RetryBackoffBaseMs: 1,
			},
			DataHub: config.DataHubConfig{
				PeerHealth: config.PeerHealthConfig{
					FailureThreshold: threshold,
					CooldownSec:      600,
					Stale404GraceSec: graceSec,
				},
			},
		},
		registrationStore: &mockRegStore{registrations: map[string][]string{}},
		seenCounterStore:  &mockSeenCounter{},
		retryProducer:     kafka.NewTestProducer(retryMock, "subtree-test", logger),
		dlqProducer:       kafka.NewTestProducer(dlqMock, "subtree-dlq-test", logger),
		dataHubClient:     client,
	}
	p.InitBase("subtree-peerhealth-test")
	p.Logger = logger
	return p, ph, retryMock, dlqMock
}

// TestIsStaleAnnouncement pins the age classification used to decide
// whether a 404 is attributed to the peer: strictly older than the grace is
// stale; exactly at the grace, younger, or unstamped (zero/negative — any
// message produced before AnnouncedAtUnixMs existed) is fresh.
func TestIsStaleAnnouncement(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	grace := time.Hour

	cases := []struct {
		name        string
		announcedAt int64
		want        bool
	}{
		{"zero stamp is fresh (age unknown)", 0, false},
		{"negative stamp is fresh (age unknown)", -5, false},
		{"younger than grace is fresh", now.Add(-30 * time.Minute).UnixMilli(), false},
		{"exactly at grace is fresh", now.Add(-time.Hour).UnixMilli(), false},
		{"one ms past grace is stale", now.Add(-time.Hour).Add(-time.Millisecond).UnixMilli(), true},
		{"two hours old is stale", now.Add(-2 * time.Hour).UnixMilli(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isStaleAnnouncement(tc.announcedAt, now, grace); got != tc.want {
				t.Errorf("isStaleAnnouncement(%d) = %v, want %v", tc.announcedAt, got, tc.want)
			}
		})
	}
}

// TestRecordPeerFetchOutcome_Classification drives every branch of the
// processor-side recording that replaces the client's blanket "any error is
// a peer failure" rule for the subtree fetch path.
func TestRecordPeerFetchOutcome_Classification(t *testing.T) {
	url := "https://classify.example.com/api"
	canceledCtx := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}

	cases := []struct {
		name        string
		ctx         context.Context
		announcedAt int64
		fetchErr    error
		wantHealthy bool // with threshold 1, "healthy" == nothing was recorded
	}{
		{
			name:        "canceled ctx with error records nothing",
			ctx:         canceledCtx(),
			fetchErr:    errors.New("context canceled"),
			wantHealthy: true,
		},
		{
			name:        "stale-announcement 404 records nothing",
			ctx:         context.Background(),
			announcedAt: time.Now().Add(-2 * time.Hour).UnixMilli(),
			fetchErr:    fmt.Errorf("wrapped: %w", datahub.ErrNotFound),
			wantHealthy: true,
		},
		{
			name:        "fresh 404 counts against the peer",
			ctx:         context.Background(),
			announcedAt: time.Now().UnixMilli(),
			fetchErr:    fmt.Errorf("wrapped: %w", datahub.ErrNotFound),
			wantHealthy: false,
		},
		{
			name:        "unstamped 404 counts against the peer",
			ctx:         context.Background(),
			announcedAt: 0,
			fetchErr:    fmt.Errorf("wrapped: %w", datahub.ErrNotFound),
			wantHealthy: false,
		},
		{
			name:        "stale announcement with a transport error still counts",
			ctx:         context.Background(),
			announcedAt: time.Now().Add(-2 * time.Hour).UnixMilli(),
			fetchErr:    errors.New("connection refused"),
			wantHealthy: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, ph, _, _ := newPeerHealthTestProcessor(t, 1)
			msg := &kafka.SubtreeMessage{
				Hash:              "h1",
				DataHubURL:        url,
				AnnouncedAtUnixMs: tc.announcedAt,
			}
			p.recordPeerFetchOutcome(tc.ctx, msg, tc.fetchErr)
			if got := ph.IsHealthy(url); got != tc.wantHealthy {
				t.Errorf("IsHealthy = %v, want %v", got, tc.wantHealthy)
			}
		})
	}
}

// TestRecordPeerFetchOutcome_SuccessAndCanceledSuccess: a success resets the
// counter, but a success observed under a dead ctx records nothing (it must
// not reset a genuinely failing peer's counter).
func TestRecordPeerFetchOutcome_SuccessAndCanceledSuccess(t *testing.T) {
	url := "https://classify-success.example.com/api"
	p, ph, _, _ := newPeerHealthTestProcessor(t, 3)
	msg := &kafka.SubtreeMessage{Hash: "h1", DataHubURL: url, AnnouncedAtUnixMs: time.Now().UnixMilli()}

	live := context.Background()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	// Two live failures, a live success: counter reset, two more failures
	// don't trip.
	transport := errors.New("connection refused")
	p.recordPeerFetchOutcome(live, msg, transport)
	p.recordPeerFetchOutcome(live, msg, transport)
	p.recordPeerFetchOutcome(live, msg, nil)
	p.recordPeerFetchOutcome(live, msg, transport)
	p.recordPeerFetchOutcome(live, msg, transport)
	if !ph.IsHealthy(url) {
		t.Fatal("a live success must reset the consecutive-failure counter")
	}

	// A canceled-ctx success must NOT reset: the third live failure trips.
	p.recordPeerFetchOutcome(canceled, msg, nil)
	p.recordPeerFetchOutcome(live, msg, transport)
	if ph.IsHealthy(url) {
		t.Fatal("a canceled-ctx success must not reset the failure counter")
	}
}

// TestHandleMessage_Stale404sDoNotOpenBreaker is the incident regression: a
// stream of announcements aged past the grace, all 404ing (teranode pruned
// them from its asset cache long ago), must route to the DLQ as before but
// must NOT open the peer-health breaker — pre-fix, every cooldown expiry was
// followed by three stale 404s re-opening it, keeping the pre-store path
// dead in a single-peer topology.
func TestHandleMessage_Stale404sDoNotOpenBreaker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	p, ph, retryMock, dlqMock := newPeerHealthTestProcessor(t, 3)
	stale := time.Now().Add(-2 * time.Hour).UnixMilli()

	const n = 10
	for i := 0; i < n; i++ {
		msg := &kafka.SubtreeMessage{
			Hash:              fmt.Sprintf("stale-%02d", i),
			DataHubURL:        server.URL,
			AnnouncedAtUnixMs: stale,
		}
		value, err := msg.Encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if err := p.handleMessage(context.Background(), &kafka.Message{Value: value}); err != nil {
			t.Fatalf("handleMessage %d: %v", i, err)
		}
	}

	if !ph.IsHealthy(server.URL) {
		t.Fatalf("%d sequential stale-404 messages must not open the breaker", n)
	}
	// The messages themselves are still permanent failures: straight to DLQ,
	// no retry budget burned.
	if got := len(dlqMock.getMessages()); got != n {
		t.Errorf("expected %d DLQ publishes, got %d", n, got)
	}
	if got := len(retryMock.getMessages()); got != 0 {
		t.Errorf("expected 0 retry publishes, got %d", got)
	}
}

// TestHandleMessage_Fresh404sStillOpenBreaker pins the unchanged half of the
// contract: a peer 404ing on FRESH announcements is lying about data it
// claims to serve — the breaker must still open at the threshold, and once
// open the IsHealthy gate must ack-and-drop without touching the DLQ.
func TestHandleMessage_Fresh404sStillOpenBreaker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	p, ph, _, dlqMock := newPeerHealthTestProcessor(t, 3)

	for i := 0; i < 3; i++ {
		msg := &kafka.SubtreeMessage{
			Hash:              fmt.Sprintf("fresh-%02d", i),
			DataHubURL:        server.URL,
			AnnouncedAtUnixMs: time.Now().UnixMilli(),
		}
		value, err := msg.Encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if err := p.handleMessage(context.Background(), &kafka.Message{Value: value}); err != nil {
			t.Fatalf("handleMessage %d: %v", i, err)
		}
	}
	if ph.IsHealthy(server.URL) {
		t.Fatal("three fresh 404s must open the breaker")
	}
	if got := len(dlqMock.getMessages()); got != 3 {
		t.Errorf("expected 3 DLQ publishes, got %d", got)
	}

	// Fourth message: skipped at the IsHealthy gate (ack-and-drop), no new
	// DLQ entry, no error returned.
	msg := &kafka.SubtreeMessage{
		Hash:              "fresh-gated",
		DataHubURL:        server.URL,
		AnnouncedAtUnixMs: time.Now().UnixMilli(),
	}
	value, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := p.handleMessage(context.Background(), &kafka.Message{Value: value}); err != nil {
		t.Fatalf("handleMessage gated: %v", err)
	}
	if got := len(dlqMock.getMessages()); got != 3 {
		t.Errorf("gated message must not reach the DLQ; expected 3 publishes, got %d", got)
	}
}

// TestStale404Grace_Defaulting: a nil cfg (struct-literal test processors)
// and a zero/negative configured grace both select the built-in default
// rather than disabling the suppression with a zero grace (which would
// classify EVERY stamped 404 as stale).
func TestStale404Grace_Defaulting(t *testing.T) {
	p := &Processor{}
	if got := p.stale404Grace(); got != datahub.DefaultStale404Grace {
		t.Errorf("nil cfg: expected default grace %s, got %s", datahub.DefaultStale404Grace, got)
	}

	p = &Processor{cfg: &config.Config{}}
	if got := p.stale404Grace(); got != datahub.DefaultStale404Grace {
		t.Errorf("zero grace: expected default grace %s, got %s", datahub.DefaultStale404Grace, got)
	}

	p = &Processor{cfg: &config.Config{DataHub: config.DataHubConfig{
		PeerHealth: config.PeerHealthConfig{Stale404GraceSec: 900},
	}}}
	if got := p.stale404Grace(); got != 15*time.Minute {
		t.Errorf("configured grace: expected 15m, got %s", got)
	}
}
