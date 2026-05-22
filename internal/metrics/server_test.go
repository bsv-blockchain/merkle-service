package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/config"
)

// TestServer_ServesMetrics spins up the metrics server on an ephemeral port
// and asserts that /metrics returns text-format Prometheus output containing
// the expected series names from each subsystem.
func TestServer_ServesMetrics(t *testing.T) {
	port := freePort(t)
	srv := NewServer(config.MetricsConfig{
		Enabled: true,
		Port:    port,
		Path:    "/metrics",
	}, nil)
	if err := srv.Init(nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	//nolint:staticcheck // Start ignores its ctx arg; test driver passes nil
	if err := srv.Start(nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	// Drive one observation per subsystem so the metric is present in /metrics
	// output even on a cold registry.
	SubtreeMessagesTotal.WithLabelValues(OutcomeProcessed).Inc()
	CallbackMessagesTotal.WithLabelValues(OutcomeDelivered).Inc()
	ObserveKafkaProduce("test-topic", 100, 1*time.Millisecond, nil)
	ObserveBumpBuild(1*time.Millisecond, 4, 1, 2, false)

	url := "http://127.0.0.1:" + strconv.Itoa(port) + "/metrics"
	body := mustGetWithRetry(t, url, 3*time.Second)

	wanted := []string{
		"merkle_subtree_messages_total",
		"merkle_callback_messages_total",
		"merkle_kafka_messages_produced_total",
		"merkle_bump_build_duration_seconds",
		"go_goroutines",
		"process_resident_memory_bytes",
	}
	for _, name := range wanted {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics output missing %q", name)
		}
	}
}

// TestServer_DisabledIsNoop verifies the Enabled=false branch doesn't bind
// to a port and Init/Start/Stop succeed silently.
func TestServer_DisabledIsNoop(t *testing.T) {
	srv := NewServer(config.MetricsConfig{Enabled: false}, nil)
	if err := srv.Init(nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	//nolint:staticcheck // Start ignores its ctx arg; test driver passes nil
	if err := srv.Start(nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if srv.IsStarted() {
		t.Error("disabled server should not report started")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// mustGetWithRetry GETs url, retrying briefly while the server is still
// coming up in its goroutine. The metrics server starts via
// http.ListenAndServe in a goroutine so the test can race the bind.
func mustGetWithRetry(t *testing.T, url string, total time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(total)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			defer func() { _ = resp.Body.Close() }()
			b, rerr := io.ReadAll(resp.Body)
			if rerr != nil {
				t.Fatalf("read body: %v", rerr)
			}
			return string(b)
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("GET %s never succeeded: %v", url, lastErr)
	return ""
}
