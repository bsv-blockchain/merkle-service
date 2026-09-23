package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestLogLevel(t *testing.T) {
	tests := []struct {
		status int
		want   slog.Level
	}{
		// 0 = handler never wrote a header or body; net/http sends 200.
		{0, slog.LevelDebug},
		{http.StatusOK, slog.LevelDebug},
		{http.StatusNoContent, slog.LevelDebug},
		{http.StatusMovedPermanently, slog.LevelDebug},
		{http.StatusBadRequest, slog.LevelInfo},
		{http.StatusUnauthorized, slog.LevelInfo},
		{http.StatusNotFound, slog.LevelInfo},
		{http.StatusTooManyRequests, slog.LevelInfo},
		{http.StatusInternalServerError, slog.LevelError},
		{http.StatusServiceUnavailable, slog.LevelError},
	}
	for _, tc := range tests {
		if got := requestLogLevel(tc.status); got != tc.want {
			t.Errorf("requestLogLevel(%d) = %v, want %v", tc.status, got, tc.want)
		}
	}
}

// TestMiddlewareLogger_LevelByStatus asserts the access-log line is emitted at
// a level derived from the response status, so the 2xx path — already covered
// by merkle_http_requests_total / merkle_http_request_duration_seconds — stays
// out of the Info-level ingest path.
func TestMiddlewareLogger_LevelByStatus(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   string
	}{
		{"success is debug", http.StatusOK, "DEBUG"},
		{"unwritten status is debug", 0, "DEBUG"},
		{"client error is info", http.StatusTooManyRequests, "INFO"},
		{"server error is error", http.StatusInternalServerError, "ERROR"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			rec := httptest.NewRecorder()
			serveStatus(t, logger, rec, tc.status)

			line := requestLogLine(t, &buf)
			if line == nil {
				t.Fatalf("no %q log line captured; got: %s", "request", buf.String())
			}
			if got := line["level"]; got != tc.want {
				t.Errorf("level = %v, want %v", got, tc.want)
			}
			if got := line["status"]; got != float64(tc.status) {
				t.Errorf("status = %v, want %v", got, tc.status)
			}
		})
	}
}

// TestMiddlewareLogger_SuccessSuppressedAtInfo is the volume guarantee behind
// issue #235: at the default level, a successful request must cost no log
// record at all.
func TestMiddlewareLogger_SuccessSuppressedAtInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	serveStatus(t, logger, httptest.NewRecorder(), http.StatusOK)

	if got := strings.TrimSpace(buf.String()); got != "" {
		t.Errorf("expected no log output for a 200 at Info level, got: %s", got)
	}
}

// serveStatus drives middlewareLogger over a handler that writes the given
// status. A status of 0 means the handler writes nothing at all.
func serveStatus(t *testing.T, logger *slog.Logger, rec *httptest.ResponseRecorder, status int) {
	t.Helper()
	h := middlewareLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if status != 0 {
			w.WriteHeader(status)
		}
	}))
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/watch", nil))
}

// requestLogLine returns the last "request" record in buf, or nil if there is
// none.
func requestLogLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var last map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal log line %q: %v", line, err)
		}
		if m["msg"] == "request" {
			last = m
		}
	}
	return last
}
