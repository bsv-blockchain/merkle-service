package metrics

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestHostLabel(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", "unknown"},
		{"   ", "unknown"},
		{"localhost", "localhost"},
		{"LOCALHOST", "localhost"},
		{"http://example.com", "example.com"},
		{"https://Example.COM:8080/path?a=b", "example.com"},
		{"http://user:pw@host.example:443/p", "host.example"},
		{"host:8080", "host"},
		{"http://[::1]:9090/foo", "::1"},
		{"http://[2001:db8::1]:80", "2001:db8::1"},
		{"http:// bad host /x", "unknown"},
		{strings.Repeat("a", 5000), "unknown"},
		{"http://" + strings.Repeat("a", 300) + ".example.com", "unknown"},
		{"unix:///run/foo.sock", "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := HostLabel(tc.in)
			if got != tc.want {
				t.Errorf("HostLabel(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestStatusClass(t *testing.T) {
	cases := []struct {
		name string
		code int
		err  error
		want string
	}{
		{"200", 200, nil, "2xx"},
		{"302", 302, nil, "3xx"},
		{"404", 404, nil, "4xx"},
		{"500", 500, nil, "5xx"},
		{"599", 599, nil, "5xx"},
		{"deadline-exceeded", 0, context.DeadlineExceeded, "timeout"},
		{"plain-error", 0, errors.New("boom"), "error"},
		{"net-timeout", 0, &fakeTimeoutError{}, "timeout"},
		{"unknown-code", 100, nil, "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := StatusClass(tc.code, tc.err)
			if got != tc.want {
				t.Errorf("StatusClass(%d, %v) = %q, want %q", tc.code, tc.err, got, tc.want)
			}
		})
	}
}

type fakeTimeoutError struct{}

func (e *fakeTimeoutError) Error() string { return "i/o timeout" }
func (e *fakeTimeoutError) Timeout() bool { return true }
func (e *fakeTimeoutError) Temporary() bool {
	return false
}

// Compile-time check that fakeTimeoutError satisfies net.Error so the
// errors.As branch in isTimeoutErr exercises it.
var _ net.Error = (*fakeTimeoutError)(nil)

func TestClassifyDBError(t *testing.T) {
	if got := ClassifyDBError(BackendSQL, nil); got != OutcomeSuccess {
		t.Errorf("nil err: got %q, want success", got)
	}
	if got := ClassifyDBError(BackendSQL, errors.New("x")); got != OutcomeError {
		t.Errorf("plain err: got %q, want error", got)
	}
	if got := ClassifyDBError(BackendSQL, context.DeadlineExceeded); got != OutcomeTimeout {
		t.Errorf("deadline: got %q, want timeout", got)
	}
}

// TestStatusClass_NetTimeout asserts that a real net.Error timeout (wrapped
// in the deadline-exceeded context error) classifies correctly. Acts as a
// cheap smoke test against the real time-based path.
func TestStatusClass_NetTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	<-ctx.Done()
	if got := StatusClass(0, ctx.Err()); got != "timeout" {
		t.Errorf("expected timeout, got %q", got)
	}
}
