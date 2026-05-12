package p2p

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	teranode "github.com/bsv-blockchain/teranode/services/p2p"
)

func TestPickDataHubURL(t *testing.T) {
	cases := []struct {
		name string
		msg  teranode.NodeStatusMessage
		want string
	}{
		{
			"both set prefers propagation",
			teranode.NodeStatusMessage{BaseURL: "https://base.example.com", PropagationURL: "https://prop.example.com"},
			"https://prop.example.com",
		},
		{
			"only base",
			teranode.NodeStatusMessage{BaseURL: "https://base.example.com"},
			"https://base.example.com",
		},
		{
			"only propagation",
			teranode.NodeStatusMessage{PropagationURL: "https://prop.example.com"},
			"https://prop.example.com",
		},
		{
			"both empty",
			teranode.NodeStatusMessage{},
			"",
		},
		{
			"propagation whitespace falls back to base",
			teranode.NodeStatusMessage{BaseURL: "https://base.example.com", PropagationURL: "   "},
			"https://base.example.com",
		},
		{
			"trims base whitespace",
			teranode.NodeStatusMessage{BaseURL: "  https://base.example.com  "},
			"https://base.example.com",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickDataHubURL(tc.msg); got != tc.want {
				t.Errorf("pickDataHubURL = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNormalizeDataHubURL(t *testing.T) {
	cases := map[string]string{
		"https://x.example.com":   "https://x.example.com",
		"https://x.example.com/":  "https://x.example.com",
		"  https://x.example.com": "https://x.example.com",
		"":                        "",
	}
	for in, want := range cases {
		if got := normalizeDataHubURL(in); got != want {
			t.Errorf("normalizeDataHubURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// fakeDataHubRegistry captures Add calls so tests can assert which URLs
// the node_status handler upserts. Mirrors the shape of
// store.DataHubRegistry but lives in this package to avoid a test-only
// dependency on the store package.
type fakeDataHubRegistry struct {
	mu     sync.Mutex
	added  []string
	addErr error
	all    []string
	allErr error
}

func (f *fakeDataHubRegistry) Add(url string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.addErr != nil {
		return f.addErr
	}
	f.added = append(f.added, url)
	return nil
}

func (f *fakeDataHubRegistry) GetAll() ([]string, error) {
	return f.all, f.allErr
}

func (f *fakeDataHubRegistry) takeAdded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.added))
	copy(out, f.added)
	return out
}

// newTestClientWithRegistry constructs a Client with allowPrivateIPs=true
// so httptest URLs (127.0.0.1) survive ssrfguard.ValidateURL. The
// alternative — real public hostnames — would require DNS access during
// tests, which is not hermetic. allowPrivateIPs=false rejection is
// covered by its own dedicated test that uses an IP literal.
func newTestClientWithRegistry(t *testing.T, reg *fakeDataHubRegistry, allowPrivate bool) *Client {
	t.Helper()
	c := &Client{
		dataHubRegistry: reg,
		allowPrivateIPs: allowPrivate,
	}
	c.InitBase("p2p-client-test")
	c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return c
}

func TestHandleNodeStatusMessage_RegistersValidURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	reg := &fakeDataHubRegistry{}
	c := newTestClientWithRegistry(t, reg, true)

	c.handleNodeStatusMessage(teranode.NodeStatusMessage{
		PeerID:  "peer-1",
		BaseURL: srv.URL,
	})

	added := reg.takeAdded()
	if len(added) != 1 || added[0] != srv.URL {
		t.Fatalf("expected one Add(%q), got %v", srv.URL, added)
	}
	if got := c.nodeStatusReceived.Load(); got != 1 {
		t.Errorf("expected nodeStatusReceived=1, got %d", got)
	}
}

func TestHandleNodeStatusMessage_PrefersPropagationURL(t *testing.T) {
	prop := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer prop.Close()
	base := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer base.Close()

	reg := &fakeDataHubRegistry{}
	c := newTestClientWithRegistry(t, reg, true)

	c.handleNodeStatusMessage(teranode.NodeStatusMessage{
		PeerID:         "peer-2",
		BaseURL:        base.URL,
		PropagationURL: prop.URL,
	})

	added := reg.takeAdded()
	if len(added) != 1 || added[0] != prop.URL {
		t.Fatalf("expected PropagationURL %q registered, got %v", prop.URL, added)
	}
}

func TestHandleNodeStatusMessage_StripsTrailingSlash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	reg := &fakeDataHubRegistry{}
	c := newTestClientWithRegistry(t, reg, true)

	c.handleNodeStatusMessage(teranode.NodeStatusMessage{
		PeerID:  "peer-3",
		BaseURL: srv.URL + "/",
	})

	added := reg.takeAdded()
	if len(added) != 1 || added[0] != srv.URL {
		t.Fatalf("expected trailing slash stripped to %q, got %v", srv.URL, added)
	}
	if strings.HasSuffix(added[0], "/") {
		t.Errorf("registered URL must not end with /, got %q", added[0])
	}
}

func TestHandleNodeStatusMessage_SkipsEmptyURL(t *testing.T) {
	reg := &fakeDataHubRegistry{}
	c := newTestClientWithRegistry(t, reg, true)

	c.handleNodeStatusMessage(teranode.NodeStatusMessage{PeerID: "peer-4"})

	if got := reg.takeAdded(); len(got) != 0 {
		t.Fatalf("expected no Add when URL is empty, got %v", got)
	}
	if got := c.nodeStatusReceived.Load(); got != 1 {
		t.Errorf("counter must still bump even when URL is empty; got %d", got)
	}
}

// TestHandleNodeStatusMessage_RejectsPrivateAddress uses an IP literal
// so the SSRF guard does not perform DNS resolution — keeps the test
// hermetic. allowPrivateIPs=false must reject loopback.
func TestHandleNodeStatusMessage_RejectsPrivateAddress(t *testing.T) {
	reg := &fakeDataHubRegistry{}
	c := newTestClientWithRegistry(t, reg, false)

	c.handleNodeStatusMessage(teranode.NodeStatusMessage{
		PeerID:  "peer-5",
		BaseURL: "http://127.0.0.1:8080",
	})

	if got := reg.takeAdded(); len(got) != 0 {
		t.Fatalf("expected no Add for loopback URL when allowPrivateIPs=false, got %v", got)
	}
}

func TestHandleNodeStatusMessage_RejectsBadScheme(t *testing.T) {
	reg := &fakeDataHubRegistry{}
	c := newTestClientWithRegistry(t, reg, true)

	c.handleNodeStatusMessage(teranode.NodeStatusMessage{
		PeerID:  "peer-6",
		BaseURL: "ftp://node.example.com/",
	})

	if got := reg.takeAdded(); len(got) != 0 {
		t.Fatalf("expected no Add for non-http(s) URL, got %v", got)
	}
}

// TestHandleNodeStatusMessage_NilRegistry ensures the handler is nil-
// tolerant. processNodeStatusMessages already short-circuits when the
// registry is nil, but a defensive guard inside handleNodeStatusMessage
// protects direct callers (tests, future helpers).
func TestHandleNodeStatusMessage_NilRegistry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	c := newTestClientWithRegistry(t, nil, true)
	c.dataHubRegistry = nil // explicit even though newTestClientWithRegistry already set it

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("handleNodeStatusMessage panicked with nil registry: %v", r)
		}
	}()
	c.handleNodeStatusMessage(teranode.NodeStatusMessage{
		PeerID:  "peer-7",
		BaseURL: srv.URL,
	})
}
