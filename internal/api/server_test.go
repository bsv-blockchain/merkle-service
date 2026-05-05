package api

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/bsv-blockchain/merkle-service/internal/config"
)

// TestStart_BindErrorIsReturnedSynchronously verifies that Start returns a
// non-nil error when the configured port cannot be bound (e.g. it is already
// in use). Prior to the fix for #33 the listen call happened inside a
// goroutine and Start always returned nil, so callers had no way to detect
// bind failures.
func TestStart_BindErrorIsReturnedSynchronously(t *testing.T) {
	// Occupy an ephemeral port first so we know it is unavailable.
	occupier, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy a port: %v", err)
	}
	defer occupier.Close()

	port := occupier.Addr().(*net.TCPAddr).Port

	srv := NewServer(config.APIConfig{Port: port}, nil, nil, nil, slog.Default())
	if err := srv.Init(nil); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	// Force the bind to target the loopback address actually held by the
	// occupier so that the conflict is deterministic across hosts whose
	// default interface might differ from 127.0.0.1.
	srv.httpServer.Addr = occupier.Addr().String()

	err = srv.Start(context.Background())
	if err == nil {
		// If the goroutine somehow won, make sure the server is shut down.
		_ = srv.Stop()
		t.Fatalf("expected Start to return a bind error, got nil")
	}
	if !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("expected wrapped listen error, got %v", err)
	}
}

// TestStart_SucceedsOnFreePort verifies the happy path: a free port binds
// synchronously and Start returns nil. Stop should then cleanly shut down
// the running goroutine.
func TestStart_SucceedsOnFreePort(t *testing.T) {
	srv := NewServer(config.APIConfig{Port: 0}, nil, nil, nil, slog.Default())
	if err := srv.Init(nil); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	// Port 0 lets the kernel assign a free port.
	srv.httpServer.Addr = "127.0.0.1:0"

	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop returned unexpected error: %v", err)
	}
}
