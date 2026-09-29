package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- validateConcurrency ---

func TestValidateConcurrency_Valid(t *testing.T) {
	for _, n := range []int{1, 2, 10, 1000} {
		if err := validateConcurrency(n); err != nil {
			t.Errorf("validateConcurrency(%d) unexpected error: %v", n, err)
		}
	}
}

func TestValidateConcurrency_Zero(t *testing.T) {
	err := validateConcurrency(0)
	if err == nil {
		t.Fatal("expected error for concurrency=0 (would deadlock on unbuffered semaphore)")
	}
	if !strings.Contains(err.Error(), "--concurrency must be >= 1") {
		t.Errorf("error message should mention '--concurrency must be >= 1', got: %v", err)
	}
}

func TestValidateConcurrency_Negative(t *testing.T) {
	for _, n := range []int{-1, -10, -1 << 20} {
		err := validateConcurrency(n)
		if err == nil {
			t.Errorf("expected error for concurrency=%d (would panic make()), got nil", n)
			continue
		}
		if !strings.Contains(err.Error(), "--concurrency must be >= 1") {
			t.Errorf("error message should mention '--concurrency must be >= 1', got: %v", err)
		}
	}
}

// --- validateTxid ---

func TestValidateTxid_Valid(t *testing.T) {
	cases := []string{
		strings.Repeat("0", 64),
		strings.Repeat("f", 64),
		strings.Repeat("A", 64),
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	}
	for _, tc := range cases {
		if err := validateTxid(tc); err != nil {
			t.Errorf("validateTxid(%q) unexpected error: %v", tc, err)
		}
	}
}

func TestValidateTxid_TooShort(t *testing.T) {
	if err := validateTxid(strings.Repeat("a", 63)); err == nil {
		t.Error("expected error for 63-char string")
	}
}

func TestValidateTxid_TooLong(t *testing.T) {
	if err := validateTxid(strings.Repeat("a", 65)); err == nil {
		t.Error("expected error for 65-char string")
	}
}

func TestValidateTxid_NonHex(t *testing.T) {
	bad := strings.Repeat("g", 64)
	if err := validateTxid(bad); err == nil {
		t.Error("expected error for non-hex character")
	}
}

func TestValidateTxid_Empty(t *testing.T) {
	if err := validateTxid(""); err == nil {
		t.Error("expected error for empty string")
	}
}

// --- loadTxids ---

func goodTxid() string  { return strings.Repeat("a", 64) }
func goodTxid2() string { return strings.Repeat("b", 64) }

func TestLoadTxids_File(t *testing.T) {
	content := goodTxid() + "\n" +
		"\n" + // blank line — skipped
		"# this is a comment\n" + // comment — skipped
		goodTxid2() + "\n"

	f, err := os.CreateTemp(t.TempDir(), "txids*.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, werr := f.WriteString(content); werr != nil {
		t.Fatal(werr)
	}
	if cerr := f.Close(); cerr != nil {
		t.Fatal(cerr)
	}

	txids, err := loadTxids(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(txids) != 2 {
		t.Fatalf("expected 2 txids, got %d", len(txids))
	}
	if txids[0] != goodTxid() || txids[1] != goodTxid2() {
		t.Errorf("unexpected txids: %v", txids)
	}
}

func TestLoadTxids_InvalidLine(t *testing.T) {
	content := goodTxid() + "\n" +
		"not-a-valid-txid\n" +
		goodTxid2() + "\n"

	f, err := os.CreateTemp(t.TempDir(), "txids*.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, werr := f.WriteString(content); werr != nil {
		t.Fatal(werr)
	}
	if cerr := f.Close(); cerr != nil {
		t.Fatal(cerr)
	}

	_, err = loadTxids(f.Name())
	if err == nil {
		t.Fatal("expected error for invalid txid in file")
	}
	if !strings.Contains(err.Error(), "not-a-valid-txid") {
		t.Errorf("error should mention the bad line, got: %v", err)
	}
}

func TestLoadTxids_Stdin(t *testing.T) {
	// Redirect stdin to a reader with two valid txids.
	content := goodTxid() + "\n" + goodTxid2() + "\n"
	origStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r
	if _, err := w.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	defer func() { os.Stdin = origStdin }()

	txids, err := loadTxids("-")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(txids) != 2 {
		t.Fatalf("expected 2 txids, got %d", len(txids))
	}
}

// --- registerOne ---

func TestRegisterOne_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/watch" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var req watchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if req.TxID != goodTxid() {
			t.Errorf("expected txid %s, got %s", goodTxid(), req.TxID)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer server.Close()

	client := &http.Client{}
	if err := registerOne(t.Context(), client, server.URL, goodTxid(), "http://cb.example/"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRegisterOne_BadRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid txid format"})
	}))
	defer server.Close()

	client := &http.Client{}
	err := registerOne(t.Context(), client, server.URL, goodTxid(), "http://cb.example/")
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention status 400, got: %v", err)
	}
}

func TestRegisterOne_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	client := &http.Client{}
	err := registerOne(t.Context(), client, server.URL, goodTxid(), "http://cb.example/")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention status 500, got: %v", err)
	}
}

// --- registerAll ---

func testTxids(n int) []string {
	txids := make([]string, n)
	for i := range txids {
		txids[i] = fmt.Sprintf("%064x", i)
	}
	return txids
}

func TestRegisterAll_ResultsInOrder(t *testing.T) {
	txids := testTxids(50)
	failing := map[string]bool{}
	for i := 1; i < len(txids); i += 2 {
		failing[txids[i]] = true
	}
	errFail := errors.New("fail")

	results := registerAll(context.Background(), txids, 4, func(_ context.Context, txid string) error {
		if failing[txid] {
			return errFail
		}
		return nil
	})

	if len(results) != len(txids) {
		t.Fatalf("got %d results, want %d", len(results), len(txids))
	}
	for i, r := range results {
		if r.txid != txids[i] {
			t.Errorf("results[%d].txid = %s, want %s", i, r.txid, txids[i])
		}
		wantErr := i%2 == 1
		if (r.err != nil) != wantErr {
			t.Errorf("results[%d].err = %v, want error: %v", i, r.err, wantErr)
		}
	}
}

func TestRegisterAll_BoundsConcurrency(t *testing.T) {
	const concurrency = 3
	txids := testTxids(200)

	var inFlight, maxInFlight atomic.Int32
	results := registerAll(context.Background(), txids, concurrency, func(context.Context, string) error {
		n := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		runtime.Gosched()
		inFlight.Add(-1)
		return nil
	})

	if len(results) != len(txids) {
		t.Fatalf("got %d results, want %d", len(results), len(txids))
	}
	if got := maxInFlight.Load(); got > concurrency {
		t.Errorf("max in-flight registrations = %d, want <= %d", got, concurrency)
	}
}

// TestRegisterAll_GoroutinesIndependentOfInput checks that a large input does
// not start one goroutine per txid while registrations are in progress.
func TestRegisterAll_GoroutinesIndependentOfInput(t *testing.T) {
	const (
		concurrency = 4
		// Headroom for unrelated runtime goroutines; far below the one
		// goroutine per txid this test guards against.
		slack = 16
	)
	txids := testTxids(10000)

	baseline := runtime.NumGoroutine()
	release := make(chan struct{})
	started := make(chan struct{}, concurrency)

	// Buffered so the goroutine can finish even if the test fails before
	// receiving from done.
	done := make(chan []result, 1)
	go func() {
		done <- registerAll(context.Background(), txids, concurrency, func(context.Context, string) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil
		})
	}()

	for range concurrency {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("workers did not start")
		}
	}
	// Keep sampling while registrations stay blocked: a per-txid loop is
	// still starting goroutines at this point, so a single sample can miss it.
	got, limit := 0, baseline+1+concurrency+slack
	for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		got = max(got, runtime.NumGoroutine())
	}
	close(release)
	if got > limit {
		t.Errorf("goroutines while blocked = %d, want <= %d", got, limit)
	}

	if results := <-done; len(results) != len(txids) {
		t.Fatalf("got %d results, want %d", len(results), len(txids))
	}
}

func TestRegisterAll_FewerTxidsThanConcurrency(t *testing.T) {
	txids := testTxids(2)
	var calls atomic.Int32

	results := registerAll(context.Background(), txids, 10, func(context.Context, string) error {
		calls.Add(1)
		return nil
	})

	if len(results) != 2 || calls.Load() != 2 {
		t.Errorf("got %d results and %d calls, want 2 and 2", len(results), calls.Load())
	}
}
