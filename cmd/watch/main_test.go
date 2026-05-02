package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

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
