package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

// The age sweeper is the backstop for subtree blobs that delete-at-height can
// never reach: a subtree blob only gets a DAH schedule when its subtree-work
// item completes, so trimmed queues, long parking, and crashes orphan blobs
// FOREVER (2026-07-15 dev-ovh-1: 39,477 orphans filled a 1TiB volume in ~3h).
// Subtree blobs are re-fetchable from DataHub, so age-based GC is safe — but
// ONLY for subtree blobs. STUMP blobs (namespaced under "stump/") are read by
// callback-delivery at delivery time with retry windows up to ~1h and must
// never be age-swept; ".dah/" manifests are bookkeeping that must survive
// until their height fires. The sweeper therefore discriminates by key
// namespace: only top-level 64-lowercase-hex files qualify, plus zero-byte
// ENOSPC litter (13,433 such files in the incident) anywhere outside .dah/.

// subtreeKey returns a valid top-level subtree blob key: 64 lowercase hex
// characters, the shape produced by content addressing (sha256 hex).
func subtreeKey(b string) string {
	return strings.Repeat(b, 32)
}

// requireCaseSensitiveFS skips the test when dir lives on a case-insensitive
// filesystem (macOS APFS/HFS+ by default). Tests that assert lowercase-hex keys
// are swept while their uppercase twins survive are meaningless there: the two
// keys collide onto a single inode, so removing one removes both. CI runs on
// case-sensitive Linux, where the discrimination is real.
func requireCaseSensitiveFS(t *testing.T, dir string) {
	t.Helper()
	lower := filepath.Join(dir, ".case-probe")
	if err := os.WriteFile(lower, nil, 0o600); err != nil {
		t.Fatalf("case-probe write: %v", err)
	}
	defer func() { _ = os.Remove(lower) }()
	if _, err := os.Stat(filepath.Join(dir, ".CASE-PROBE")); err == nil {
		t.Skip("filesystem is case-insensitive; lowercase/uppercase keys collide")
	}
}

// TestFileBlobStore_SweepOlderThanDeletesOnlyOldSubtreeBlobs pins the core
// contract: top-level 64-lowercase-hex files older than maxAge are removed
// (with their byte size accounted), while fresh subtree blobs, STUMP blobs of
// any age, non-subtree-shaped names, and .dah manifests are never touched.
func TestFileBlobStore_SweepOlderThanDeletesOnlyOldSubtreeBlobs(t *testing.T) {
	dir := t.TempDir()
	requireCaseSensitiveFS(t, dir)
	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}

	oldSubtree := subtreeKey("0a")
	freshSubtree := subtreeKey("0b")
	oldStump := "stump/" + subtreeKey("0c")
	oldUppercase := strings.Repeat("0A", 32) // 64 chars but not lowercase hex
	oldShortHex := strings.Repeat("d", 63)   // hex but not 64 chars
	oldNonHex := "not-a-subtree.bin"

	payload := []byte("subtree-payload")
	for _, key := range []string{oldSubtree, freshSubtree, oldStump, oldUppercase, oldShortHex, oldNonHex} {
		if setErr := bs.Set(key, payload); setErr != nil {
			t.Fatalf("Set(%s): %v", key, setErr)
		}
	}
	// A schedule for a future height — its manifest must survive sweeping
	// even when the manifest file itself is old.
	if schedErr := bs.ScheduleDelete(freshSubtree, 99); schedErr != nil {
		t.Fatalf("ScheduleDelete: %v", schedErr)
	}

	// Age everything except freshSubtree well past maxAge — including the
	// manifest, the stump, and the non-subtree-shaped names.
	past := time.Now().Add(-2 * time.Hour)
	for _, rel := range []string{oldSubtree, oldStump, oldUppercase, oldShortHex, oldNonHex} {
		if chErr := os.Chtimes(filepath.Join(dir, rel), past, past); chErr != nil {
			t.Fatalf("Chtimes(%s): %v", rel, chErr)
		}
	}
	manifests, globErr := filepath.Glob(filepath.Join(dir, dahDirName, "*", "*.list"))
	if globErr != nil || len(manifests) == 0 {
		t.Fatalf("locating manifests: %v (found %d)", globErr, len(manifests))
	}
	for _, m := range manifests {
		if chErr := os.Chtimes(m, past, past); chErr != nil {
			t.Fatalf("aging manifest %s: %v", m, chErr)
		}
	}

	files, bytes, err := bs.SweepOlderThan(time.Hour)
	if err != nil {
		t.Fatalf("SweepOlderThan: %v", err)
	}
	if files != 1 {
		t.Errorf("SweepOlderThan files = %d, want 1", files)
	}
	if bytes != int64(len(payload)) {
		t.Errorf("SweepOlderThan bytes = %d, want %d", bytes, len(payload))
	}

	if _, err := bs.Get(oldSubtree); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("old subtree blob should be swept; Get err = %v", err)
	}
	for _, key := range []string{freshSubtree, oldStump, oldUppercase, oldShortHex, oldNonHex} {
		if _, err := bs.Get(key); err != nil {
			t.Errorf("%s must survive the sweep; Get err = %v", key, err)
		}
	}

	// The aged manifest must still fire later — sweeping must not have
	// touched the bookkeeping.
	bs.SetCurrentBlockHeight(99)
	if _, err := bs.Get(freshSubtree); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("schedule must survive sweeping and fire at its height; Get err = %v", err)
	}
}

// TestFileBlobStore_SweepOlderThanReapsZeroByteLitter pins the ENOSPC-litter
// rule: zero-byte files older than ~5 minutes are reaped regardless of the
// age threshold and regardless of namespace (a zero-byte STUMP is unreadable
// litter, not a deliverable blob) — but never inside .dah/, and never while
// fresh (a write in progress is briefly zero-byte).
func TestFileBlobStore_SweepOlderThanReapsZeroByteLitter(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}

	emptySubtree := subtreeKey("0e")
	emptyStump := "stump/" + subtreeKey("0f")
	emptyFresh := subtreeKey("1a")
	for _, key := range []string{emptySubtree, emptyStump, emptyFresh} {
		if setErr := bs.Set(key, nil); setErr != nil {
			t.Fatalf("Set(%s): %v", key, setErr)
		}
	}
	// A zero-byte file inside .dah/ (torn manifest create) must be left for
	// the DAH machinery — the sweeper never reaches into bookkeeping space.
	dahHeightDir := filepath.Join(dir, dahDirName, "42")
	if mkErr := os.MkdirAll(dahHeightDir, 0o750); mkErr != nil {
		t.Fatalf("MkdirAll(.dah/42): %v", mkErr)
	}
	emptyManifest := filepath.Join(dahHeightDir, "torn.list")
	if wErr := os.WriteFile(emptyManifest, nil, 0o600); wErr != nil {
		t.Fatalf("WriteFile(torn.list): %v", wErr)
	}

	// 10 minutes old: past the zero-byte grace, well inside the 30m maxAge.
	past := time.Now().Add(-10 * time.Minute)
	for _, p := range []string{
		filepath.Join(dir, emptySubtree),
		filepath.Join(dir, emptyStump),
		emptyManifest,
	} {
		if chErr := os.Chtimes(p, past, past); chErr != nil {
			t.Fatalf("Chtimes(%s): %v", p, chErr)
		}
	}

	files, bytes, err := bs.SweepOlderThan(30 * time.Minute)
	if err != nil {
		t.Fatalf("SweepOlderThan: %v", err)
	}
	if files != 2 {
		t.Errorf("SweepOlderThan files = %d, want 2 (the two aged zero-byte blobs)", files)
	}
	if bytes != 0 {
		t.Errorf("SweepOlderThan bytes = %d, want 0", bytes)
	}

	for _, key := range []string{emptySubtree, emptyStump} {
		if _, err := bs.Get(key); !errors.Is(err, ErrBlobNotFound) {
			t.Errorf("aged zero-byte %s should be reaped; Get err = %v", key, err)
		}
	}
	if _, err := bs.Get(emptyFresh); err != nil {
		t.Errorf("fresh zero-byte file must survive (write may be in flight); Get err = %v", err)
	}
	if _, err := os.Stat(emptyManifest); err != nil {
		t.Errorf("zero-byte file under .dah/ must never be touched; Stat err = %v", err)
	}
}

// TestFileBlobStore_StartAgeSweeper pins the runner: an immediate sweep on
// start (deterministic for operators and tests alike), metrics counters
// updated with the sweep's files/bytes, and an idempotent stop function.
func TestFileBlobStore_StartAgeSweeper(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}

	ancient := subtreeKey("2b")
	payload := []byte("x")
	if err := bs.Set(ancient, payload); err != nil {
		t.Fatalf("Set: %v", err)
	}
	past := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, ancient), past, past); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	filesBefore := testutil.ToFloat64(metrics.BlobStoreSweptFilesTotal)
	bytesBefore := testutil.ToFloat64(metrics.BlobStoreSweptBytesTotal)

	stop := bs.StartAgeSweeper(time.Minute, time.Hour, nil)
	defer stop()

	if _, err := bs.Get(ancient); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("StartAgeSweeper must run an immediate sweep; Get err = %v", err)
	}
	if got := testutil.ToFloat64(metrics.BlobStoreSweptFilesTotal) - filesBefore; got != 1 {
		t.Errorf("swept-files counter delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.BlobStoreSweptBytesTotal) - bytesBefore; got != float64(len(payload)) {
		t.Errorf("swept-bytes counter delta = %v, want %d", got, len(payload))
	}

	// stop must be idempotent and not hang.
	stop()
	stop()
}

// TestStartAgeSweeperFromConfig pins the wiring helper the block-processor
// uses: file-backed store + positive interval + positive max age starts a
// sweeper (immediate first sweep proves it); interval 0, max age 0, or a
// memory store yields a safe no-op stop.
func TestStartAgeSweeperFromConfig(t *testing.T) {
	newAgedStore := func(t *testing.T) (*FileBlobStore, string) {
		t.Helper()
		dir := t.TempDir()
		bs, err := NewFileBlobStore(dir)
		if err != nil {
			t.Fatalf("NewFileBlobStore: %v", err)
		}
		ancient := subtreeKey("3c")
		if err := bs.Set(ancient, []byte("x")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		past := time.Now().Add(-3 * time.Hour)
		if err := os.Chtimes(filepath.Join(dir, ancient), past, past); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
		return bs, ancient
	}

	t.Run("enabled config sweeps immediately", func(t *testing.T) {
		bs, ancient := newAgedStore(t)
		stop := StartAgeSweeperFromConfig(bs, config.BlobStoreConfig{SweepIntervalSec: 60, SweepMaxAgeSec: 3600}, nil)
		defer stop()

		if _, err := bs.Get(ancient); !errors.Is(err, ErrBlobNotFound) {
			t.Errorf("enabled sweeper must sweep on start; Get err = %v", err)
		}
	})

	t.Run("interval 0 disables the sweeper", func(t *testing.T) {
		bs, ancient := newAgedStore(t)
		stop := StartAgeSweeperFromConfig(bs, config.BlobStoreConfig{SweepIntervalSec: 0, SweepMaxAgeSec: 3600}, nil)
		stop() // must not panic

		if _, err := bs.Get(ancient); err != nil {
			t.Errorf("disabled sweeper must not remove anything; Get err = %v", err)
		}
	})

	t.Run("max age 0 disables the sweeper", func(t *testing.T) {
		bs, ancient := newAgedStore(t)
		stop := StartAgeSweeperFromConfig(bs, config.BlobStoreConfig{SweepIntervalSec: 60, SweepMaxAgeSec: 0}, nil)
		stop() // must not panic

		if _, err := bs.Get(ancient); err != nil {
			t.Errorf("disabled sweeper must not remove anything; Get err = %v", err)
		}
	})

	t.Run("memory store returns safe no-op", func(t *testing.T) {
		stop := StartAgeSweeperFromConfig(NewMemoryBlobStore(), config.BlobStoreConfig{SweepIntervalSec: 60, SweepMaxAgeSec: 3600}, nil)
		stop() // must not panic
	})
}
