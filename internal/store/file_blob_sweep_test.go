package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/config"
)

// The age sweeper is the backstop for blobs that delete-at-height can never
// reach: subtrees announced but never mined (no block ever supplies their
// height), plus anything orphaned by a crash between a blob write and its
// schedule append, and pre-existing files from before manifests shipped.
// Age is the only signal available for these — every blob with a knowable
// height is already covered by the (event-driven) DAH path, so the sweeper
// only ever fires on data the chain abandoned.

// TestFileBlobStore_SweepOrphansDeletesOnlyOldFiles pins the core contract:
// files older than maxAge are removed, fresh files and DAH manifests are not.
func TestFileBlobStore_SweepOrphansDeletesOnlyOldFiles(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}

	for _, key := range []string{"old-blob", "fresh-blob", "stump/oldref"} {
		if setErr := bs.Set(key, []byte("payload")); setErr != nil {
			t.Fatalf("Set(%s): %v", key, setErr)
		}
	}
	// A schedule for a future height — its manifest must survive sweeping
	// even when the manifest file itself is old.
	if schedErr := bs.ScheduleDelete("fresh-blob", 99); schedErr != nil {
		t.Fatalf("ScheduleDelete: %v", schedErr)
	}

	// Age the old files (and the manifest) well past maxAge.
	past := time.Now().Add(-2 * time.Hour)
	for _, rel := range []string{"old-blob", "stump/oldref"} {
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

	removed, err := bs.SweepOrphans(time.Hour)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if removed != 2 {
		t.Errorf("SweepOrphans removed = %d, want 2", removed)
	}

	if _, err := bs.Get("old-blob"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("old-blob should be swept; Get err = %v", err)
	}
	if _, err := bs.Get("stump/oldref"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("stump/oldref should be swept; Get err = %v", err)
	}
	if _, err := bs.Get("fresh-blob"); err != nil {
		t.Errorf("fresh-blob must survive the sweep; Get err = %v", err)
	}

	// The aged manifest must still fire later — sweeping must not have
	// touched the bookkeeping.
	bs.SetCurrentBlockHeight(99)
	if _, err := bs.Get("fresh-blob"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("schedule must survive sweeping and fire at its height; Get err = %v", err)
	}
}

// TestFileBlobStore_StartOrphanSweeper pins the runner: an immediate sweep on
// start (deterministic for operators and tests alike), and a stop function
// that halts the loop.
func TestFileBlobStore_StartOrphanSweeper(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}

	if err := bs.Set("ancient", []byte("x")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	past := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, "ancient"), past, past); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	stop := bs.StartOrphanSweeper(time.Minute, time.Hour, nil)
	defer stop()

	if _, err := bs.Get("ancient"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("StartOrphanSweeper must run an immediate sweep; Get err = %v", err)
	}

	// stop must be idempotent and not hang.
	stop()
	stop()
}

// TestStartBlobSweeperFromConfig pins the wiring helper the store factories
// use: file-backed store + positive max age starts a sweeper (immediate first
// sweep proves it); memory store or disabled config yields a no-op stop.
func TestStartBlobSweeperFromConfig(t *testing.T) {
	t.Run("file store with sweeper enabled sweeps immediately", func(t *testing.T) {
		dir := t.TempDir()
		bs, err := NewFileBlobStore(dir)
		if err != nil {
			t.Fatalf("NewFileBlobStore: %v", err)
		}
		if err := bs.Set("ancient", []byte("x")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		past := time.Now().Add(-3 * time.Hour)
		if err := os.Chtimes(filepath.Join(dir, "ancient"), past, past); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}

		stop := StartBlobSweeperFromConfig(bs, config.BlobStoreConfig{OrphanMaxAgeSec: 3600, SweepIntervalSec: 60}, nil)
		defer stop()

		if _, err := bs.Get("ancient"); !errors.Is(err, ErrBlobNotFound) {
			t.Errorf("enabled sweeper must sweep on start; Get err = %v", err)
		}
	})

	t.Run("disabled and non-file stores return safe no-op", func(t *testing.T) {
		bs, err := NewFileBlobStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewFileBlobStore: %v", err)
		}
		stop := StartBlobSweeperFromConfig(bs, config.BlobStoreConfig{OrphanMaxAgeSec: 0}, nil)
		stop() // must not panic

		stop = StartBlobSweeperFromConfig(NewMemoryBlobStore(), config.BlobStoreConfig{OrphanMaxAgeSec: 3600}, nil)
		stop() // must not panic
	})
}

// TestFileBlobStore_StartOrphanSweeperGuardsInterval pins that a non-positive
// interval cannot panic time.NewTicker: the runner clamps it to a sane
// default. (StartBlobSweeperFromConfig already clamps, but this method is
// exported and callable directly.)
func TestFileBlobStore_StartOrphanSweeperGuardsInterval(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}
	if err := bs.Set("ancient", []byte("x")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	past := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, "ancient"), past, past); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	stop := bs.StartOrphanSweeper(0, time.Hour, nil) // must not panic
	defer stop()

	if _, err := bs.Get("ancient"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("immediate sweep must still run with clamped interval; Get err = %v", err)
	}
}
