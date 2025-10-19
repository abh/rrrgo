package fsck

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/abh/rrrgo/recent"
	"github.com/abh/rrrgo/recentfile"
)

// setupTest creates a test Recent with 1h and 6h intervals
func setupTest(t *testing.T) (*recent.Recent, []*recentfile.Recentfile) {
	t.Helper()
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)

	if err := principal.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := principal.Write(); err != nil {
		t.Fatal(err)
	}
	principal.Unlock()

	rec, err := recent.NewWithPrincipal(principal)
	if err != nil {
		t.Fatal(err)
	}

	return rec, rec.Recentfiles()
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestNewerDeleteEvent verifies fsck doesn't report false positive when:
// - Old file has "new" event (epoch 500)
// - New file has "delete" event (epoch 1000)
// - File doesn't exist on disk (correctly deleted)
func TestNewerDeleteEvent(t *testing.T) {
	rec, rfs := setupTest(t)
	tmpDir := rec.LocalRoot()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Add events with recent timestamps
	now := recentfile.EpochNow()
	oldEpoch := recentfile.EpochFromFloat(float64(now) - 3600) // 1 hour ago
	newEpoch := now // now

	// Add "new" event to 6h file (older, 1 hour ago)
	if err := rfs[1].Update(testFile, "new", oldEpoch); err != nil {
		t.Fatalf("Update 6h failed: %v", err)
	}

	// Add "delete" event to 1h file (newer, now)
	if err := rfs[0].Update(testFile, "delete", newEpoch); err != nil {
		t.Fatalf("Update 1h failed: %v", err)
	}

	// File doesn't exist on disk (correctly deleted)

	result, err := Run(rec, Options{Logger: quietLogger(), SkipEvents: false})
	if err != nil {
		t.Fatal(err)
	}

	// BUG: fsck reports this as an issue, but shouldn't (most recent event is delete)
	if result.Issues > 0 {
		t.Errorf("FAIL: got %d issues, want 0 (most recent event is delete)", result.Issues)
	}
}
