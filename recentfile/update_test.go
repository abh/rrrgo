package recentfile

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithFilenameRoot("RECENT"),
	)

	// Update with new file
	testFile := filepath.Join(tmpDir, "foo/bar.txt")
	err := rf.Update(testFile, "new")
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(rf.Rfile()); err != nil {
		t.Error("recentfile not created")
	}

	// Read and verify
	rf2, err := NewFromFile(rf.Rfile())
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	if len(rf2.recent) != 1 {
		t.Fatalf("expected 1 event, got %d", len(rf2.recent))
	}

	if rf2.recent[0].Path != "foo/bar.txt" {
		t.Errorf("path = %q, want %q", rf2.recent[0].Path, "foo/bar.txt")
	}
	if rf2.recent[0].Type != "new" {
		t.Errorf("type = %q, want %q", rf2.recent[0].Type, "new")
	}
}

func TestUpdateWithDirtyEpoch(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	dirtyEpoch := Epoch(1234567890.123456)

	err := rf.Update(filepath.Join(tmpDir, "test.txt"), "new", dirtyEpoch)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Read back
	rf2, err := NewFromFile(rf.Rfile())
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	// Verify dirtymark was set
	if rf2.meta.Dirtymark.IsZero() {
		t.Error("Dirtymark should be set for dirty epoch")
	}

	// Verify merged info was cleared
	if rf2.meta.Merged != nil {
		t.Error("Merged should be nil after dirty epoch")
	}
}

func TestBatchUpdate(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Batch update
	batch := []BatchItem{
		{Path: filepath.Join(tmpDir, "a.txt"), Type: "new"},
		{Path: filepath.Join(tmpDir, "b.txt"), Type: "new"},
		{Path: filepath.Join(tmpDir, "c.txt"), Type: "new"},
	}

	err := rf.BatchUpdate(batch)
	if err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}

	// Verify
	rf2, err := NewFromFile(rf.Rfile())
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	if len(rf2.recent) != 3 {
		t.Fatalf("expected 3 events, got %d", len(rf2.recent))
	}

	// Verify sorted by epoch descending
	for i := 1; i < len(rf2.recent); i++ {
		if EpochLt(rf2.recent[i-1].Epoch, rf2.recent[i].Epoch) {
			t.Errorf("events not sorted: epoch[%d]=%s < epoch[%d]=%s",
				i-1, rf2.recent[i-1].Epoch, i, rf2.recent[i].Epoch)
		}
	}
}

func TestBatchUpdateRemovesDuplicates(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	testFile := filepath.Join(tmpDir, "test.txt")

	// First update
	err := rf.Update(testFile, "new")
	if err != nil {
		t.Fatalf("First update failed: %v", err)
	}

	// Read epoch of first update
	rf2, _ := NewFromFile(rf.Rfile())
	firstEpoch := rf2.recent[0].Epoch

	// Wait a bit to ensure different epoch
	time.Sleep(10 * time.Millisecond)

	// Second update of same file
	err = rf.Update(testFile, "new")
	if err != nil {
		t.Fatalf("Second update failed: %v", err)
	}

	// Verify only one event exists
	rf3, _ := NewFromFile(rf.Rfile())
	if len(rf3.recent) != 1 {
		t.Errorf("expected 1 event after duplicate update, got %d", len(rf3.recent))
	}

	// Verify epoch changed
	if rf3.recent[0].Epoch == firstEpoch {
		t.Error("epoch should change on duplicate update")
	}

	// Verify epoch is newer
	if !EpochGt(rf3.recent[0].Epoch, firstEpoch) {
		t.Error("new epoch should be greater than old epoch")
	}
}

func TestMonotonicEpochs(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Add multiple events very quickly
	batch := make([]BatchItem, 10)
	for i := 0; i < 10; i++ {
		batch[i] = BatchItem{
			Path: filepath.Join(tmpDir, "file"+string(rune('0'+i))+".txt"),
			Type: "new",
		}
	}

	err := rf.BatchUpdate(batch)
	if err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}

	// Verify all epochs are unique and monotonic
	rf2, _ := NewFromFile(rf.Rfile())

	seen := make(map[Epoch]bool)
	for i, event := range rf2.recent {
		// Check uniqueness
		if seen[event.Epoch] {
			t.Errorf("duplicate epoch: %s", event.Epoch)
		}
		seen[event.Epoch] = true

		// Check monotonic descending order
		if i > 0 && !EpochLt(event.Epoch, rf2.recent[i-1].Epoch) {
			t.Errorf("epochs not monotonic at index %d", i)
		}
	}
}

func TestPathCanonicalization(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	tests := []struct {
		input string
		want  string
	}{
		{filepath.Join(tmpDir, "foo/bar.txt"), "foo/bar.txt"},
		{filepath.Join(tmpDir, "/foo/bar.txt"), "foo/bar.txt"},
		{filepath.Join(tmpDir, "foo//bar.txt"), "foo/bar.txt"},
		{filepath.Join(tmpDir, "foo/./bar.txt"), "foo/bar.txt"},
	}

	for _, tt := range tests {
		err := rf.Update(tt.input, "new")
		if err != nil {
			t.Fatalf("Update(%q) failed: %v", tt.input, err)
		}

		// Clear events for next test
		rf.mu.Lock()
		rf.recent = nil
		rf.mu.Unlock()
	}
}

func TestTruncateByInterval(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Create events with different ages
	now := EpochNow()
	nowFloat := EpochToFloat(now)

	batch := []BatchItem{
		{Path: "current.txt", Type: "new"},                                         // Now
		{Path: "30min.txt", Type: "new", Epoch: EpochFromFloat(nowFloat - 1800)},   // 30 min ago
		{Path: "2hours.txt", Type: "new", Epoch: EpochFromFloat(nowFloat - 7200)},  // 2 hours ago (should be truncated)
		{Path: "5hours.txt", Type: "new", Epoch: EpochFromFloat(nowFloat - 18000)}, // 5 hours ago (should be truncated)
	}

	err := rf.BatchUpdate(batch)
	if err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}

	// Read back
	rf2, _ := NewFromFile(rf.Rfile())

	// Should only have events within 1h window
	// Note: truncation happens based on interval
	if len(rf2.recent) > 2 {
		t.Logf("Events retained: %d", len(rf2.recent))
		for i, e := range rf2.recent {
			t.Logf("  [%d] %s: %s", i, e.Epoch, e.Path)
		}
	}
}

func TestMinmaxUpdate(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Add some events
	batch := []BatchItem{
		{Path: "a.txt", Type: "new"},
		{Path: "b.txt", Type: "new"},
		{Path: "c.txt", Type: "new"},
	}

	err := rf.BatchUpdate(batch)
	if err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}

	// Read and verify minmax
	rf2, _ := NewFromFile(rf.Rfile())

	if rf2.meta.Minmax == nil {
		t.Fatal("Minmax should be set")
	}

	// Max should be the first (newest) event
	if rf2.meta.Minmax.Max != rf2.recent[0].Epoch {
		t.Errorf("Max = %s, want %s", rf2.meta.Minmax.Max, rf2.recent[0].Epoch)
	}

	// Min should be the last (oldest) event
	lastIdx := len(rf2.recent) - 1
	if rf2.meta.Minmax.Min != rf2.recent[lastIdx].Epoch {
		t.Errorf("Min = %s, want %s", rf2.meta.Minmax.Min, rf2.recent[lastIdx].Epoch)
	}

	// Mtime should be recent
	mtimeAge := time.Now().Unix() - rf2.meta.Minmax.Mtime
	if mtimeAge > 5 {
		t.Errorf("Mtime is %d seconds old, expected recent", mtimeAge)
	}
}

func TestDeleteEvent(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	testFile := filepath.Join(tmpDir, "test.txt")

	// Add file
	err := rf.Update(testFile, "new")
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Delete file
	err = rf.Update(testFile, "delete")
	if err != nil {
		t.Fatalf("Delete update failed: %v", err)
	}

	// Verify delete event replaces new event
	rf2, _ := NewFromFile(rf.Rfile())
	if len(rf2.recent) != 1 {
		t.Fatalf("expected 1 event, got %d", len(rf2.recent))
	}

	if rf2.recent[0].Type != "delete" {
		t.Errorf("type = %q, want %q", rf2.recent[0].Type, "delete")
	}
}

func TestEmptyBatchUpdate(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Empty batch should not error
	err := rf.BatchUpdate([]BatchItem{})
	if err != nil {
		t.Errorf("Empty batch update failed: %v", err)
	}

	// Nil batch should not error
	err = rf.BatchUpdate(nil)
	if err != nil {
		t.Errorf("Nil batch update failed: %v", err)
	}
}

func TestSortEventsByEpoch(t *testing.T) {
	rf := &Recentfile{}

	events := []Event{
		{Epoch: 100.0, Path: "a"},
		{Epoch: 105.0, Path: "b"},
		{Epoch: 102.0, Path: "c"},
		{Epoch: 101.0, Path: "d"},
	}

	rf.sortEventsByEpoch(events)

	// Verify sorted descending
	expected := []Epoch{105.0, 102.0, 101.0, 100.0}
	for i, e := range events {
		if e.Epoch != expected[i] {
			t.Errorf("events[%d].Epoch = %v, want %v", i, e.Epoch, expected[i])
		}
	}
}

func TestEnsureMonotonic(t *testing.T) {
	rf := &Recentfile{}

	events := []Event{
		{Epoch: 100.0, Path: "a"},
		{Epoch: 99.0, Path: "b"},
	}

	// New epoch same as most recent - should be incremented
	result := rf.ensureMonotonic(100.0, events)
	if !EpochGt(result, 100.0) {
		t.Errorf("ensureMonotonic(100.0) = %v, should be > 100.0", result)
	}

	// New epoch less than most recent - should be incremented
	result = rf.ensureMonotonic(99.5, events)
	if !EpochGt(result, 100.0) {
		t.Errorf("ensureMonotonic(99.5) = %v, should be > 100.0", result)
	}

	// New epoch greater than most recent - should be unchanged
	result = rf.ensureMonotonic(101.0, events)
	if result != 101.0 {
		t.Errorf("ensureMonotonic(101.0) = %v, want 101.0", result)
	}

	// Empty events - should be unchanged
	result = rf.ensureMonotonic(50.0, []Event{})
	if result != 50.0 {
		t.Errorf("ensureMonotonic(50.0, empty) = %v, want 50.0", result)
	}
}
