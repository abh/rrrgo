package recent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/abh/rrrgo/recentfile"
)

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a principal recentfile
	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h", "1d", "1W"}),
	)

	// Write it to disk
	if err := principal.Lock(); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if err := principal.Write(); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	principal.Unlock()

	// Create Recent from file
	principalPath := principal.Rfile()
	rec, err := New(principalPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Verify principal
	if rec.PrincipalRecentfile() == nil {
		t.Fatal("Principal is nil")
	}

	if rec.PrincipalRecentfile().Interval() != "1h" {
		t.Errorf("Principal interval = %s, want 1h", rec.PrincipalRecentfile().Interval())
	}

	// Verify hierarchy
	rfs := rec.Recentfiles()
	if len(rfs) != 4 {
		t.Errorf("Recentfiles count = %d, want 4 (1h, 6h, 1d, 1W)", len(rfs))
	}

	// Verify sorted by interval
	expectedIntervals := []string{"1h", "6h", "1d", "1W"}
	intervals := rec.Intervals()
	if len(intervals) != len(expectedIntervals) {
		t.Fatalf("Intervals count = %d, want %d", len(intervals), len(expectedIntervals))
	}
	for i, want := range expectedIntervals {
		if intervals[i] != want {
			t.Errorf("Interval[%d] = %s, want %s", i, intervals[i], want)
		}
	}
}

func TestNewWithPrincipal(t *testing.T) {
	tmpDir := t.TempDir()

	// Create principal
	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h", "1d"}),
	)

	// Create Recent
	rec, err := NewWithPrincipal(principal)
	if err != nil {
		t.Fatalf("NewWithPrincipal failed: %v", err)
	}

	// Verify
	if rec.PrincipalRecentfile() != principal {
		t.Error("Principal not set correctly")
	}

	if len(rec.Recentfiles()) != 3 {
		t.Errorf("Recentfiles count = %d, want 3", len(rec.Recentfiles()))
	}
}

func TestNewWithPrincipalNil(t *testing.T) {
	_, err := NewWithPrincipal(nil)
	if err == nil {
		t.Error("NewWithPrincipal(nil) should error")
	}
}

func TestRecentfileByInterval(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h", "1d"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Test existing intervals
	rf1h := rec.RecentfileByInterval("1h")
	if rf1h == nil {
		t.Error("RecentfileByInterval(1h) returned nil")
	}
	if rf1h.Interval() != "1h" {
		t.Errorf("Interval = %s, want 1h", rf1h.Interval())
	}

	rf6h := rec.RecentfileByInterval("6h")
	if rf6h == nil {
		t.Error("RecentfileByInterval(6h) returned nil")
	}

	// Test non-existing interval
	rfNone := rec.RecentfileByInterval("1W")
	if rfNone != nil {
		t.Error("RecentfileByInterval(1W) should return nil")
	}
}

func TestUpdate(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Update
	testFile := filepath.Join(tmpDir, "test.txt")
	err := rec.Update(testFile, "new")
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify event in principal
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(events))
	}

	if events[0].Path != "test.txt" {
		t.Errorf("Path = %s, want test.txt", events[0].Path)
	}
}

func TestBatchUpdate(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
	)

	rec, _ := NewWithPrincipal(principal)

	// Batch update
	batch := []recentfile.BatchItem{
		{Path: filepath.Join(tmpDir, "file1.txt"), Type: "new"},
		{Path: filepath.Join(tmpDir, "file2.txt"), Type: "new"},
		{Path: filepath.Join(tmpDir, "file3.txt"), Type: "new"},
	}

	err := rec.BatchUpdate(batch)
	if err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}

	// Verify
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) != 3 {
		t.Errorf("Expected 3 events, got %d", len(events))
	}
}

func TestAggregate(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h", "1d"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Add events
	batch := []recentfile.BatchItem{
		{Path: "file1.txt", Type: "new"},
		{Path: "file2.txt", Type: "new"},
	}
	rec.BatchUpdate(batch)

	// Aggregate
	err := rec.Aggregate(true)
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	// Verify 6h file exists and has events
	rf6h := rec.RecentfileByInterval("6h")
	if rf6h == nil {
		t.Fatal("6h recentfile not found")
	}

	// Read from disk to verify
	rf6hRead, err := recentfile.NewFromFile(rf6h.Rfile())
	if err != nil {
		t.Fatalf("Read 6h file failed: %v", err)
	}

	if len(rf6hRead.RecentEvents()) == 0 {
		t.Error("6h file should have events after aggregation")
	}
}

func TestEnsureFilesExist(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h", "1d"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Ensure files exist
	err := rec.EnsureFilesExist()
	if err != nil {
		t.Fatalf("EnsureFilesExist failed: %v", err)
	}

	// Verify all files exist
	for _, interval := range []string{"1h", "6h", "1d"} {
		path := filepath.Join(tmpDir, "RECENT-"+interval+".yaml")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("File %s doesn't exist: %v", interval, err)
		}
	}
}

func TestLoadAll(t *testing.T) {
	tmpDir := t.TempDir()

	// Create and write principal with events
	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)
	principal.BatchUpdate([]recentfile.BatchItem{
		{Path: "file1.txt", Type: "new"},
	})

	// Create 6h file with events
	rf6h := principal.SparseClone()
	rf6h.SetInterval("6h")
	rf6h.BatchUpdate([]recentfile.BatchItem{
		{Path: "file2.txt", Type: "new"},
	})

	// Create fresh Recent (files exist on disk)
	rec, err := New(principal.Rfile())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Initially, 6h recentfile in memory has no events
	rf6hMem := rec.RecentfileByInterval("6h")
	if len(rf6hMem.RecentEvents()) > 0 {
		t.Error("6h should have no events in memory initially")
	}

	// LoadAll should load events from disk
	err = rec.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	// Now 6h should have events
	rf6hAfter := rec.RecentfileByInterval("6h")
	if len(rf6hAfter.RecentEvents()) != 1 {
		t.Errorf("6h has %d events after LoadAll, want 1", len(rf6hAfter.RecentEvents()))
	}
}

func TestStats(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Add events
	rec.BatchUpdate([]recentfile.BatchItem{
		{Path: "file1.txt", Type: "new"},
		{Path: "file2.txt", Type: "new"},
	})

	// Get stats
	stats := rec.Stats()

	if stats.Intervals != 2 {
		t.Errorf("Stats.Intervals = %d, want 2", stats.Intervals)
	}

	if stats.TotalEvents != 2 {
		t.Errorf("Stats.TotalEvents = %d, want 2", stats.TotalEvents)
	}

	// Check file stats for 1h
	fs1h, ok := stats.Files["1h"]
	if !ok {
		t.Fatal("Stats missing 1h file")
	}

	if fs1h.Interval != "1h" {
		t.Errorf("FileStats.Interval = %s, want 1h", fs1h.Interval)
	}

	if fs1h.Events != 2 {
		t.Errorf("FileStats.Events = %d, want 2", fs1h.Events)
	}

	if fs1h.Size == 0 {
		t.Error("FileStats.Size should be non-zero")
	}
}

func TestValidate(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h", "1d"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Should be valid
	errs := rec.Validate()
	if len(errs) > 0 {
		t.Errorf("Validate returned errors: %v", errs)
	}
}

func TestValidateNilPrincipal(t *testing.T) {
	rec := &Recent{}

	errs := rec.Validate()
	if len(errs) == 0 {
		t.Error("Validate should return errors for nil principal")
	}
}

func TestValidateMismatchedLocalRoot(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Manually break a recentfile's local root
	rec.mu.Lock()
	if len(rec.recentfiles) > 1 {
		rec.recentfiles[1].SetLocalRoot("/different/path")
	}
	rec.mu.Unlock()

	// Should detect error
	errs := rec.Validate()
	if len(errs) == 0 {
		t.Error("Validate should detect mismatched local root")
	}
}

func TestValidateDuplicateInterval(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Manually add a duplicate interval
	rec.mu.Lock()
	duplicate := rec.principal.SparseClone()
	duplicate.SetInterval("6h")
	rec.recentfiles = append(rec.recentfiles, duplicate)
	rec.mu.Unlock()

	// Should detect duplicate
	errs := rec.Validate()
	if len(errs) == 0 {
		t.Error("Validate should detect duplicate interval")
	}

	// Check error message mentions "duplicate"
	foundDuplicate := false
	for _, err := range errs {
		if err != nil {
			errStr := err.Error()
			if len(errStr) > 0 && errStr[:9] == "duplicate" {
				foundDuplicate = true
				// Verify message format
				if len(errStr) < 20 {
					t.Errorf("Duplicate error message too short: %s", errStr)
				}
				t.Logf("Duplicate interval error: %s", errStr)
				break
			}
		}
	}

	if !foundDuplicate {
		t.Error("Expected error message to start with 'duplicate'")
	}
}

func TestPrincipalInAggregatorList(t *testing.T) {
	tmpDir := t.TempDir()

	// Create principal with aggregator list that includes principal's own interval
	// This should NOT create a duplicate
	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"1h", "6h", "1d"}), // "1h" is a duplicate
	)

	rec, err := NewWithPrincipal(principal)
	if err != nil {
		t.Fatalf("NewWithPrincipal failed: %v", err)
	}

	// Should have 3 intervals, not 4 (1h should not be duplicated)
	intervals := rec.Intervals()
	if len(intervals) != 3 {
		t.Errorf("Expected 3 intervals (1h, 6h, 1d), got %d: %v", len(intervals), intervals)
	}

	// Verify no duplicates
	seen := make(map[string]bool)
	for _, interval := range intervals {
		if seen[interval] {
			t.Errorf("Duplicate interval found: %s", interval)
		}
		seen[interval] = true
	}

	// Validate should pass (no duplicates)
	errs := rec.Validate()
	if len(errs) > 0 {
		t.Errorf("Validate returned unexpected errors: %v", errs)
	}
}

func TestLocalRoot(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
	)

	rec, _ := NewWithPrincipal(principal)

	if rec.LocalRoot() != tmpDir {
		t.Errorf("LocalRoot = %s, want %s", rec.LocalRoot(), tmpDir)
	}
}

func TestIntervals(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h", "1d", "1W"}),
	)

	rec, _ := NewWithPrincipal(principal)

	intervals := rec.Intervals()
	expected := []string{"1h", "6h", "1d", "1W"}

	if len(intervals) != len(expected) {
		t.Fatalf("Intervals length = %d, want %d", len(intervals), len(expected))
	}

	for i, want := range expected {
		if intervals[i] != want {
			t.Errorf("Intervals[%d] = %s, want %s", i, intervals[i], want)
		}
	}
}

func TestString(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)

	rec, _ := NewWithPrincipal(principal)

	str := rec.String()
	if str == "" {
		t.Error("String() returned empty string")
	}

	// Should contain key information
	if len(str) < 10 {
		t.Errorf("String() too short: %s", str)
	}
}

func TestRecentfilesReturnsCocy(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Get recentfiles
	rfs1 := rec.Recentfiles()
	rfs2 := rec.Recentfiles()

	// Should be different slices (copy)
	if &rfs1[0] == &rfs2[0] {
		t.Error("Recentfiles() should return a copy, not original slice")
	}

	// But same content
	if len(rfs1) != len(rfs2) {
		t.Error("Recentfiles() copies should have same length")
	}
}

func TestNewNonExistentFile(t *testing.T) {
	_, err := New("/nonexistent/RECENT-1h.yaml")
	if err == nil {
		t.Error("New() should fail for non-existent file")
	}
}

func TestEnsureFilesExistIdempotent(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)

	rec, _ := NewWithPrincipal(principal)

	// Call twice
	if err := rec.EnsureFilesExist(); err != nil {
		t.Fatalf("First EnsureFilesExist failed: %v", err)
	}

	if err := rec.EnsureFilesExist(); err != nil {
		t.Fatalf("Second EnsureFilesExist failed: %v", err)
	}

	// Files should still exist
	for _, interval := range []string{"1h", "6h"} {
		path := filepath.Join(tmpDir, "RECENT-"+interval+".yaml")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("File %s missing after second call", interval)
		}
	}
}

func TestVerbose(t *testing.T) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
	)

	rec, _ := NewWithPrincipal(principal)

	// Set verbose
	rec.Verbose(true)

	rec.mu.RLock()
	if !rec.verbose {
		t.Error("Verbose not set")
	}
	rec.mu.RUnlock()

	// Unset verbose
	rec.Verbose(false)

	rec.mu.RLock()
	if rec.verbose {
		t.Error("Verbose not unset")
	}
	rec.mu.RUnlock()
}

func TestNoAggregator(t *testing.T) {
	tmpDir := t.TempDir()

	// Principal without aggregator
	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		// No aggregator
	)

	rec, err := NewWithPrincipal(principal)
	if err != nil {
		t.Fatalf("NewWithPrincipal failed: %v", err)
	}

	// Should have only principal
	if len(rec.Recentfiles()) != 1 {
		t.Errorf("Recentfiles count = %d, want 1", len(rec.Recentfiles()))
	}

	// Aggregate should not error
	if err := rec.Aggregate(false); err != nil {
		t.Errorf("Aggregate failed: %v", err)
	}
}
