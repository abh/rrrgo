package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/abh/rrrgo/recent"
	"github.com/abh/rrrgo/recentfile"
)

func setupTestRecent(t *testing.T) (*recent.Recent, string) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h"}),
	)

	rec, err := recent.NewWithPrincipal(principal)
	if err != nil {
		t.Fatalf("NewWithPrincipal failed: %v", err)
	}

	return rec, tmpDir
}

func TestNew(t *testing.T) {
	rec, _ := setupTestRecent(t)

	w, err := New(rec)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if w == nil {
		t.Fatal("New returned nil watcher")
	}

	if w.recent != rec {
		t.Error("Watcher recent not set correctly")
	}

	if w.rootDir != rec.LocalRoot() {
		t.Errorf("Watcher rootDir = %s, want %s", w.rootDir, rec.LocalRoot())
	}
}

func TestNewNilRecent(t *testing.T) {
	_, err := New(nil)
	if err == nil {
		t.Error("New(nil) should error")
	}
}

func TestStartStop(t *testing.T) {
	rec, _ := setupTestRecent(t)

	w, err := New(rec)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Start
	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify running
	if !w.IsRunning() {
		t.Error("Watcher should be running after Start")
	}

	// Stop
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify stopped
	if w.IsRunning() {
		t.Error("Watcher should not be running after Stop")
	}
}

func TestWatchFileCreation(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	w, _ := New(rec, WithVerbose(false))
	w.Start()
	defer w.Stop()

	// Create a file
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Wait for event to be processed
	time.Sleep(200 * time.Millisecond)

	// Force flush
	w.flushBatch()

	// Check if event was recorded
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(events))
	}

	if events[0].Path != "test.txt" {
		t.Errorf("Event path = %s, want test.txt", events[0].Path)
	}

	if events[0].Type != "new" {
		t.Errorf("Event type = %s, want new", events[0].Type)
	}
}

func TestWatchFileModification(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	// Create file before starting watcher
	testFile := filepath.Join(tmpDir, "test.txt")
	os.WriteFile(testFile, []byte("initial"), 0o644)

	w, _ := New(rec)
	w.Start()
	defer w.Stop()

	// Modify the file
	time.Sleep(100 * time.Millisecond)
	if err := os.WriteFile(testFile, []byte("modified"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Wait and flush
	time.Sleep(200 * time.Millisecond)
	w.flushBatch()

	// Should have recorded the modification
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) < 1 {
		t.Error("Expected at least 1 event for modification")
	}
}

func TestWatchFileDeletion(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	// Create file
	testFile := filepath.Join(tmpDir, "test.txt")
	os.WriteFile(testFile, []byte("test"), 0o644)

	w, _ := New(rec)
	w.Start()
	defer w.Stop()

	time.Sleep(100 * time.Millisecond)

	// Delete the file
	if err := os.Remove(testFile); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Wait and flush
	time.Sleep(200 * time.Millisecond)
	w.flushBatch()

	// Check for delete event
	events := rec.PrincipalRecentfile().RecentEvents()
	found := false
	for _, e := range events {
		if e.Path == "test.txt" && e.Type == "delete" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Delete event not recorded")
	}
}

func TestWatchDirectoryCreation(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	w, _ := New(rec)
	w.Start()
	defer w.Stop()

	// Create a directory
	subDir := filepath.Join(tmpDir, "subdir")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Create a file in the new directory
	testFile := filepath.Join(subDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Wait and flush
	time.Sleep(200 * time.Millisecond)
	w.flushBatch()

	// Should have events for both directory and file
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) < 1 {
		t.Error("Expected events for new directory/file")
	}

	// Check that file in subdirectory was detected
	found := false
	for _, e := range events {
		if e.Path == "subdir/test.txt" {
			found = true
			break
		}
	}

	if !found {
		t.Error("File in new subdirectory not detected")
	}
}

func TestIgnoreRECENTFiles(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	w, _ := New(rec)
	w.Start()
	defer w.Stop()

	// Create RECENT files that should be ignored
	recentFiles := []string{
		"RECENT-1h.yaml",
		"RECENT-6h.yaml",
		"RECENT-1h.yaml.lock",
		"RECENT-1h.yaml.new",
		"RECENT.recent",
	}

	for _, name := range recentFiles {
		path := filepath.Join(tmpDir, name)
		os.WriteFile(path, []byte("test"), 0o644)
	}

	// Wait and flush
	time.Sleep(200 * time.Millisecond)
	w.flushBatch()

	// Should have no events (all ignored)
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) > 0 {
		t.Errorf("Expected 0 events (RECENT files should be ignored), got %d", len(events))
		for _, e := range events {
			t.Logf("  Event: %s (%s)", e.Path, e.Type)
		}
	}
}

func TestBatchDeduplication(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	w, _ := New(rec,
		WithBatchSize(100),
		WithBatchDelay(500*time.Millisecond))
	w.Start()
	defer w.Stop()

	testFile := filepath.Join(tmpDir, "test.txt")

	// Write to the same file multiple times rapidly
	for i := 0; i < 10; i++ {
		os.WriteFile(testFile, []byte("iteration"), 0o644)
		time.Sleep(10 * time.Millisecond)
	}

	// Wait and flush
	time.Sleep(200 * time.Millisecond)
	w.flushBatch()

	// Should have only 1 event (deduplicated)
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) != 1 {
		t.Errorf("Expected 1 event (deduplicated), got %d", len(events))
	}
}

func TestBatchSizeFlush(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	// Small batch size to trigger flush
	w, _ := New(rec,
		WithBatchSize(5),
		WithBatchDelay(10*time.Second)) // Long delay, should flush by size
	w.Start()
	defer w.Stop()

	// Create more files than batch size
	for i := 0; i < 10; i++ {
		testFile := filepath.Join(tmpDir, "test"+string(rune('0'+i))+".txt")
		os.WriteFile(testFile, []byte("test"), 0o644)
	}

	// Wait for events to be processed
	time.Sleep(500 * time.Millisecond)
	w.flushBatch() // Force final flush

	// Should have recorded all files
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) < 5 {
		t.Errorf("Expected at least 5 events, got %d", len(events))
	}
}

func TestBatchDelayFlush(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	// Short delay to trigger flush
	w, _ := New(rec,
		WithBatchSize(1000),                  // Large batch size
		WithBatchDelay(200*time.Millisecond)) // Short delay
	w.Start()
	defer w.Stop()

	testFile := filepath.Join(tmpDir, "test.txt")
	os.WriteFile(testFile, []byte("test"), 0o644)

	// Wait for delay flush
	time.Sleep(400 * time.Millisecond)

	// Should have been flushed by delay
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) != 1 {
		t.Errorf("Expected 1 event (flushed by delay), got %d", len(events))
	}
}

func TestStats(t *testing.T) {
	rec, _ := setupTestRecent(t)

	w, _ := New(rec)
	w.Start()
	defer w.Stop()

	stats := w.Stats()

	// Stats should be available
	if stats.TimeSinceFlush < 0 {
		t.Error("TimeSinceFlush should be non-negative")
	}

	// QueuedEvents and BatchSize should be 0 or positive
	if stats.QueuedEvents < 0 {
		t.Error("QueuedEvents should be non-negative")
	}

	if stats.BatchSize < 0 {
		t.Error("BatchSize should be non-negative")
	}
}

func TestWithOptions(t *testing.T) {
	rec, _ := setupTestRecent(t)

	errorCalled := false
	errorHandler := func(err error) {
		errorCalled = true
	}

	w, _ := New(rec,
		WithBatchSize(100),
		WithBatchDelay(2*time.Second),
		WithVerbose(true),
		WithErrorHandler(errorHandler))

	if w.batchSize != 100 {
		t.Errorf("batchSize = %d, want 100", w.batchSize)
	}

	if w.batchDelay != 2*time.Second {
		t.Errorf("batchDelay = %v, want 2s", w.batchDelay)
	}

	if !w.verbose {
		t.Error("verbose not set")
	}

	if w.errorHandler == nil {
		t.Error("errorHandler not set")
	}

	// Test error handler
	w.errorHandler(nil)
	if !errorCalled {
		t.Error("errorHandler not called")
	}
}

func TestDeduplicateBatch(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	w, _ := New(rec)

	batch := []recentfile.BatchItem{
		{Path: filepath.Join(tmpDir, "file1.txt"), Type: "new"},
		{Path: filepath.Join(tmpDir, "file2.txt"), Type: "new"},
		{Path: filepath.Join(tmpDir, "file1.txt"), Type: "new"}, // Duplicate
		{Path: filepath.Join(tmpDir, "file3.txt"), Type: "new"},
		{Path: filepath.Join(tmpDir, "file2.txt"), Type: "delete"}, // Update to delete
	}

	deduped := w.deduplicateBatch(batch)

	// Should have 3 unique paths
	if len(deduped) != 3 {
		t.Errorf("deduplicateBatch result length = %d, want 3", len(deduped))
	}

	// Check that file2.txt has type "delete" (last event)
	found := false
	for _, item := range deduped {
		if item.Path == filepath.Join(tmpDir, "file2.txt") {
			if item.Type != "delete" {
				t.Errorf("file2.txt type = %s, want delete", item.Type)
			}
			found = true
		}
	}

	if !found {
		t.Error("file2.txt not found in deduplicated batch")
	}
}

func TestIsRunning(t *testing.T) {
	rec, _ := setupTestRecent(t)

	w, _ := New(rec)

	// Not running initially
	if w.IsRunning() {
		t.Error("Watcher should not be running before Start")
	}

	w.Start()

	// Running after start
	if !w.IsRunning() {
		t.Error("Watcher should be running after Start")
	}

	w.Stop()

	// Not running after stop
	if w.IsRunning() {
		t.Error("Watcher should not be running after Stop")
	}
}

func TestMultipleFiles(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	w, _ := New(rec)
	w.Start()
	defer w.Stop()

	// Create multiple files
	for i := 0; i < 20; i++ {
		testFile := filepath.Join(tmpDir, "test"+string(rune('a'+i))+".txt")
		os.WriteFile(testFile, []byte("test"), 0o644)
	}

	// Wait and flush
	time.Sleep(300 * time.Millisecond)
	w.flushBatch()

	// Should have events for all files
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) < 10 {
		t.Errorf("Expected at least 10 events, got %d", len(events))
	}
}

func TestStopFlushesRemainingEvents(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	w, _ := New(rec,
		WithBatchSize(1000),            // Large batch size
		WithBatchDelay(10*time.Second)) // Long delay
	w.Start()

	testFile := filepath.Join(tmpDir, "test.txt")
	os.WriteFile(testFile, []byte("test"), 0o644)

	// Wait briefly for event to be queued
	time.Sleep(100 * time.Millisecond)

	// Stop should flush remaining events
	w.Stop()

	// Should have the event even though we didn't wait for delay
	events := rec.PrincipalRecentfile().RecentEvents()
	if len(events) != 1 {
		t.Errorf("Expected 1 event after Stop, got %d", len(events))
	}
}

func TestSymlinksNotFollowed(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	// Create a directory outside the watch tree
	externalDir := filepath.Join(tmpDir, "..", "external")
	os.Mkdir(externalDir, 0o755)
	defer os.RemoveAll(externalDir)

	// Create a symlink inside the watch tree
	symlinkPath := filepath.Join(tmpDir, "symlink")
	if err := os.Symlink(externalDir, symlinkPath); err != nil {
		t.Skipf("Cannot create symlink: %v", err)
	}

	w, _ := New(rec)

	// Should not fail when encountering symlink
	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer w.Stop()

	// Create file in external directory
	externalFile := filepath.Join(externalDir, "test.txt")
	os.WriteFile(externalFile, []byte("test"), 0o644)

	// Wait and flush
	time.Sleep(200 * time.Millisecond)
	w.flushBatch()

	// Should not have event for file in symlinked directory
	events := rec.PrincipalRecentfile().RecentEvents()
	for _, e := range events {
		if e.Path == "symlink/test.txt" {
			t.Error("Should not watch files in symlinked directories")
		}
	}
}

func TestIgnoreTemporaryFiles(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	w, _ := New(rec)
	w.Start()
	defer w.Stop()

	// Create temporary files that should be ignored
	tempFiles := []string{
		".FRMRecent-RECENT-1h.yaml-bVoi.yaml", // Dot prefix - Perl temp file
		".02STAMP.IMOhgo",                     // Dot prefix - temp stamp
		"RECENT.recent.tmp",                   // .tmp suffix
		".hidden.txt",                         // Any dot prefix
	}

	// Create normal file that should NOT be ignored
	normalFile := "data.txt"

	// Create all files
	for _, name := range tempFiles {
		path := filepath.Join(tmpDir, name)
		os.WriteFile(path, []byte("test"), 0o644)
	}
	os.WriteFile(filepath.Join(tmpDir, normalFile), []byte("test"), 0o644)

	// Wait and flush
	time.Sleep(200 * time.Millisecond)
	w.flushBatch()

	events := rec.PrincipalRecentfile().RecentEvents()

	// Should have only 1 event (the normal file)
	if len(events) != 1 {
		t.Errorf("Expected 1 event (normal file only), got %d", len(events))
		for _, e := range events {
			t.Logf("  Event: %s (%s)", e.Path, e.Type)
		}
	}

	// Verify the normal file was recorded
	if len(events) > 0 && events[0].Path != normalFile {
		t.Errorf("Event path = %s, want %s", events[0].Path, normalFile)
	}
}
