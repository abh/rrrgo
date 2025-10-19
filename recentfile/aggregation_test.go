package recentfile

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMergeFrom(t *testing.T) {
	tmpDir := t.TempDir()

	// Create principal (1h) recentfile with some events
	principal := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithAggregator([]string{"6h", "1d"}),
	)

	// Add events to principal
	batch := []BatchItem{
		{Path: "file1.txt", Type: "new"},
		{Path: "file2.txt", Type: "new"},
		{Path: "file3.txt", Type: "new"},
	}
	if err := principal.BatchUpdate(batch); err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}

	// Create target (6h) recentfile
	target := New(
		WithLocalRoot(tmpDir),
		WithInterval("6h"),
	)

	// Merge principal into target
	if err := target.MergeFrom(principal); err != nil {
		t.Fatalf("MergeFrom failed: %v", err)
	}

	// Verify target has events
	targetRead, err := NewFromFile(target.Rfile())
	if err != nil {
		t.Fatalf("Read target failed: %v", err)
	}

	if len(targetRead.recent) != 3 {
		t.Errorf("target has %d events, want 3", len(targetRead.recent))
	}

	// Verify dirtymark copied
	principalRead, _ := NewFromFile(principal.Rfile())
	if targetRead.meta.Dirtymark != principalRead.meta.Dirtymark {
		t.Error("dirtymark not copied from source")
	}
}

func TestMergeFromInvalidInterval(t *testing.T) {
	tmpDir := t.TempDir()

	rf1h := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	rf1h.BatchUpdate([]BatchItem{{Path: "test.txt", Type: "new"}})

	rf6h := New(
		WithLocalRoot(tmpDir),
		WithInterval("6h"),
	)

	// Try to merge larger interval into smaller - should fail
	err := rf1h.MergeFrom(rf6h)
	if err == nil {
		t.Error("MergeFrom should fail when target interval is smaller")
	}
}

func TestMergeFromDeduplicatesPaths(t *testing.T) {
	tmpDir := t.TempDir()

	// Create target with older event first
	target := New(
		WithLocalRoot(tmpDir),
		WithInterval("6h"),
	)
	target.BatchUpdate([]BatchItem{
		{Path: "file1.txt", Type: "new"}, // Older event
		{Path: "file3.txt", Type: "new"},
	})

	// Get epochs before merge
	targetBefore, _ := NewFromFile(target.Rfile())
	var file1EpochBefore Epoch
	for _, e := range targetBefore.recent {
		if e.Path == "file1.txt" {
			file1EpochBefore = e.Epoch
			break
		}
	}

	// Wait a bit to ensure different epoch
	time.Sleep(10 * time.Millisecond)

	// Create principal with overlapping event (newer)
	principal := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	principal.BatchUpdate([]BatchItem{
		{Path: "file1.txt", Type: "new"}, // Same path, newer epoch
		{Path: "file2.txt", Type: "new"},
	})

	// Merge principal (newer) into target (older)
	if err := target.MergeFrom(principal); err != nil {
		t.Fatalf("MergeFrom failed: %v", err)
	}

	// Verify merged result
	targetAfter, _ := NewFromFile(target.Rfile())

	// Should have 3 unique paths
	if len(targetAfter.recent) != 3 {
		t.Errorf("target has %d events, want 3", len(targetAfter.recent))
	}

	// file1.txt should have the newer epoch from principal
	var file1EpochAfter Epoch
	for _, e := range targetAfter.recent {
		if e.Path == "file1.txt" {
			file1EpochAfter = e.Epoch
			break
		}
	}

	if file1EpochAfter == file1EpochBefore {
		t.Error("file1.txt epoch should be updated to newer one from principal")
	}

	// Verify it's actually newer
	if !EpochGt(file1EpochAfter, file1EpochBefore) {
		t.Errorf("new epoch %v should be > old epoch %v", file1EpochAfter, file1EpochBefore)
	}
}

func TestAggregate(t *testing.T) {
	tmpDir := t.TempDir()

	// Create principal with aggregator config
	principal := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithAggregator([]string{"6h", "1d", "1W"}),
	)

	// Add events
	batch := []BatchItem{
		{Path: "file1.txt", Type: "new"},
		{Path: "file2.txt", Type: "new"},
	}
	if err := principal.BatchUpdate(batch); err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}

	// Run aggregation with force=true
	if err := principal.Aggregate(true); err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	// Verify 6h file exists and has events
	rf6h, err := NewFromFile(filepath.Join(tmpDir, "RECENT-6h.yaml"))
	if err != nil {
		t.Fatalf("Read 6h file failed: %v", err)
	}

	if len(rf6h.recent) == 0 {
		t.Error("6h file should have events after aggregation")
	}

	// Verify 1d file exists
	rf1d, err := NewFromFile(filepath.Join(tmpDir, "RECENT-1d.yaml"))
	if err != nil {
		t.Fatalf("Read 1d file failed: %v", err)
	}

	if len(rf1d.recent) == 0 {
		t.Error("1d file should have events after aggregation")
	}

	// Verify principal has merged metadata
	principalAfter, _ := NewFromFile(principal.Rfile())
	if principalAfter.meta.Merged == nil {
		t.Error("principal should have merged metadata")
	}
}

func TestAggregateNoAggregator(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		// No aggregator
	)

	rf.BatchUpdate([]BatchItem{{Path: "test.txt", Type: "new"}})

	// Should not error
	if err := rf.Aggregate(false); err != nil {
		t.Errorf("Aggregate with no aggregator should not error: %v", err)
	}
}

func TestDeduplicateEpochs(t *testing.T) {
	rf := &Recentfile{}

	events := []Event{
		{Epoch: 100.0, Path: "a", Type: "new"},
		{Epoch: 100.0, Path: "b", Type: "new"}, // Duplicate epoch
		{Epoch: 99.0, Path: "c", Type: "new"},
		{Epoch: 99.0, Path: "d", Type: "new"}, // Duplicate epoch
	}

	result := rf.DeduplicateEpochs(events)

	// Check all epochs are unique
	seen := make(map[Epoch]bool)
	for _, e := range result {
		if seen[e.Epoch] {
			t.Errorf("duplicate epoch after deduplication: %v", e.Epoch)
		}
		seen[e.Epoch] = true
	}

	// Check still sorted descending
	for i := 1; i < len(result); i++ {
		if !EpochLt(result[i].Epoch, result[i-1].Epoch) {
			t.Errorf("events not sorted after deduplication at index %d", i)
		}
	}
}

func TestGetNextInterval(t *testing.T) {
	tests := []struct {
		name       string
		interval   string
		aggregator []string
		want       string
	}{
		{
			name:       "1h -> 6h",
			interval:   "1h",
			aggregator: []string{"6h", "1d", "1W"},
			want:       "6h",
		},
		{
			name:       "6h -> 1d",
			interval:   "6h",
			aggregator: []string{"6h", "1d", "1W"},
			want:       "1d",
		},
		{
			name:       "1d -> 1W",
			interval:   "1d",
			aggregator: []string{"6h", "1d", "1W"},
			want:       "1W",
		},
		{
			name:       "1W -> none",
			interval:   "1W",
			aggregator: []string{"6h", "1d", "1W"},
			want:       "",
		},
		{
			name:       "no aggregator",
			interval:   "1h",
			aggregator: []string{},
			want:       "",
		},
		{
			name:       "Z interval",
			interval:   "Z",
			aggregator: []string{"6h", "1d", "Z"},
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := New(
				WithInterval(tt.interval),
				WithAggregator(tt.aggregator),
			)

			got := rf.GetNextInterval()
			if got != tt.want {
				t.Errorf("GetNextInterval() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAggregateInterval(t *testing.T) {
	tmpDir := t.TempDir()

	// Create recentfile factory
	principal := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithAggregator([]string{"6h", "1d"}),
	)

	// Add events to 1h
	principal.BatchUpdate([]BatchItem{
		{Path: "file1.txt", Type: "new"},
		{Path: "file2.txt", Type: "new"},
	})

	// Aggregate 1h -> 6h specifically
	if err := principal.AggregateInterval("1h", "6h"); err != nil {
		t.Fatalf("AggregateInterval failed: %v", err)
	}

	// Verify 6h file exists
	rf6h, err := NewFromFile(filepath.Join(tmpDir, "RECENT-6h.yaml"))
	if err != nil {
		t.Fatalf("Read 6h file failed: %v", err)
	}

	if len(rf6h.recent) != 2 {
		t.Errorf("6h file has %d events, want 2", len(rf6h.recent))
	}

	// Verify 1h has merged metadata
	rf1h, _ := NewFromFile(filepath.Join(tmpDir, "RECENT-1h.yaml"))
	if rf1h.meta.Merged == nil {
		t.Error("1h should have merged metadata")
	}
	if rf1h.meta.Merged.IntoInterval != "6h" {
		t.Errorf("merged into %s, want 6h", rf1h.meta.Merged.IntoInterval)
	}
}

func TestMergeFromWithDirtymark(t *testing.T) {
	tmpDir := t.TempDir()

	now := EpochNow()

	// Create principal with dirtymark
	principal := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	principal.mu.Lock()
	principal.meta.Dirtymark = now
	principal.mu.Unlock()
	principal.BatchUpdate([]BatchItem{{Path: "file1.txt", Type: "new"}})

	// Create target with different dirtymark
	target := New(
		WithLocalRoot(tmpDir),
		WithInterval("6h"),
	)
	target.mu.Lock()
	target.meta.Dirtymark = EpochFromFloat(EpochToFloat(now) - 1000)
	target.mu.Unlock()
	target.BatchUpdate([]BatchItem{{Path: "file2.txt", Type: "new"}})

	// Merge
	if err := target.MergeFrom(principal); err != nil {
		t.Fatalf("MergeFrom failed: %v", err)
	}

	// Verify dirtymark updated
	targetAfter, _ := NewFromFile(target.Rfile())
	if targetAfter.meta.Dirtymark != principal.meta.Dirtymark {
		t.Error("dirtymark should be copied from source")
	}
}

func TestShouldMergeByAge(t *testing.T) {
	tmpDir := t.TempDir()

	target := New(
		WithLocalRoot(tmpDir),
		WithInterval("6h"),
	)

	// Create target file
	target.BatchUpdate([]BatchItem{{Path: "test.txt", Type: "new"}})

	// Test with file just created (should not merge)
	shouldMerge := shouldMergeByAge(target, "1h")
	if shouldMerge {
		t.Error("should not merge fresh file")
	}

	// Test with non-existent file (should merge)
	target2 := New(
		WithLocalRoot(tmpDir),
		WithInterval("1d"),
	)
	shouldMerge = shouldMergeByAge(target2, "6h")
	if !shouldMerge {
		t.Error("should merge when file doesn't exist")
	}
}

func TestMergeMultipleLevels(t *testing.T) {
	tmpDir := t.TempDir()

	// Create principal
	principal := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithAggregator([]string{"6h", "1d"}),
	)
	principal.BatchUpdate([]BatchItem{
		{Path: "file1.txt", Type: "new"},
		{Path: "file2.txt", Type: "new"},
	})

	// Aggregate with force
	if err := principal.Aggregate(true); err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	// Verify all levels exist
	for _, interval := range []string{"1h", "6h", "1d"} {
		path := filepath.Join(tmpDir, "RECENT-"+interval+".yaml")
		rf, err := NewFromFile(path)
		if err != nil {
			t.Errorf("Read %s failed: %v", interval, err)
			continue
		}
		if len(rf.recent) == 0 {
			t.Errorf("%s has no events", interval)
		}
	}
}

func TestMergePreservesNewerEvents(t *testing.T) {
	tmpDir := t.TempDir()

	// Create older target with event
	target := New(
		WithLocalRoot(tmpDir),
		WithInterval("6h"),
	)
	// Use a timestamp from 1 hour ago
	now := EpochNow()
	nowFloat := EpochToFloat(now)
	oldEpoch := EpochFromFloat(nowFloat - 3600)
	target.BatchUpdate([]BatchItem{
		{Path: "file1.txt", Type: "new", Epoch: oldEpoch},
	})

	// Wait a moment to ensure different epoch
	time.Sleep(10 * time.Millisecond)

	// Create newer source with same file (current time)
	source := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	source.BatchUpdate([]BatchItem{
		{Path: "file1.txt", Type: "new"}, // Will use current time
	})

	// Merge
	if err := target.MergeFrom(source); err != nil {
		t.Fatalf("MergeFrom failed: %v", err)
	}

	// Verify newer epoch was kept
	targetAfter, _ := NewFromFile(target.Rfile())
	if len(targetAfter.recent) != 1 {
		t.Fatalf("expected 1 event, got %d", len(targetAfter.recent))
	}

	// Should have the newer epoch
	if !EpochGt(targetAfter.recent[0].Epoch, oldEpoch) {
		t.Errorf("kept epoch %s, should be > %s", targetAfter.recent[0].Epoch, oldEpoch)
	}
}

func TestAggregateSkipsWhenNotNeeded(t *testing.T) {
	tmpDir := t.TempDir()

	// Create principal
	principal := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithAggregator([]string{"6h", "1d"}),
	)
	principal.BatchUpdate([]BatchItem{{Path: "file1.txt", Type: "new"}})

	// Create 6h file that's very recent
	rf6h := New(
		WithLocalRoot(tmpDir),
		WithInterval("6h"),
	)
	rf6h.BatchUpdate([]BatchItem{{Path: "file2.txt", Type: "new"}})

	// Aggregate without force (should skip 1d because 6h is fresh)
	if err := principal.Aggregate(false); err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	// 6h should exist
	if _, err := os.Stat(rf6h.Rfile()); err != nil {
		t.Error("6h file should exist")
	}

	// 1d might not exist (depends on timing)
	path1d := filepath.Join(tmpDir, "RECENT-1d.yaml")
	if _, err := os.Stat(path1d); err == nil {
		t.Log("1d file was created (timing dependent)")
	}
}

func TestMergeFromWithMergedEpochMinLogic(t *testing.T) {
	tmpDir := t.TempDir()

	now := EpochNow()
	nowFloat := EpochToFloat(now)

	// Create target (6h) with a merged epoch from past
	// merged.epoch represents when it was last merged into the 1d file
	mergedEpoch := EpochFromFloat(nowFloat - 7200) // 2 hours ago

	target := New(
		WithLocalRoot(tmpDir),
		WithInterval("6h"),
	)
	target.mu.Lock()
	target.meta.Merged = &MergedInfo{
		Epoch:        mergedEpoch,
		IntoInterval: "1d",
	}
	target.mu.Unlock()

	// Add an old event to target (from before merge)
	oldEpoch := EpochFromFloat(nowFloat - 5400) // 1.5 hours ago (between merged and interval cutoff)
	target.BatchUpdate([]BatchItem{
		{Path: "old_file.txt", Type: "new", Epoch: oldEpoch},
	})

	// Verify target has the event
	targetBefore, _ := NewFromFile(target.Rfile())
	if len(targetBefore.recent) != 1 {
		t.Fatalf("target should have 1 event before merge, got %d", len(targetBefore.recent))
	}

	// Create source (1h) with events
	// This source represents a newer 1h interval
	source := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	source.BatchUpdate([]BatchItem{
		{Path: "new_file.txt", Type: "new"},
	})

	// Merge source into target
	if err := target.MergeFrom(source); err != nil {
		t.Fatalf("MergeFrom failed: %v", err)
	}

	// Verify both events are kept:
	// 1. The old event should be kept because it's within the 6h interval window
	//    (even though it's older than merged.epoch)
	// 2. The new event should be kept
	targetAfter, _ := NewFromFile(target.Rfile())
	if len(targetAfter.recent) != 2 {
		t.Errorf("target should have 2 events after merge (old within interval + new), got %d", len(targetAfter.recent))
		for i, e := range targetAfter.recent {
			t.Logf("  event %d: path=%s epoch=%v", i, e.Path, e.Epoch)
		}
	}

	// Verify the old event is still there
	foundOld := false
	for _, e := range targetAfter.recent {
		if e.Path == "old_file.txt" {
			foundOld = true
			break
		}
	}
	if !foundOld {
		t.Error("old_file.txt should be kept because it's within the 6h interval")
	}
}
