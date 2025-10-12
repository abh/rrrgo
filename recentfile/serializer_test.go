package recentfile

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestYAMLSerializer(t *testing.T) {
	rf := &Recentfile{
		meta: MetaData{
			Protocol:         1,
			Interval:         "1h",
			Filenameroot:     "RECENT",
			SerializerSuffix: ".yaml",
			Dirtymark:        1234567890.123456,
			Aggregator:       []string{"6h", "1d"},
		},
		recent: []Event{
			{Epoch: 1234567890.123456, Path: "foo/bar.txt", Type: "new"},
			{Epoch: 1234567889.111111, Path: "baz/qux.txt", Type: "new"},
		},
	}

	// Marshal
	serializer := &YAMLSerializer{}
	data, err := serializer.Marshal(rf)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("Marshal produced empty data")
	}

	// Unmarshal
	sd, err := serializer.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare metadata
	if sd.Meta.Protocol != rf.meta.Protocol {
		t.Errorf("Protocol = %d, want %d", sd.Meta.Protocol, rf.meta.Protocol)
	}
	if sd.Meta.Interval != rf.meta.Interval {
		t.Errorf("Interval = %s, want %s", sd.Meta.Interval, rf.meta.Interval)
	}
	if sd.Meta.Dirtymark != rf.meta.Dirtymark {
		t.Errorf("Dirtymark = %s, want %s", sd.Meta.Dirtymark, rf.meta.Dirtymark)
	}

	// Compare events
	if len(sd.Recent) != len(rf.recent) {
		t.Fatalf("Recent count = %d, want %d", len(sd.Recent), len(rf.recent))
	}

	for i, event := range sd.Recent {
		if event.Epoch != rf.recent[i].Epoch {
			t.Errorf("Event[%d].Epoch = %s, want %s", i, event.Epoch, rf.recent[i].Epoch)
		}
		if event.Path != rf.recent[i].Path {
			t.Errorf("Event[%d].Path = %s, want %s", i, event.Path, rf.recent[i].Path)
		}
		if event.Type != rf.recent[i].Type {
			t.Errorf("Event[%d].Type = %s, want %s", i, event.Type, rf.recent[i].Type)
		}
	}
}

func TestJSONSerializer(t *testing.T) {
	rf := &Recentfile{
		meta: MetaData{
			Protocol:         1,
			Interval:         "6h",
			Filenameroot:     "RECENT",
			SerializerSuffix: ".json",
			Dirtymark:        1234567890.654321,
		},
		recent: []Event{
			{Epoch: 1234567890.654321, Path: "test.txt", Type: "new"},
		},
	}

	// Marshal
	serializer := &JSONSerializer{}
	data, err := serializer.Marshal(rf)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("Marshal produced empty data")
	}

	// Unmarshal
	sd, err := serializer.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare
	if sd.Meta.Protocol != rf.meta.Protocol {
		t.Errorf("Protocol = %d, want %d", sd.Meta.Protocol, rf.meta.Protocol)
	}
	if len(sd.Recent) != len(rf.recent) {
		t.Errorf("Recent count = %d, want %d", len(sd.Recent), len(rf.recent))
	}
}

func TestGetSerializer(t *testing.T) {
	tests := []struct {
		suffix  string
		wantErr bool
	}{
		{".yaml", false},
		{".yml", false},
		{".json", false},
		{".xml", true},
		{".txt", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.suffix, func(t *testing.T) {
			s, err := GetSerializer(tt.suffix)
			if tt.wantErr {
				if err == nil {
					t.Error("GetSerializer() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("GetSerializer() unexpected error: %v", err)
				}
				if s == nil {
					t.Error("GetSerializer() returned nil serializer")
				}
			}
		})
	}
}

func TestWriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a recentfile
	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithFilenameRoot("RECENT"),
		WithAggregator([]string{"6h", "1d"}),
	)

	// Add some events
	rf.mu.Lock()
	rf.recent = []Event{
		{Epoch: 1234567890.123456, Path: "foo/bar.txt", Type: "new"},
		{Epoch: 1234567889.111111, Path: "baz/qux.txt", Type: "delete"},
	}
	rf.meta.Dirtymark = 1234567890.123456
	rf.mu.Unlock()

	// Write
	if err := rf.Write(); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(rf.Rfile()); err != nil {
		t.Errorf("Recentfile not created: %v", err)
	}

	// Read it back
	rf2, err := NewFromFile(rf.Rfile())
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	// Compare
	if rf2.meta.Protocol != rf.meta.Protocol {
		t.Errorf("Protocol = %d, want %d", rf2.meta.Protocol, rf.meta.Protocol)
	}
	if rf2.meta.Interval != rf.meta.Interval {
		t.Errorf("Interval = %s, want %s", rf2.meta.Interval, rf.meta.Interval)
	}
	if rf2.meta.Dirtymark != rf.meta.Dirtymark {
		t.Errorf("Dirtymark = %s, want %s", rf2.meta.Dirtymark, rf.meta.Dirtymark)
	}
	if !reflect.DeepEqual(rf2.meta.Aggregator, rf.meta.Aggregator) {
		t.Errorf("Aggregator = %v, want %v", rf2.meta.Aggregator, rf.meta.Aggregator)
	}

	if len(rf2.recent) != len(rf.recent) {
		t.Fatalf("Recent count = %d, want %d", len(rf2.recent), len(rf.recent))
	}

	for i := range rf2.recent {
		if rf2.recent[i].Epoch != rf.recent[i].Epoch {
			t.Errorf("Event[%d].Epoch = %s, want %s", i, rf2.recent[i].Epoch, rf.recent[i].Epoch)
		}
		if rf2.recent[i].Path != rf.recent[i].Path {
			t.Errorf("Event[%d].Path = %s, want %s", i, rf2.recent[i].Path, rf.recent[i].Path)
		}
		if rf2.recent[i].Type != rf.recent[i].Type {
			t.Errorf("Event[%d].Type = %s, want %s", i, rf2.recent[i].Type, rf.recent[i].Type)
		}
	}
}

func TestAtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	rf.mu.Lock()
	rf.recent = []Event{{Epoch: 1234567890.0, Path: "test.txt", Type: "new"}}
	rf.mu.Unlock()

	// Write
	if err := rf.Write(); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify .new file is gone (atomic rename completed)
	newFile := rf.Rfile() + ".new"
	if _, err := os.Stat(newFile); !os.IsNotExist(err) {
		t.Error(".new file still exists after write")
	}

	// Verify target file exists
	if _, err := os.Stat(rf.Rfile()); err != nil {
		t.Errorf("Target file doesn't exist: %v", err)
	}
}

func TestSplitRfilename(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantRoot string
		wantInt  string
		wantSuf  string
		wantErr  bool
	}{
		{
			name:     "standard yaml",
			filename: "RECENT-1h.yaml",
			wantRoot: "RECENT",
			wantInt:  "1h",
			wantSuf:  ".yaml",
			wantErr:  false,
		},
		{
			name:     "6h interval",
			filename: "RECENT-6h.yaml",
			wantRoot: "RECENT",
			wantInt:  "6h",
			wantSuf:  ".yaml",
			wantErr:  false,
		},
		{
			name:     "json format",
			filename: "RECENT-1d.json",
			wantRoot: "RECENT",
			wantInt:  "1d",
			wantSuf:  ".json",
			wantErr:  false,
		},
		{
			name:     "Z interval",
			filename: "RECENT-Z.yaml",
			wantRoot: "RECENT",
			wantInt:  "Z",
			wantSuf:  ".yaml",
			wantErr:  false,
		},
		{
			name:     "custom root",
			filename: "MYRECENT-1h.yaml",
			wantRoot: "MYRECENT",
			wantInt:  "1h",
			wantSuf:  ".yaml",
			wantErr:  false,
		},
		{
			name:     "invalid format - no interval",
			filename: "RECENT.yaml",
			wantErr:  true,
		},
		{
			name:     "invalid format - no suffix",
			filename: "RECENT-1h",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, interval, suffix, err := SplitRfilename(tt.filename)

			if tt.wantErr {
				if err == nil {
					t.Error("SplitRfilename() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("SplitRfilename() unexpected error: %v", err)
			}

			if root != tt.wantRoot {
				t.Errorf("root = %q, want %q", root, tt.wantRoot)
			}
			if interval != tt.wantInt {
				t.Errorf("interval = %q, want %q", interval, tt.wantInt)
			}
			if suffix != tt.wantSuf {
				t.Errorf("suffix = %q, want %q", suffix, tt.wantSuf)
			}
		})
	}
}

func TestAssertSymlink(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithFilenameRoot("RECENT"),
	)

	// Write the recentfile
	rf.mu.Lock()
	rf.recent = []Event{{Epoch: 1234567890.0, Path: "test.txt", Type: "new"}}
	rf.mu.Unlock()

	if err := rf.Write(); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create symlink
	if err := rf.AssertSymlink(); err != nil {
		t.Fatalf("AssertSymlink failed: %v", err)
	}

	// Verify symlink exists
	symlinkPath := filepath.Join(tmpDir, "RECENT.recent")
	target, err := os.Readlink(symlinkPath)
	if err != nil {
		t.Fatalf("Readlink failed: %v", err)
	}

	expectedTarget := "RECENT-1h.yaml"
	if target != expectedTarget {
		t.Errorf("Symlink points to %q, want %q", target, expectedTarget)
	}

	// Call AssertSymlink again - should be idempotent
	if err := rf.AssertSymlink(); err != nil {
		t.Fatalf("Second AssertSymlink failed: %v", err)
	}

	// Verify symlink still correct
	target, err = os.Readlink(symlinkPath)
	if err != nil {
		t.Fatalf("Readlink failed after second call: %v", err)
	}
	if target != expectedTarget {
		t.Errorf("Symlink changed to %q, want %q", target, expectedTarget)
	}
}

func TestNewFromFileInvalidPath(t *testing.T) {
	// Try to read non-existent file
	_, err := NewFromFile("/nonexistent/RECENT-1h.yaml")
	if err == nil {
		t.Error("NewFromFile() should fail for non-existent file")
	}
}

func TestMinmaxAndMergedInfo(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Set minmax and merged info
	rf.mu.Lock()
	rf.meta.Minmax = &MinmaxInfo{
		Max:   1234567890.0,
		Min:   1234567880.0,
		Mtime: 1234567890,
	}
	rf.meta.Merged = &MergedInfo{
		Epoch:        1234567885.0,
		IntoInterval: "6h",
	}
	rf.recent = []Event{{Epoch: 1234567890.0, Path: "test.txt", Type: "new"}}
	rf.mu.Unlock()

	// Write and read back
	if err := rf.Write(); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	rf2, err := NewFromFile(rf.Rfile())
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	// Verify minmax
	if rf2.meta.Minmax == nil {
		t.Fatal("Minmax is nil")
	}
	if rf2.meta.Minmax.Max != rf.meta.Minmax.Max {
		t.Errorf("Minmax.Max = %s, want %s", rf2.meta.Minmax.Max, rf.meta.Minmax.Max)
	}
	if rf2.meta.Minmax.Min != rf.meta.Minmax.Min {
		t.Errorf("Minmax.Min = %s, want %s", rf2.meta.Minmax.Min, rf.meta.Minmax.Min)
	}

	// Verify merged
	if rf2.meta.Merged == nil {
		t.Fatal("Merged is nil")
	}
	if rf2.meta.Merged.Epoch != rf.meta.Merged.Epoch {
		t.Errorf("Merged.Epoch = %s, want %s", rf2.meta.Merged.Epoch, rf.meta.Merged.Epoch)
	}
}

func TestProducersField(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Set producers (note uppercase P)
	rf.mu.Lock()
	rf.meta.Producers = map[string]interface{}{
		"github.com/abh/rrrgo": "0.1.0",
		"$0":                                  "/usr/bin/rrr-server",
		"time":                                1234567890.123456,
	}
	rf.recent = []Event{{Epoch: 1234567890.0, Path: "test.txt", Type: "new"}}
	rf.mu.Unlock()

	// Write and read back
	if err := rf.Write(); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	rf2, err := NewFromFile(rf.Rfile())
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	// Verify producers
	if rf2.meta.Producers == nil {
		t.Fatal("Producers is nil")
	}
	if len(rf2.meta.Producers) != len(rf.meta.Producers) {
		t.Errorf("Producers count = %d, want %d", len(rf2.meta.Producers), len(rf.meta.Producers))
	}
}

func TestDetectFormatSymlinkYAML(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a YAML recentfile
	yamlFile := filepath.Join(tmpDir, "RECENT-1h.yaml")
	if err := os.WriteFile(yamlFile, []byte("meta:\n  interval: 1h\n"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create symlink to it
	symlinkPath := filepath.Join(tmpDir, "RECENT.recent")
	if err := os.Symlink("RECENT-1h.yaml", symlinkPath); err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	// Detect format from symlink
	suffix, err := detectFormat(symlinkPath)
	if err != nil {
		t.Fatalf("detectFormat failed: %v", err)
	}

	if suffix != ".yaml" {
		t.Errorf("suffix = %q, want %q", suffix, ".yaml")
	}
}

func TestDetectFormatSymlinkJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a JSON recentfile
	jsonFile := filepath.Join(tmpDir, "RECENT-1h.json")
	if err := os.WriteFile(jsonFile, []byte(`{"meta":{"interval":"1h"}}`), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create symlink to it
	symlinkPath := filepath.Join(tmpDir, "RECENT.recent")
	if err := os.Symlink("RECENT-1h.json", symlinkPath); err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	// Detect format from symlink
	suffix, err := detectFormat(symlinkPath)
	if err != nil {
		t.Fatalf("detectFormat failed: %v", err)
	}

	if suffix != ".json" {
		t.Errorf("suffix = %q, want %q", suffix, ".json")
	}
}

func TestDetectFormatContentJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a .recent file with JSON content
	recentFile := filepath.Join(tmpDir, "RECENT.recent")
	if err := os.WriteFile(recentFile, []byte(`{"meta":{"interval":"1h"}}`), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Detect format from content
	suffix, err := detectFormat(recentFile)
	if err != nil {
		t.Fatalf("detectFormat failed: %v", err)
	}

	if suffix != ".json" {
		t.Errorf("suffix = %q, want %q", suffix, ".json")
	}
}

func TestDetectFormatContentYAML(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a .recent file with YAML content
	recentFile := filepath.Join(tmpDir, "RECENT.recent")
	if err := os.WriteFile(recentFile, []byte("meta:\n  interval: 1h\n"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Detect format from content
	suffix, err := detectFormat(recentFile)
	if err != nil {
		t.Fatalf("detectFormat failed: %v", err)
	}

	if suffix != ".yaml" {
		t.Errorf("suffix = %q, want %q", suffix, ".yaml")
	}
}

func TestDetectFormatEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create empty file
	emptyFile := filepath.Join(tmpDir, "RECENT.recent")
	if err := os.WriteFile(emptyFile, []byte(""), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Detect format - should default to YAML
	suffix, err := detectFormat(emptyFile)
	if err != nil {
		t.Fatalf("detectFormat failed: %v", err)
	}

	if suffix != ".yaml" {
		t.Errorf("suffix = %q, want %q (default for empty file)", suffix, ".yaml")
	}
}

func TestDetectFormatMissingFile(t *testing.T) {
	// Try to detect format of non-existent file
	_, err := detectFormat("/nonexistent/RECENT.recent")
	if err == nil {
		t.Error("detectFormat should fail for non-existent file")
	}
}

func TestNewFromFileWithRecentSymlinkYAML(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a recentfile
	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithFilenameRoot("RECENT"),
	)

	// Add events and write
	rf.mu.Lock()
	rf.recent = []Event{{Epoch: 1234567890.0, Path: "test.txt", Type: "new"}}
	rf.mu.Unlock()

	if err := rf.Write(); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create symlink
	if err := rf.AssertSymlink(); err != nil {
		t.Fatalf("AssertSymlink failed: %v", err)
	}

	// Read via symlink
	symlinkPath := filepath.Join(tmpDir, "RECENT.recent")
	rf2, err := NewFromFile(symlinkPath)
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	// Verify metadata
	if rf2.interval != "1h" {
		t.Errorf("interval = %s, want 1h", rf2.interval)
	}
	if rf2.serializerSuffix != ".yaml" {
		t.Errorf("serializerSuffix = %s, want .yaml", rf2.serializerSuffix)
	}

	// Verify events
	if len(rf2.recent) != 1 {
		t.Errorf("event count = %d, want 1", len(rf2.recent))
	}
}

func TestNewFromFileWithRecentSymlinkJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a JSON recentfile
	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
		WithFilenameRoot("RECENT"),
		WithSerializerSuffix(".json"),
	)

	// Add events and write
	rf.mu.Lock()
	rf.recent = []Event{{Epoch: 1234567890.0, Path: "test.txt", Type: "new"}}
	rf.mu.Unlock()

	if err := rf.Write(); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create symlink
	if err := rf.AssertSymlink(); err != nil {
		t.Fatalf("AssertSymlink failed: %v", err)
	}

	// Read via symlink
	symlinkPath := filepath.Join(tmpDir, "RECENT.recent")
	rf2, err := NewFromFile(symlinkPath)
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	// Verify metadata
	if rf2.interval != "1h" {
		t.Errorf("interval = %s, want 1h", rf2.interval)
	}
	if rf2.serializerSuffix != ".json" {
		t.Errorf("serializerSuffix = %s, want .json", rf2.serializerSuffix)
	}

	// Verify events
	if len(rf2.recent) != 1 {
		t.Errorf("event count = %d, want 1", len(rf2.recent))
	}
}

func TestNewFromFileWithRecentRegularFileYAML(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a .recent file directly with YAML content
	recentPath := filepath.Join(tmpDir, "RECENT.recent")
	yamlContent := `meta:
  protocol: 1
  filenameroot: RECENT
  interval: 1h
  serializer_suffix: .yaml
recent:
  - epoch: 1234567890.0
    path: test.txt
    type: new
`
	if err := os.WriteFile(recentPath, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Read via .recent file
	rf, err := NewFromFile(recentPath)
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	// Verify metadata
	if rf.interval != "1h" {
		t.Errorf("interval = %s, want 1h", rf.interval)
	}
	if rf.serializerSuffix != ".yaml" {
		t.Errorf("serializerSuffix = %s, want .yaml", rf.serializerSuffix)
	}
	if rf.filenameRoot != "RECENT" {
		t.Errorf("filenameRoot = %s, want RECENT", rf.filenameRoot)
	}

	// Verify events
	if len(rf.recent) != 1 {
		t.Errorf("event count = %d, want 1", len(rf.recent))
	}
}

func TestNewFromFileWithRecentRegularFileJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a .recent file directly with JSON content
	recentPath := filepath.Join(tmpDir, "RECENT.recent")
	jsonContent := `{
  "meta": {
    "protocol": 1,
    "filenameroot": "RECENT",
    "interval": "1h",
    "serializer_suffix": ".json"
  },
  "recent": [
    {
      "epoch": 1234567890.0,
      "path": "test.txt",
      "type": "new"
    }
  ]
}`
	if err := os.WriteFile(recentPath, []byte(jsonContent), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Read via .recent file
	rf, err := NewFromFile(recentPath)
	if err != nil {
		t.Fatalf("NewFromFile failed: %v", err)
	}

	// Verify metadata
	if rf.interval != "1h" {
		t.Errorf("interval = %s, want 1h", rf.interval)
	}
	if rf.serializerSuffix != ".json" {
		t.Errorf("serializerSuffix = %s, want .json", rf.serializerSuffix)
	}
	if rf.filenameRoot != "RECENT" {
		t.Errorf("filenameRoot = %s, want RECENT", rf.filenameRoot)
	}

	// Verify events
	if len(rf.recent) != 1 {
		t.Errorf("event count = %d, want 1", len(rf.recent))
	}
}

func TestStreamEventsWithRecentFile(t *testing.T) {
	tmpDir := t.TempDir()
	recentPath := filepath.Join(tmpDir, "RECENT.recent")

	// Write a JSON .recent file
	content := `{"meta":{"interval":"1h"},"recent":[{"epoch":1.0,"path":"f.txt","type":"new"}]}`
	if err := os.WriteFile(recentPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	// Should auto-detect format and stream successfully
	stats, err := StreamEvents(recentPath, 0, nil)
	if err != nil {
		t.Fatalf("StreamEvents(.recent file) failed: %v", err)
	}
	if stats.EventCount != 1 {
		t.Errorf("EventCount = %d, want 1", stats.EventCount)
	}
}
