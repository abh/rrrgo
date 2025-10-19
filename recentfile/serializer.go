package recentfile

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Serializer is the interface for marshaling and unmarshaling recentfiles.
type Serializer interface {
	Marshal(rf *Recentfile) ([]byte, error)
	Unmarshal(data []byte) (*SerializedData, error)
}

// SerializedData represents the on-disk format of a recentfile.
type SerializedData struct {
	Meta   MetaData `yaml:"meta" json:"meta"`
	Recent []Event  `yaml:"recent" json:"recent"`
}

// YAMLSerializer handles YAML serialization.
type YAMLSerializer struct{}

// Marshal serializes a recentfile to YAML bytes.
func (s *YAMLSerializer) Marshal(rf *Recentfile) ([]byte, error) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	data := SerializedData{
		Meta:   rf.meta,
		Recent: rf.recent,
	}

	return yaml.Marshal(&data)
}

// Unmarshal deserializes YAML bytes to SerializedData.
func (s *YAMLSerializer) Unmarshal(data []byte) (*SerializedData, error) {
	var sd SerializedData
	if err := yaml.Unmarshal(data, &sd); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}
	return &sd, nil
}

// JSONSerializer handles JSON serialization.
type JSONSerializer struct{}

// Marshal serializes a recentfile to JSON bytes.
func (s *JSONSerializer) Marshal(rf *Recentfile) ([]byte, error) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	data := SerializedData{
		Meta:   rf.meta,
		Recent: rf.recent,
	}

	return json.MarshalIndent(&data, "", "  ")
}

// Unmarshal deserializes JSON bytes to SerializedData.
func (s *JSONSerializer) Unmarshal(data []byte) (*SerializedData, error) {
	var sd SerializedData
	if err := json.Unmarshal(data, &sd); err != nil {
		return nil, fmt.Errorf("unmarshal json: %w", err)
	}
	return &sd, nil
}

// GetSerializer returns the appropriate serializer for the given suffix.
func GetSerializer(suffix string) (Serializer, error) {
	switch suffix {
	case ".yaml", ".yml":
		return &YAMLSerializer{}, nil
	case ".json":
		return &JSONSerializer{}, nil
	default:
		return nil, fmt.Errorf("unsupported serializer suffix: %s", suffix)
	}
}

// Marshal serializes a recentfile using its configured serializer.
func (rf *Recentfile) Marshal() ([]byte, error) {
	serializer, err := GetSerializer(rf.serializerSuffix)
	if err != nil {
		return nil, err
	}
	return serializer.Marshal(rf)
}

// Unmarshal deserializes data into a recentfile using the given suffix.
func Unmarshal(data []byte, suffix string) (*SerializedData, error) {
	serializer, err := GetSerializer(suffix)
	if err != nil {
		return nil, err
	}
	return serializer.Unmarshal(data)
}

// detectFormat attempts to detect the serialization format of a RECENT file.
// It first tries to resolve symlinks, then falls back to content sniffing.
// Returns the detected suffix (e.g., ".yaml", ".json") and any error.
func detectFormat(path string) (string, error) {
	// Check if file exists
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("stat %s: %w", path, err)
	}

	// Try to resolve symlink
	target, err := os.Readlink(path)
	if err == nil {
		// It's a symlink - try to extract suffix from target
		_, _, suffix, err := SplitRfilename(target)
		if err == nil {
			return suffix, nil
		}
		// If parsing failed, fall through to content sniffing
	}

	// Not a symlink or couldn't parse target - read content
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}

	// Empty file - default to YAML
	if len(data) == 0 {
		return ".yaml", nil
	}

	// Read first 512 bytes max for detection
	sample := data
	if len(sample) > 512 {
		sample = sample[:512]
	}

	// Trim whitespace and check first character
	trimmed := string(sample)
	for i, c := range trimmed {
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			if trimmed[i] == '{' {
				return ".json", nil
			}
			break
		}
	}

	// Default to YAML
	return ".yaml", nil
}

// Write writes the recentfile atomically to disk.
// Writes to a temporary file (.new), then renames to the target.
func (rf *Recentfile) Write() error {
	// Marshal the data
	data, err := rf.Marshal()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	// Get the target file path
	rfile := rf.Rfile()

	// Ensure parent directory exists
	dir := filepath.Dir(rfile)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	// Write to temporary file
	tmpfile := rfile + ".new"
	if err := os.WriteFile(tmpfile, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmpfile, err)
	}

	// Atomic rename
	if err := os.Rename(tmpfile, rfile); err != nil {
		os.Remove(tmpfile) // Clean up on failure
		return fmt.Errorf("rename %s to %s: %w", tmpfile, rfile, err)
	}

	return nil
}

// Read reads the recentfile from disk.
func (rf *Recentfile) Read() error {
	rfile := rf.Rfile()

	// Read file
	data, err := os.ReadFile(rfile)
	if err != nil {
		return fmt.Errorf("read %s: %w", rfile, err)
	}

	// Unmarshal
	sd, err := Unmarshal(data, rf.serializerSuffix)
	if err != nil {
		return fmt.Errorf("unmarshal %s: %w", rfile, err)
	}

	// Update recentfile
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.meta = sd.Meta
	rf.recent = sd.Recent

	// Update internal state from metadata
	rf.interval = sd.Meta.Interval
	rf.filenameRoot = sd.Meta.Filenameroot
	rf.serializerSuffix = sd.Meta.SerializerSuffix

	return nil
}

// NewFromFile reads a recentfile from disk.
func NewFromFile(path string) (*Recentfile, error) {
	filename := filepath.Base(path)

	var root, interval, suffix string
	var err error

	// Check if this is a .recent file
	if filepath.Ext(filename) == ".recent" {
		// Auto-detect format
		suffix, err = detectFormat(path)
		if err != nil {
			return nil, fmt.Errorf("detect format for %s: %w", filename, err)
		}

		// Read file to extract metadata
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}

		// Unmarshal to get metadata
		sd, err := Unmarshal(data, suffix)
		if err != nil {
			return nil, fmt.Errorf("unmarshal %s: %w", path, err)
		}

		// Extract values from metadata
		root = sd.Meta.Filenameroot
		interval = sd.Meta.Interval
		suffix = sd.Meta.SerializerSuffix

		// Create recentfile with metadata values
		rf := &Recentfile{
			localRoot:        filepath.Dir(path),
			rfile:            path,
			interval:         interval,
			filenameRoot:     root,
			serializerSuffix: suffix,
			meta:             sd.Meta,
			recent:           sd.Recent,
		}

		// Initialize done tracker
		rf.done = &Done{
			rfInterval: interval,
		}

		return rf, nil
	}

	// Existing logic for standard filenames
	root, interval, suffix, err = SplitRfilename(filename)
	if err != nil {
		return nil, fmt.Errorf("parse filename %s: %w", filename, err)
	}

	// Create recentfile with basic config
	rf := &Recentfile{
		localRoot:        filepath.Dir(path),
		rfile:            path,
		interval:         interval,
		filenameRoot:     root,
		serializerSuffix: suffix,
		meta: MetaData{
			Protocol:         1,
			Filenameroot:     root,
			Interval:         interval,
			SerializerSuffix: suffix,
		},
	}

	// Initialize done tracker
	rf.done = &Done{
		rfInterval: interval,
	}

	// Read the file
	if err := rf.Read(); err != nil {
		return nil, err
	}

	return rf, nil
}

// AssertSymlink creates or updates the RECENT.recent symlink to point to this recentfile.
// This is used for the principal recentfile so clients can find it easily.
func (rf *Recentfile) AssertSymlink() error {
	dir := filepath.Dir(rf.Rfile())
	symlinkPath := filepath.Join(dir, rf.filenameRoot+".recent")

	// Get the target (just the filename, not full path)
	target := rf.Rfilename()

	// Check if symlink exists and points to correct target
	if existing, err := os.Readlink(symlinkPath); err == nil {
		if existing == target {
			return nil // Already correct
		}
	}

	// Create temporary symlink
	tmpSymlink := symlinkPath + ".tmp"
	os.Remove(tmpSymlink) // Remove if exists

	if err := os.Symlink(target, tmpSymlink); err != nil {
		return fmt.Errorf("create symlink %s -> %s: %w", tmpSymlink, target, err)
	}

	// Atomic rename
	if err := os.Rename(tmpSymlink, symlinkPath); err != nil {
		os.Remove(tmpSymlink)
		return fmt.Errorf("rename symlink %s to %s: %w", tmpSymlink, symlinkPath, err)
	}

	return nil
}

// StreamStats contains statistics from streaming through a RECENT file.
type StreamStats struct {
	Meta       MetaData
	EventCount int
	FileSize   int64
}

// StreamEventCallback is called for each batch of events during streaming.
// Return false to stop streaming, true to continue.
type StreamEventCallback func(events []Event) bool

// StreamEvents streams through events in a RECENT file without loading all into memory.
// It processes events in batches and calls the callback for each batch.
// batchSize: number of events to accumulate before calling callback (0 = no callback)
// Returns metadata, total event count, and file size.
func StreamEvents(path string, batchSize int, callback StreamEventCallback) (stats *StreamStats, err error) {
	filename := filepath.Base(path)
	var suffix string

	// Check if this is a .recent file
	if filepath.Ext(filename) == ".recent" {
		// Auto-detect format
		suffix, err = detectFormat(path)
		if err != nil {
			return nil, fmt.Errorf("detect format for %s: %w", filename, err)
		}
	} else {
		// Parse filename to get format
		_, _, suffix, err = SplitRfilename(filename)
		if err != nil {
			return nil, fmt.Errorf("parse filename %s: %w", filename, err)
		}
	}

	// Open file
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close %s: %w", path, closeErr)
		}
	}()

	// Get file size
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}

	stats = &StreamStats{
		FileSize: fi.Size(),
	}

	// Stream based on format
	switch suffix {
	case ".json":
		return streamEventsJSON(f, stats, batchSize, callback)
	case ".yaml", ".yml":
		return streamEventsYAML(f, stats, batchSize, callback)
	default:
		return nil, fmt.Errorf("unsupported format: %s", suffix)
	}
}

// streamEventsJSON streams events from a JSON file.
func streamEventsJSON(r io.Reader, stats *StreamStats, batchSize int, callback StreamEventCallback) (*StreamStats, error) {
	dec := json.NewDecoder(r)

	// Read opening brace
	t, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("read opening: %w", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("expected '{', got %v", t)
	}

	var batch []Event
	eventCount := 0

	for dec.More() {
		// Read field name
		t, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("read field name: %w", err)
		}
		fieldName, ok := t.(string)
		if !ok {
			return nil, fmt.Errorf("expected field name, got %v", t)
		}

		switch fieldName {
		case "meta":
			// Parse metadata
			if err := dec.Decode(&stats.Meta); err != nil {
				return nil, fmt.Errorf("decode meta: %w", err)
			}

		case "recent":
			// Read opening bracket for events array
			t, err := dec.Token()
			if err != nil {
				return nil, fmt.Errorf("read events array: %w", err)
			}
			if delim, ok := t.(json.Delim); !ok || delim != '[' {
				return nil, fmt.Errorf("expected '[', got %v", t)
			}

			// Stream through events
			for dec.More() {
				var event Event
				if err := dec.Decode(&event); err != nil {
					return nil, fmt.Errorf("decode event %d: %w", eventCount, err)
				}

				eventCount++

				if callback != nil && batchSize > 0 {
					batch = append(batch, event)
					if len(batch) >= batchSize {
						if !callback(batch) {
							// Callback requested stop
							stats.EventCount = eventCount
							return stats, nil
						}
						batch = batch[:0] // Clear batch
					}
				}
			}

			// Process remaining events in batch
			if callback != nil && len(batch) > 0 {
				callback(batch)
			}

			// Read closing bracket
			t, err = dec.Token()
			if err != nil {
				return nil, fmt.Errorf("read events closing bracket: %w", err)
			}
			if delim, ok := t.(json.Delim); !ok || delim != ']' {
				return nil, fmt.Errorf("expected ']', got %v", t)
			}

		default:
			// Skip unknown fields
			var v interface{}
			if err := dec.Decode(&v); err != nil {
				return nil, fmt.Errorf("skip field %s: %w", fieldName, err)
			}
		}
	}

	// Read closing brace
	t, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("read closing: %w", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '}' {
		return nil, fmt.Errorf("expected '}', got %v", t)
	}

	stats.EventCount = eventCount
	return stats, nil
}

// streamEventsYAML streams events from a YAML file.
// Note: YAML doesn't have native streaming support, so we load the whole file.
// For YAML files, use the standard Read() method for small files.
func streamEventsYAML(r io.Reader, stats *StreamStats, batchSize int, callback StreamEventCallback) (*StreamStats, error) {
	// YAML doesn't support streaming, so read entire file
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read yaml: %w", err)
	}

	var sd SerializedData
	if err := yaml.Unmarshal(data, &sd); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	stats.Meta = sd.Meta
	stats.EventCount = len(sd.Recent)

	// Process events in batches if callback provided
	if callback != nil && batchSize > 0 {
		for i := 0; i < len(sd.Recent); i += batchSize {
			end := i + batchSize
			if end > len(sd.Recent) {
				end = len(sd.Recent)
			}
			if !callback(sd.Recent[i:end]) {
				break
			}
		}
	}

	return stats, nil
}

// ValidateFile validates a RECENT file's structure without loading all events into memory.
// Returns metadata, event count, and any errors.
func ValidateFile(path string) (*StreamStats, error) {
	// Stream through file without processing events
	return StreamEvents(path, 0, nil)
}
