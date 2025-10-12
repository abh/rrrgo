package recentfile

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.ntppool.org/common/version"
)

// Recentfile represents a single RECENT file covering a specific time interval.
type Recentfile struct {
	// Metadata about this recentfile
	meta MetaData

	// Recent events (sorted by epoch descending)
	recent []Event

	// Internal state
	localRoot        string
	rfile            string // cached full path
	interval         string // e.g., "1h", "6h"
	filenameRoot     string // e.g., "RECENT"
	serializerSuffix string // e.g., ".yaml"

	// Locking
	locked      bool
	lockDir     string
	lockTimeout time.Duration

	// Done tracking
	done *Done

	// Flags
	verbose    bool
	verboseLog string

	mu sync.RWMutex
}

// MetaData contains metadata about a recentfile.
type MetaData struct {
	Aggregator       []string               `yaml:"aggregator,omitempty" json:"aggregator,omitempty"`
	Canonize         string                 `yaml:"canonize,omitempty" json:"canonize,omitempty"`
	Comment          string                 `yaml:"comment,omitempty" json:"comment,omitempty"`
	Dirtymark        Epoch                  `yaml:"dirtymark,omitempty" json:"dirtymark,omitempty"`
	Filenameroot     string                 `yaml:"filenameroot" json:"filenameroot"`
	Interval         string                 `yaml:"interval" json:"interval"`
	Merged           *MergedInfo            `yaml:"merged,omitempty" json:"merged,omitempty"`
	Minmax           *MinmaxInfo            `yaml:"minmax,omitempty" json:"minmax,omitempty"`
	Protocol         int                    `yaml:"protocol" json:"protocol"`
	SerializerSuffix string                 `yaml:"serializer_suffix" json:"serializer_suffix"`
	Producers        map[string]interface{} `yaml:"Producers,omitempty" json:"Producers,omitempty"` // uppercase!
}

// MergedInfo tracks when this recentfile was merged into a larger interval.
type MergedInfo struct {
	Epoch        Epoch  `yaml:"epoch" json:"epoch"`
	Time         Epoch  `yaml:"time,omitempty" json:"time,omitempty"`                   // not used
	IntoInterval string `yaml:"into_interval,omitempty" json:"into_interval,omitempty"` // not used
}

// MinmaxInfo tracks the timestamp range covered by this recentfile.
type MinmaxInfo struct {
	Max   Epoch `yaml:"max" json:"max"`
	Min   Epoch `yaml:"min" json:"min"`
	Mtime int64 `yaml:"mtime,omitempty" json:"mtime,omitempty"`
}

// Event represents a single file system event.
type Event struct {
	Epoch Epoch  `yaml:"epoch" json:"epoch"`
	Path  string `yaml:"path" json:"path"`
	Type  string `yaml:"type" json:"type"` // "new" or "delete"
}

// BatchItem is used for batch updates.
type BatchItem struct {
	Path  string
	Type  string // "new" or "delete"
	Epoch Epoch  // optional dirty epoch
}

// Option is a functional option for configuring a Recentfile.
type Option func(*Recentfile)

// WithInterval sets the interval for this recentfile.
func WithInterval(interval string) Option {
	return func(rf *Recentfile) {
		rf.interval = interval
		rf.meta.Interval = interval
	}
}

// WithFilenameRoot sets the filename root (e.g., "RECENT").
func WithFilenameRoot(root string) Option {
	return func(rf *Recentfile) {
		rf.filenameRoot = root
		rf.meta.Filenameroot = root
	}
}

// WithLocalRoot sets the local root directory.
func WithLocalRoot(root string) Option {
	return func(rf *Recentfile) {
		rf.localRoot = root
	}
}

// WithVerbose sets verbose logging.
func WithVerbose(v bool) Option {
	return func(rf *Recentfile) {
		rf.verbose = v
	}
}

// WithAggregator sets the aggregator intervals.
func WithAggregator(agg []string) Option {
	return func(rf *Recentfile) {
		rf.meta.Aggregator = agg
	}
}

// WithSerializerSuffix sets the serializer suffix.
func WithSerializerSuffix(suffix string) Option {
	return func(rf *Recentfile) {
		rf.serializerSuffix = suffix
		rf.meta.SerializerSuffix = suffix
	}
}

// New creates a new Recentfile with the given options.
func New(opts ...Option) *Recentfile {
	rf := &Recentfile{
		filenameRoot:     "RECENT",
		serializerSuffix: ".yaml",
		lockTimeout:      600 * time.Second,
		meta: MetaData{
			Protocol:         1,
			Filenameroot:     "RECENT",
			SerializerSuffix: ".yaml",
		},
	}

	for _, opt := range opts {
		opt(rf)
	}

	// Initialize done tracker
	rf.done = &Done{
		rfInterval: rf.interval,
	}

	// Set initial producers information
	rf.updateProducers()

	return rf
}

// Rfile returns the full path to this recentfile.
func (rf *Recentfile) Rfile() string {
	rf.mu.RLock()
	if rf.rfile != "" {
		rfile := rf.rfile
		rf.mu.RUnlock()
		return rfile
	}
	rf.mu.RUnlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.rfile = filepath.Join(rf.localRoot, rf.Rfilename())
	return rf.rfile
}

// Rfilename returns just the filename (root-interval.suffix).
func (rf *Recentfile) Rfilename() string {
	return fmt.Sprintf("%s-%s%s", rf.filenameRoot, rf.interval, rf.serializerSuffix)
}

// SplitRfilename parses a filename into its components.
// Expected format: "RECENT-1h.yaml" -> root="RECENT", interval="1h", suffix=".yaml"
func SplitRfilename(name string) (root, interval, suffix string, err error) {
	// Pattern: root-interval.suffix
	re := regexp.MustCompile(`^(.+)-([^-\.]+)(\.[^\.]+)$`)
	matches := re.FindStringSubmatch(name)
	if len(matches) != 4 {
		return "", "", "", fmt.Errorf("invalid recentfile name: %s", name)
	}

	return matches[1], matches[2], matches[3], nil
}

// Interval returns the interval string for this recentfile.
func (rf *Recentfile) Interval() string {
	return rf.interval
}

// LocalRoot returns the local root directory.
func (rf *Recentfile) LocalRoot() string {
	return rf.localRoot
}

// SetLocalRoot sets the local root directory.
func (rf *Recentfile) SetLocalRoot(root string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.localRoot = root
	rf.rfile = "" // clear cached path
}

// SetInterval sets the interval.
func (rf *Recentfile) SetInterval(interval string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.interval = interval
	rf.meta.Interval = interval
	rf.rfile = "" // clear cached path
}

// Meta returns the metadata.
func (rf *Recentfile) Meta() MetaData {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.meta
}

// RecentEvents returns the events slice.
func (rf *Recentfile) RecentEvents() []Event {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// Return a copy to prevent external modification
	events := make([]Event, len(rf.recent))
	copy(events, rf.recent)
	return events
}

// Interval parsing constants
const (
	SecondSeconds  int64 = 1
	MinuteSeconds  int64 = 60
	HourSeconds    int64 = 3600
	DaySeconds     int64 = 86400
	WeekSeconds    int64 = 604800
	MonthSeconds   int64 = 2592000  // 30 days
	QuarterSeconds int64 = 7776000  // 90 days
	YearSeconds    int64 = 31557600 // 365.25 days
	ZSeconds       int64 = math.MaxInt64
)

// IntervalSecs returns the duration of this interval in seconds.
func (rf *Recentfile) IntervalSecs() int64 {
	return IntervalSecsFor(rf.interval)
}

// IntervalSecsFor returns duration for arbitrary interval string.
// Examples: "1h" -> 3600, "6h" -> 21600, "Z" -> MaxInt64
func IntervalSecsFor(interval string) int64 {
	if interval == "" {
		return 0
	}

	if interval == "Z" {
		return ZSeconds
	}

	// Parse count and unit
	re := regexp.MustCompile(`^(\d*)([smhdWMQY])$`)
	matches := re.FindStringSubmatch(interval)
	if len(matches) != 3 {
		return 0
	}

	countStr := matches[1]
	unit := matches[2]

	count := int64(1)
	if countStr != "" {
		fmt.Sscanf(countStr, "%d", &count)
	}

	var unitSecs int64
	switch unit {
	case "s":
		unitSecs = SecondSeconds
	case "m":
		unitSecs = MinuteSeconds
	case "h":
		unitSecs = HourSeconds
	case "d":
		unitSecs = DaySeconds
	case "W":
		unitSecs = WeekSeconds
	case "M":
		unitSecs = MonthSeconds
	case "Q":
		unitSecs = QuarterSeconds
	case "Y":
		unitSecs = YearSeconds
	default:
		return 0
	}

	return count * unitSecs
}

// LocalPath combines localroot with a relative path from an event.
func (rf *Recentfile) LocalPath(path string) string {
	if path == "" {
		return rf.localRoot
	}
	// Split on slashes and use filepath.Join for OS compatibility
	parts := strings.Split(path, "/")
	return filepath.Join(append([]string{rf.localRoot}, parts...)...)
}

// NaivePathNormalize canonicalizes a path by removing double slashes,
// resolving ./ and ../, and removing trailing slashes.
func NaivePathNormalize(path string) string {
	// Remove double slashes: // -> /
	re := regexp.MustCompile(`/+`)
	path = re.ReplaceAllString(path, "/")

	// Remove /./ sequences
	for strings.Contains(path, "/./") {
		path = strings.ReplaceAll(path, "/./", "/")
	}

	// Resolve /../ sequences
	for strings.Contains(path, "/../") {
		re := regexp.MustCompile(`/[^/]+/\.\./`)
		path = re.ReplaceAllString(path, "/")
	}

	// Remove trailing slash
	path = strings.TrimSuffix(path, "/")

	return path
}

// SparseClone creates a shallow copy with shared config but different interval.
// Used when creating recentfiles for aggregator intervals.
func (rf *Recentfile) SparseClone() *Recentfile {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	clone := &Recentfile{
		localRoot:        rf.localRoot,
		filenameRoot:     rf.filenameRoot,
		serializerSuffix: rf.serializerSuffix,
		lockTimeout:      rf.lockTimeout,
		verbose:          rf.verbose,
		verboseLog:       rf.verboseLog,
		meta: MetaData{
			Aggregator:       rf.meta.Aggregator,
			Protocol:         rf.meta.Protocol,
			Filenameroot:     rf.meta.Filenameroot,
			SerializerSuffix: rf.meta.SerializerSuffix,
			Comment:          rf.meta.Comment,
			Canonize:         rf.meta.Canonize,
		},
	}

	clone.done = &Done{
		rfInterval: clone.interval,
	}

	return clone
}

// Done returns the Done tracker for this recentfile.
func (rf *Recentfile) Done() *Done {
	return rf.done
}

// Verbose returns the verbose flag.
func (rf *Recentfile) Verbose() bool {
	return rf.verbose
}

// SetVerbose sets the verbose flag.
func (rf *Recentfile) SetVerbose(v bool) {
	rf.verbose = v
}

// Update adds or updates a single file event.
// path: Full path to the file (will be canonicalized relative to localRoot)
// eventType: "new" or "delete"
// dirtyEpoch: Optional epoch to use (for backdated events)
func (rf *Recentfile) Update(path, eventType string, dirtyEpoch ...Epoch) error {
	// Build batch item
	item := BatchItem{
		Path: path,
		Type: eventType,
	}
	if len(dirtyEpoch) > 0 {
		item.Epoch = dirtyEpoch[0]
	}

	// Use BatchUpdate for consistency
	return rf.BatchUpdate([]BatchItem{item})
}

// BatchUpdate processes multiple events efficiently.
func (rf *Recentfile) BatchUpdate(batch []BatchItem) error {
	if len(batch) == 0 {
		return nil
	}

	// Lock the recentfile
	if err := rf.Lock(); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer rf.Unlock()

	// Read current events (if file exists)
	_ = rf.Read() // Ignore error if file doesn't exist yet

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Canonicalize paths and assign epochs
	now := EpochNow()
	processedBatch := make([]Event, 0, len(batch))

	// We need to track the working list of events to ensure monotonicity
	workingEvents := make([]Event, len(rf.recent))
	copy(workingEvents, rf.recent)

	for _, item := range batch {
		// Canonicalize path
		canonPath, err := rf.canonizePath(item.Path)
		if err != nil {
			return fmt.Errorf("canonize path %s: %w", item.Path, err)
		}

		// Assign epoch
		var epoch Epoch
		if !item.Epoch.IsZero() && EpochLt(item.Epoch, now) {
			// Dirty epoch (backdated)
			epoch = rf.ensureMonotonic(item.Epoch, workingEvents)
			// Set dirtymark
			rf.meta.Dirtymark = now
			// Clear merged info (forces re-aggregation)
			rf.meta.Merged = nil
		} else {
			// Current epoch
			epoch = rf.ensureMonotonic(now, workingEvents)
		}

		newEvent := Event{
			Epoch: epoch,
			Path:  canonPath,
			Type:  item.Type,
		}
		processedBatch = append(processedBatch, newEvent)

		// Add to working events so next iteration sees it for monotonicity
		workingEvents = append([]Event{newEvent}, workingEvents...)
	}

	// Remove duplicates of paths in processedBatch from current events
	pathSet := make(map[string]bool)
	for _, event := range processedBatch {
		pathSet[event.Path] = true
	}

	newRecent := make([]Event, 0, len(rf.recent)+len(processedBatch))
	for _, event := range rf.recent {
		if !pathSet[event.Path] {
			newRecent = append(newRecent, event)
		}
	}

	// Add new events
	newRecent = append(newRecent, processedBatch...)

	// Sort by epoch descending
	rf.sortEventsByEpoch(newRecent)

	// Truncate old events
	rf.recent = rf.truncate(newRecent)

	// Update minmax
	rf.updateMinmax()

	// Update producers to reflect current Go implementation
	rf.updateProducers()

	// Write to disk
	rf.mu.Unlock()
	if err := rf.Write(); err != nil {
		rf.mu.Lock()
		return fmt.Errorf("write: %w", err)
	}
	rf.mu.Lock()

	// Update symlink (if this is the principal file)
	rf.mu.Unlock()
	if err := rf.AssertSymlink(); err != nil {
		rf.mu.Lock()
		// Non-fatal, just log
		if rf.verbose {
			fmt.Fprintf(os.Stderr, "warn: assert symlink: %v\n", err)
		}
	} else {
		rf.mu.Lock()
	}

	return nil
}

// canonizePath removes the localroot prefix and normalizes the path.
func (rf *Recentfile) canonizePath(path string) (string, error) {
	// Remove localroot prefix
	path = strings.TrimPrefix(path, rf.localRoot)
	path = strings.TrimPrefix(path, "/")

	// Apply canonize method (default: naive_path_normalize)
	if rf.meta.Canonize == "" || rf.meta.Canonize == "naive_path_normalize" {
		path = NaivePathNormalize(path)
	}

	return path, nil
}

// ensureMonotonic ensures the epoch is greater than the most recent epoch.
func (rf *Recentfile) ensureMonotonic(epoch Epoch, events []Event) Epoch {
	if len(events) == 0 {
		return epoch
	}

	// If new epoch <= most recent epoch, increment it
	if EpochLe(epoch, events[0].Epoch) {
		return EpochIncreaseABit(events[0].Epoch)
	}

	return epoch
}

// sortEventsByEpoch sorts events by epoch descending (in-place).
func (rf *Recentfile) sortEventsByEpoch(events []Event) {
	// Simple insertion sort (good for mostly-sorted data)
	for i := 1; i < len(events); i++ {
		j := i
		for j > 0 && EpochLt(events[j-1].Epoch, events[j].Epoch) {
			events[j-1], events[j] = events[j], events[j-1]
			j--
		}
	}
}

// truncate removes events outside the interval window.
func (rf *Recentfile) truncate(events []Event) []Event {
	if len(events) == 0 {
		return events
	}

	// Calculate cutoff epoch
	var cutoff Epoch
	if rf.meta.Merged != nil && !rf.meta.Merged.Epoch.IsZero() {
		// Use merged epoch as cutoff
		cutoff = rf.meta.Merged.Epoch
	} else {
		// Calculate cutoff based on interval
		intervalSecs := rf.IntervalSecs()
		if intervalSecs == ZSeconds {
			// Z interval keeps everything
			return events
		}

		now := EpochNow()
		nowFloat := EpochToFloat(now)
		cutoffFloat := nowFloat - float64(intervalSecs)
		cutoff = EpochFromFloat(cutoffFloat)
	}

	// Find first event >= cutoff
	result := make([]Event, 0, len(events))
	for _, event := range events {
		if EpochGe(event.Epoch, cutoff) {
			result = append(result, event)
		}
	}

	return result
}

// updateMinmax updates the min/max metadata based on current events.
func (rf *Recentfile) updateMinmax() {
	if len(rf.recent) == 0 {
		rf.meta.Minmax = nil
		return
	}

	// Events are sorted descending, so first is max, last is min
	rf.meta.Minmax = &MinmaxInfo{
		Max:   rf.recent[0].Epoch,
		Min:   rf.recent[len(rf.recent)-1].Epoch,
		Mtime: time.Now().Unix(),
	}
}

// updateProducers updates the Producers field to reflect the current Go implementation.
func (rf *Recentfile) updateProducers() {
	now := EpochNow()

	// Get executable path
	exePath, err := os.Executable()
	if err != nil {
		// Fallback if we can't get the executable path
		exePath = os.Args[0]
	}

	// Create/update producers map
	rf.meta.Producers = map[string]interface{}{
		"$0":                   exePath,
		"github.com/abh/rrrgo": version.Version(),
		"time":                 EpochToFloat(now),
	}
}
