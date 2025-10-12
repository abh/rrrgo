package recent

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/abh/rrrgo/recentfile"
)

// Recent manages a collection of recentfiles covering different time intervals.
// It coordinates the hierarchy: 1h -> 6h -> 1d -> 1W -> 1M -> 1Q -> 1Y -> Z
type Recent struct {
	// The principal (smallest interval) recentfile
	principal *recentfile.Recentfile

	// All recentfiles in the hierarchy, sorted by interval (smallest to largest)
	recentfiles []*recentfile.Recentfile

	// Local root directory
	localRoot string

	// Verbose logging
	verbose bool

	mu sync.RWMutex
}

// New creates a Recent collection from a principal recentfile path.
// The principal file must exist and contain aggregator configuration.
func New(principalPath string) (*Recent, error) {
	// Load the principal recentfile
	principal, err := recentfile.NewFromFile(principalPath)
	if err != nil {
		return nil, fmt.Errorf("load principal: %w", err)
	}

	// Get local root from principal's directory
	localRoot := filepath.Dir(principalPath)

	// Create Recent collection
	r := &Recent{
		principal: principal,
		localRoot: localRoot,
	}

	// Initialize recentfile hierarchy
	if err := r.initializeHierarchy(); err != nil {
		return nil, fmt.Errorf("initialize hierarchy: %w", err)
	}

	return r, nil
}

// NewWithPrincipal creates a Recent collection with an in-memory principal.
// This is useful for creating new hierarchies or testing.
func NewWithPrincipal(principal *recentfile.Recentfile) (*Recent, error) {
	if principal == nil {
		return nil, fmt.Errorf("principal cannot be nil")
	}

	r := &Recent{
		principal: principal,
		localRoot: principal.LocalRoot(),
	}

	// Initialize recentfile hierarchy
	if err := r.initializeHierarchy(); err != nil {
		return nil, fmt.Errorf("initialize hierarchy: %w", err)
	}

	return r, nil
}

// initializeHierarchy creates recentfile objects for all intervals in the aggregator.
func (r *Recent) initializeHierarchy() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Start with principal
	r.recentfiles = []*recentfile.Recentfile{r.principal}

	// Get aggregator intervals
	meta := r.principal.Meta()
	aggregator := meta.Aggregator

	if len(aggregator) == 0 {
		// No aggregation configured, only principal
		return nil
	}

	// Create recentfile objects for each aggregator interval
	principalInterval := r.principal.Interval()
	for _, interval := range aggregator {
		// Skip if this interval is the same as principal (avoid duplicates)
		if interval == principalInterval {
			continue
		}
		rf := r.principal.SparseClone()
		rf.SetInterval(interval)
		r.recentfiles = append(r.recentfiles, rf)
	}

	// Sort by interval duration (smallest to largest)
	sort.Slice(r.recentfiles, func(i, j int) bool {
		return r.recentfiles[i].IntervalSecs() < r.recentfiles[j].IntervalSecs()
	})

	return nil
}

// PrincipalRecentfile returns the principal (smallest interval) recentfile.
func (r *Recent) PrincipalRecentfile() *recentfile.Recentfile {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.principal
}

// Recentfiles returns all recentfiles in the hierarchy, sorted by interval.
func (r *Recent) Recentfiles() []*recentfile.Recentfile {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make([]*recentfile.Recentfile, len(r.recentfiles))
	copy(result, r.recentfiles)
	return result
}

// RecentfileByInterval returns the recentfile for a specific interval.
// Returns nil if the interval is not in the hierarchy.
func (r *Recent) RecentfileByInterval(interval string) *recentfile.Recentfile {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, rf := range r.recentfiles {
		if rf.Interval() == interval {
			return rf
		}
	}
	return nil
}

// LocalRoot returns the local root directory.
func (r *Recent) LocalRoot() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.localRoot
}

// Intervals returns the list of all intervals in the hierarchy.
func (r *Recent) Intervals() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	intervals := make([]string, len(r.recentfiles))
	for i, rf := range r.recentfiles {
		intervals[i] = rf.Interval()
	}
	return intervals
}

// Update adds or updates a single file event in the principal recentfile.
func (r *Recent) Update(path, eventType string, dirtyEpoch ...recentfile.Epoch) error {
	principal := r.PrincipalRecentfile()
	return principal.Update(path, eventType, dirtyEpoch...)
}

// BatchUpdate processes multiple events in the principal recentfile.
func (r *Recent) BatchUpdate(batch []recentfile.BatchItem) error {
	principal := r.PrincipalRecentfile()
	return principal.BatchUpdate(batch)
}

// Aggregate runs aggregation on the principal recentfile.
// This will merge events into larger intervals as configured.
func (r *Recent) Aggregate(force bool) error {
	principal := r.PrincipalRecentfile()
	return principal.Aggregate(force)
}

// EnsureFilesExist ensures all recentfiles in the hierarchy exist on disk.
// If they don't exist, creates empty files with appropriate metadata.
func (r *Recent) EnsureFilesExist() error {
	for _, rf := range r.Recentfiles() {
		rfile := rf.Rfile()

		// Check if file exists
		if _, err := os.Stat(rfile); os.IsNotExist(err) {
			// File doesn't exist, create it
			if r.verbose {
				fmt.Printf("Creating %s\n", rfile)
			}

			// Lock and write empty file
			if err := rf.Lock(); err != nil {
				return fmt.Errorf("lock %s: %w", rf.Interval(), err)
			}

			if err := rf.Write(); err != nil {
				rf.Unlock()
				return fmt.Errorf("write %s: %w", rf.Interval(), err)
			}

			rf.Unlock()
		}
	}

	return nil
}

// LoadAll loads all recentfiles from disk.
// This is useful to refresh the in-memory state.
func (r *Recent) LoadAll() error {
	for _, rf := range r.Recentfiles() {
		rfile := rf.Rfile()

		// Skip if file doesn't exist
		if _, err := os.Stat(rfile); os.IsNotExist(err) {
			continue
		}

		// Read the file
		if err := rf.Read(); err != nil {
			return fmt.Errorf("read %s: %w", rf.Interval(), err)
		}
	}

	return nil
}

// Verbose sets verbose logging.
func (r *Recent) Verbose(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.verbose = v
}

// Stats returns statistics about the Recent collection.
func (r *Recent) Stats() Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := Stats{
		Intervals: len(r.recentfiles),
		Files:     make(map[string]FileStats),
	}

	for _, rf := range r.recentfiles {
		interval := rf.Interval()
		events := rf.RecentEvents()

		fs := FileStats{
			Interval: interval,
			Events:   len(events),
		}

		// Get file info if it exists
		if info, err := os.Stat(rf.Rfile()); err == nil {
			fs.Size = info.Size()
			fs.Mtime = info.ModTime().Unix()
		}

		stats.Files[interval] = fs
		stats.TotalEvents += fs.Events
	}

	return stats
}

// Stats represents statistics about a Recent collection.
type Stats struct {
	Intervals   int                  // Number of intervals
	TotalEvents int                  // Total events across all files
	Files       map[string]FileStats // Per-file statistics
}

// FileStats represents statistics for a single recentfile.
type FileStats struct {
	Interval string // e.g., "1h", "6h"
	Events   int    // Number of events
	Size     int64  // File size in bytes
	Mtime    int64  // Last modification time (Unix timestamp)
}

// Validate checks the consistency of the Recent collection.
// Returns a list of validation errors, or nil if everything is valid.
func (r *Recent) Validate() []error {
	var errors []error

	// Check that principal exists
	if r.principal == nil {
		errors = append(errors, fmt.Errorf("principal recentfile is nil"))
		return errors
	}

	// Check that recentfiles list contains principal
	found := false
	for _, rf := range r.Recentfiles() {
		if rf == r.principal {
			found = true
			break
		}
	}
	if !found {
		errors = append(errors, fmt.Errorf("principal not in recentfiles list"))
	}

	// Check that intervals are sorted and unique
	rfs := r.Recentfiles()
	for i := 1; i < len(rfs); i++ {
		prevDuration := rfs[i-1].IntervalSecs()
		currDuration := rfs[i].IntervalSecs()
		if currDuration == prevDuration {
			errors = append(errors, fmt.Errorf("duplicate interval: %s (%d seconds) appears multiple times",
				rfs[i].Interval(), currDuration))
		} else if currDuration < prevDuration {
			errors = append(errors, fmt.Errorf("intervals not sorted: %s (%d) should come before %s (%d)",
				rfs[i].Interval(), currDuration,
				rfs[i-1].Interval(), prevDuration))
		}
	}

	// Check that all recentfiles have same local root
	for _, rf := range rfs {
		if rf.LocalRoot() != r.localRoot {
			errors = append(errors, fmt.Errorf("recentfile %s has wrong local root: %s != %s",
				rf.Interval(), rf.LocalRoot(), r.localRoot))
		}
	}

	// Check that all recentfiles have same aggregator config
	principalAgg := r.principal.Meta().Aggregator
	for _, rf := range rfs {
		if rf == r.principal {
			continue
		}
		rfAgg := rf.Meta().Aggregator
		if len(rfAgg) != len(principalAgg) {
			errors = append(errors, fmt.Errorf("recentfile %s has different aggregator length",
				rf.Interval()))
		}
	}

	return errors
}

// String returns a string representation of the Recent collection.
func (r *Recent) String() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return fmt.Sprintf("Recent{root=%s, principal=%s, intervals=%d}",
		r.localRoot,
		r.principal.Interval(),
		len(r.recentfiles))
}
