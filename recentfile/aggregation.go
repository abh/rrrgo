package recentfile

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"time"
)

// Aggregate merges this recentfile into larger interval files.
// This should be called on the principal (smallest interval) file.
// It will merge into each aggregator interval in sequence.
func (rf *Recentfile) Aggregate(force bool) error {
	// Get aggregator intervals
	aggregator := rf.meta.Aggregator
	if len(aggregator) == 0 {
		return nil // No aggregation configured
	}

	// Sort intervals by duration (smallest to largest)
	intervals := make([]string, len(aggregator))
	copy(intervals, aggregator)
	sort.Slice(intervals, func(i, j int) bool {
		return IntervalSecsFor(intervals[i]) < IntervalSecsFor(intervals[j])
	})

	// Filter to intervals >= current interval
	myDuration := rf.IntervalSecs()
	targetIntervals := []string{}
	for _, interval := range intervals {
		if IntervalSecsFor(interval) > myDuration {
			targetIntervals = append(targetIntervals, interval)
		}
	}

	if len(targetIntervals) == 0 {
		return nil // No larger intervals to aggregate into
	}

	// Track interval of the level BEFORE current source (for age checking)
	// Perl: uses $aggs[$i-1]{object} to check against previous level's interval
	prevSourceInterval := rf.interval

	// Create aggregation chain (Bug #3 fix)
	// Each level merges from the previous level, not all from principal
	source := rf

	// Aggregate into each target interval
	for _, targetInterval := range targetIntervals {
		// Create sparse clone for target interval from PREVIOUS level
		target := source.SparseClone()
		target.SetInterval(targetInterval)

		// Decide if we should merge
		// First iteration (source is principal): always merge
		// Later iterations: check if target file is old enough
		shouldMerge := force || source.interval == rf.interval
		if !shouldMerge {
			// Check target file age vs PREVIOUS source's interval duration
			// Perl: $next_age > $prev->interval_secs (prev = level before current source)
			shouldMerge = shouldMergeByAge(target, prevSourceInterval)
		}

		if !shouldMerge {
			// Skip remaining intervals
			break
		}

		// Perform the merge from previous level (not always from principal)
		if err := target.MergeFrom(source); err != nil {
			return fmt.Errorf("merge into %s: %w", targetInterval, err)
		}

		// Update source's merged metadata
		source.mu.Lock()
		if len(target.recent) > 0 {
			source.meta.Merged = &MergedInfo{
				Epoch:        target.recent[0].Epoch,
				IntoInterval: targetInterval,
			}
		}
		source.mu.Unlock()

		// Write source file to persist merged metadata (needed for next aggregation cycle)
		if err := source.Lock(); err != nil {
			return fmt.Errorf("lock source %s: %w", source.interval, err)
		}
		if err := source.Write(); err != nil {
			source.Unlock()
			return fmt.Errorf("write source %s: %w", source.interval, err)
		}
		source.Unlock()

		// Save current source's interval before moving to next level
		prevSourceInterval = source.interval
		// Use target as source for next iteration (creates the chain)
		source = target
	}

	return nil
}

// MergeFrom merges events from the source recentfile into this (larger interval) recentfile.
// This recentfile (rf) should have a larger interval than the source.
func (rf *Recentfile) MergeFrom(source *Recentfile) error {
	// Sanity check: target interval should be larger than source
	if rf.IntervalSecs() <= source.IntervalSecs() {
		return fmt.Errorf("cannot merge %s into %s (target must be larger)",
			source.interval, rf.interval)
	}

	// Lock both files
	if err := rf.Lock(); err != nil {
		return fmt.Errorf("lock target: %w", err)
	}
	defer rf.Unlock()

	if err := source.Lock(); err != nil {
		return fmt.Errorf("lock source: %w", err)
	}
	defer source.Unlock()

	// Read both files (ignore error if target doesn't exist yet)
	if err := rf.Read(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read target: %w", err)
	}

	if err := source.Read(); err != nil {
		return fmt.Errorf("read source: %w", err)
	}

	rf.mu.Lock()
	source.mu.RLock()

	// Calculate oldest allowed epoch
	// IMPORTANT: Check dirtymark BEFORE copying (Perl does comparison before assignment)
	var oldestAllowed Epoch
	if rf.meta.Dirtymark != source.meta.Dirtymark {
		// Dirtymarks differ, keep everything
		oldestAllowed = 0
	} else if rf.meta.Merged != nil && !rf.meta.Merged.Epoch.IsZero() {
		// Target has merged metadata - calculate cutoff
		// Perl: } elsif (my $merged = $self->merged) {
		now := EpochNow()
		nowFloat := EpochToFloat(now)
		intervalSecs := rf.IntervalSecs()
		var intervalCutoff Epoch
		if intervalSecs != ZSeconds {
			cutoffFloat := nowFloat - float64(intervalSecs)
			intervalCutoff = EpochFromFloat(cutoffFloat)
		}

		// Use minimum of interval cutoff and merged epoch
		// Perl: $oldest_allowed = min($epoch - $secs, $merged->{epoch}||0)
		mergedEpoch := rf.meta.Merged.Epoch
		if !intervalCutoff.IsZero() && EpochLt(intervalCutoff, mergedEpoch) {
			oldestAllowed = intervalCutoff
		} else {
			oldestAllowed = mergedEpoch
		}

		// Adjust if source has older events than oldest_allowed
		// Perl: if (@$other_recent && $other_recent->[-1]{epoch} < $oldest_allowed)
		// If source's oldest event is older than our cutoff, use it instead (more permissive)
		if len(source.recent) > 0 {
			sourceOldest := source.recent[len(source.recent)-1].Epoch
			if !oldestAllowed.IsZero() && EpochLt(sourceOldest, oldestAllowed) {
				oldestAllowed = sourceOldest
			}
		}
	} else {
		// No merged metadata - keep everything (first merge)
		// Perl: $oldest_allowed stays at 0 if no merged metadata exists
		oldestAllowed = 0
	}

	// Merge events from both
	mergedEvents := make(map[string]Event) // path -> event

	// Add events from target (rf) - filter old events like Perl does
	for _, event := range rf.recent {
		// Skip old events from target (Bug #2 fix)
		if !oldestAllowed.IsZero() && EpochLt(event.Epoch, oldestAllowed) {
			continue
		}
		mergedEvents[event.Path] = event
	}

	// Add/update events from source
	for _, event := range source.recent {
		// Check if event is old enough to skip
		if !oldestAllowed.IsZero() && EpochLt(event.Epoch, oldestAllowed) {
			continue
		}

		// Check if we should keep this event
		if existing, ok := mergedEvents[event.Path]; ok {
			// Path exists, keep the newer one
			if EpochGt(event.Epoch, existing.Epoch) {
				mergedEvents[event.Path] = event
			}
		} else {
			// New path
			mergedEvents[event.Path] = event
		}
	}

	// Convert map to slice
	newRecent := make([]Event, 0, len(mergedEvents))
	for _, event := range mergedEvents {
		// Handle delete events for Z interval
		if rf.interval == "Z" && event.Type == "delete" {
			// Optionally skip delete events in Z file
			// For now, keep them (configurable in future)
		}
		newRecent = append(newRecent, event)
	}

	// Sort by epoch descending
	rf.sortEventsByEpoch(newRecent)

	// Handle epoch conflicts (very rare)
	newRecent = rf.DeduplicateEpochs(newRecent)

	// Don't truncate - filtering already happened via oldestAllowed
	// Perl writes merged events directly without additional truncation
	rf.recent = newRecent

	// Update minmax
	rf.updateMinmax()

	// Copy source dirtymark (Perl does this after filtering, before write)
	// Perl: if (!$self->dirtymark || $other->dirtymark ne $self->dirtymark)
	if rf.meta.Dirtymark.IsZero() || rf.meta.Dirtymark != source.meta.Dirtymark {
		rf.meta.Dirtymark = source.meta.Dirtymark
	}

	source.mu.RUnlock()
	rf.mu.Unlock()

	// Write target file
	if err := rf.Write(); err != nil {
		return fmt.Errorf("write target: %w", err)
	}

	return nil
}

// DeduplicateEpochs ensures all events have unique epochs.
// If duplicates are found, increments them slightly.
func (rf *Recentfile) DeduplicateEpochs(events []Event) []Event {
	if len(events) <= 1 {
		return events
	}

	result := make([]Event, len(events))
	copy(result, events)

	seen := make(map[Epoch]bool)
	for i := range result {
		epoch := result[i].Epoch

		// If duplicate, increment until unique
		for seen[epoch] {
			epoch = EpochIncreaseABit(epoch)
		}

		seen[epoch] = true
		result[i].Epoch = epoch
	}

	// Re-sort after deduplication
	rf.sortEventsByEpoch(result)

	return result
}

// shouldMergeByAge checks if target file is old enough to warrant merging.
func shouldMergeByAge(target *Recentfile, prevInterval string) bool {
	targetFile := target.Rfile()
	stat, err := os.Stat(targetFile)
	if os.IsNotExist(err) {
		return true // File doesn't exist, create it
	}
	if err != nil {
		return false // Can't stat, skip
	}

	// Check if target file is older than previous interval duration
	targetAge := time.Since(stat.ModTime())
	prevDuration := time.Duration(IntervalSecsFor(prevInterval)) * time.Second

	return targetAge > prevDuration
}

// GetNextInterval returns the next larger interval from the aggregator list.
// Returns empty string if no larger interval exists.
func (rf *Recentfile) GetNextInterval() string {
	aggregator := rf.meta.Aggregator
	if len(aggregator) == 0 {
		return ""
	}

	myDuration := rf.IntervalSecs()

	// Find smallest interval larger than current
	var nextInterval string
	var nextDuration int64 = ZSeconds

	for _, interval := range aggregator {
		duration := IntervalSecsFor(interval)
		if duration > myDuration && duration < nextDuration {
			nextInterval = interval
			nextDuration = duration
		}
	}

	return nextInterval
}

// AggregateInterval aggregates a specific source interval into a specific target interval.
// This is a more direct version of Aggregate for testing or manual control.
func (rf *Recentfile) AggregateInterval(sourceInterval, targetInterval string) error {
	// Create source recentfile
	source := rf.SparseClone()
	source.SetInterval(sourceInterval)
	if err := source.Read(); err != nil {
		return fmt.Errorf("read source %s: %w", sourceInterval, err)
	}

	// Create target recentfile
	target := rf.SparseClone()
	target.SetInterval(targetInterval)

	// Merge
	if err := target.MergeFrom(source); err != nil {
		return fmt.Errorf("merge %s into %s: %w", sourceInterval, targetInterval, err)
	}

	// Update source's merged metadata
	if err := source.Lock(); err != nil {
		return fmt.Errorf("lock source: %w", err)
	}
	defer source.Unlock()

	source.mu.Lock()
	if len(target.recent) > 0 {
		source.meta.Merged = &MergedInfo{
			Epoch:        target.recent[0].Epoch,
			IntoInterval: targetInterval,
		}
	}
	source.mu.Unlock()

	if err := source.Write(); err != nil {
		return fmt.Errorf("write source metadata: %w", err)
	}

	return nil
}
