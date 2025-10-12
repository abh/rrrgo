package recentfile

import (
	"sync"
)

// Done tracks which timestamp intervals have been successfully processed.
// This prevents re-processing of already-synced files in client mode.
// For server mode, this is less critical but still used during aggregation.
type Done struct {
	// intervals is an array of [high, low] epoch pairs, sorted descending by high
	intervals [][2]Epoch

	// rfInterval is the interval of the owning recentfile
	rfInterval string

	// logfile for optional debug logging
	logfile string

	mu sync.RWMutex
}

// Covered checks if one or two epochs are within covered intervals.
// Single epoch: returns true if epoch is in any interval.
// Two epochs: returns true if both epochs are in the same interval.
func (d *Done) Covered(epoch1 Epoch, epoch2 ...Epoch) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.intervals) == 0 {
		return false
	}

	// Check if intervals are sorted
	isSorted := d.isSorted()

	if len(epoch2) == 0 {
		// Single epoch check
		for _, iv := range d.intervals {
			hi, lo := iv[0], iv[1]
			if EpochLe(epoch1, hi) && EpochGe(epoch1, lo) {
				return true
			}
			if isSorted && EpochGt(epoch1, hi) {
				// No chance anymore
				return false
			}
		}
		return false
	}

	// Two epochs check - both must be in same interval
	e1, e2 := epoch1, epoch2[0]
	// Ensure e1 >= e2
	if EpochLt(e1, e2) {
		e1, e2 = e2, e1
	}

	for _, iv := range d.intervals {
		hi, lo := iv[0], iv[1]
		goodCount := 0

		for _, e := range []Epoch{e1, e2} {
			if e == hi || e == lo || (EpochLt(e, hi) && EpochGt(e, lo)) {
				goodCount++
			}
		}

		if goodCount >= 2 {
			return true
		}
	}

	return false
}

// Register marks epochs as processed.
// events: the full list of events
// indices: which event indices to register (nil means all)
func (d *Done) Register(events []Event, indices []int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if indices == nil {
		// Register all events
		indices = make([]int, len(events))
		for i := range indices {
			indices[i] = i
		}
	}

	for _, i := range indices {
		d.registerOne(events, i)
	}
}

// registerOne registers a single event index.
func (d *Done) registerOne(events []Event, i int) {
	if i < 0 || i >= len(events) {
		return
	}

	epoch := events[i].Epoch

	// If already covered, skip
	if d.coveredUnlocked(epoch) {
		return
	}

	if len(d.intervals) == 0 {
		// First interval
		d.intervals = append(d.intervals, [2]Epoch{epoch, epoch})
		return
	}

	registered := 0

	// Check if we can extend existing intervals
	for idx := range d.intervals {
		iv := &d.intervals[idx]
		ivHi, ivLo := iv[0], iv[1]

		// Check left neighbor (i-1)
		if i > 0 {
			leftEpoch := events[i-1].Epoch
			if EpochGe(leftEpoch, ivLo) && EpochLe(leftEpoch, ivHi) && EpochGe(ivLo, epoch) {
				// Left neighbor is in this interval, extend downward
				iv[1] = epoch
				registered++
			}
		}

		// Check right neighbor (i+1)
		if i < len(events)-1 {
			rightEpoch := events[i+1].Epoch
			if EpochLe(rightEpoch, ivHi) && EpochGe(rightEpoch, ivLo) && EpochLe(ivHi, epoch) {
				// Right neighbor is in this interval, extend upward
				iv[0] = epoch
				registered++
			}
		}

		if registered >= 2 {
			break
		}
	}

	if registered == 2 {
		// We extended two intervals, they might now overlap - consolidate
		d.consolidate(epoch)
	} else if registered == 1 {
		// We extended one interval, consolidate any overlaps
		d.consolidate(0)
	} else {
		// Didn't extend any interval, create new one
		d.insertInterval(epoch)
	}
}

// coveredUnlocked checks coverage without locking (must be called with lock held).
func (d *Done) coveredUnlocked(epoch Epoch) bool {
	for _, iv := range d.intervals {
		if EpochLe(epoch, iv[0]) && EpochGe(epoch, iv[1]) {
			return true
		}
	}
	return false
}

// insertInterval inserts a new [epoch, epoch] interval in the correct position.
func (d *Done) insertInterval(epoch Epoch) {
	// Find insertion position (intervals sorted descending by high)
	insertPos := len(d.intervals)
	for i, iv := range d.intervals {
		if EpochGt(epoch, iv[0]) {
			insertPos = i
			break
		}
	}

	// Insert at position
	newInterval := [2]Epoch{epoch, epoch}
	d.intervals = append(d.intervals[:insertPos], append([][2]Epoch{newInterval}, d.intervals[insertPos:]...)...)
}

// consolidate merges overlapping intervals.
func (d *Done) consolidate(targetEpoch Epoch) {
	if len(d.intervals) <= 1 {
		return
	}

	// Repeatedly merge overlapping intervals until no more merges possible
	for {
		merged := false

		for i := 0; i < len(d.intervals)-1; i++ {
			curr := d.intervals[i]
			next := d.intervals[i+1]

			// Check if curr.lo <= next.hi (intervals overlap or touch)
			if EpochLe(curr[1], next[0]) {
				// Merge: extend next to include curr
				d.intervals[i+1][0] = EpochMax(curr[0], next[0])
				d.intervals[i+1][1] = EpochMin(curr[1], next[1])

				// Remove curr
				d.intervals = append(d.intervals[:i], d.intervals[i+1:]...)
				merged = true
				break
			}
		}

		if !merged {
			break
		}
	}
}

// Merge combines intervals from another Done object.
func (d *Done) Merge(other *Done) {
	if other == nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	other.mu.RLock()
	otherIntervals := make([][2]Epoch, len(other.intervals))
	copy(otherIntervals, other.intervals)
	other.mu.RUnlock()

	// Merge each interval from other
	for _, oiv := range otherIntervals {
		d.mergeOneInterval(oiv)
	}
}

// mergeOneInterval merges a single interval into this Done.
func (d *Done) mergeOneInterval(oiv [2]Epoch) {
	if len(d.intervals) == 0 {
		d.intervals = append(d.intervals, oiv)
		return
	}

	// Find if this interval overlaps or can extend any existing interval
	merged := false

	for i := range d.intervals {
		iv := &d.intervals[i]

		// Check if intervals overlap or touch
		// oiv overlaps iv if:
		//   oiv.hi >= iv.lo AND oiv.lo <= iv.hi
		if EpochGe(oiv[0], iv[1]) && EpochLe(oiv[1], iv[0]) {
			// Merge: extend iv to cover both
			iv[0] = EpochMax(oiv[0], iv[0])
			iv[1] = EpochMin(oiv[1], iv[1])
			merged = true
			break
		}
	}

	if !merged {
		// Insert as new interval
		insertPos := len(d.intervals)
		for i, iv := range d.intervals {
			if EpochGt(oiv[0], iv[0]) {
				insertPos = i
				break
			} else if EpochLt(oiv[0], iv[1]) {
				// Falls within another interval, check if we need to insert after
				insertPos = i + 1
				break
			}
		}

		d.intervals = append(d.intervals[:insertPos], append([][2]Epoch{oiv}, d.intervals[insertPos:]...)...)
	}

	// Consolidate after merge
	d.consolidate(0)
}

// Reset clears all intervals (called when dirtymark changes).
func (d *Done) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.intervals = nil
}

// isSorted checks if intervals are sorted (must be called with read lock held).
func (d *Done) isSorted() bool {
	for i := 1; i < len(d.intervals); i++ {
		if EpochGe(d.intervals[i][0], d.intervals[i-1][0]) {
			return false
		}
	}
	return true
}

// Intervals returns a copy of the intervals (for testing/debugging).
func (d *Done) Intervals() [][2]Epoch {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([][2]Epoch, len(d.intervals))
	copy(result, d.intervals)
	return result
}
