# RECENT File System - Design and Implementation Guide

This document explains how the RECENT file aggregation system works and must be implemented correctly.

## Overview

The RECENT file system tracks file changes in a mirrored repository by maintaining index files at different time intervals. This allows efficient incremental syncing - clients can check small files (1h) frequently and larger files (6h, 1d, etc.) less often.

## Reference Implementation

**Authoritative source:** Perl implementation at `../rersyncrecent/lib/File/Rsync/Mirror/Recentfile.pm`

This Go implementation MUST match the Perl behavior exactly for compatibility with existing clients.

---

## File Hierarchy

Files are organized in a hierarchy by time interval:

```
1h → 6h → 1d → 1W → 1M → 1Q → 1Y → Z
```

- **Principal file** (typically 1h): Smallest interval, updated by watcher on every file change
- **Aggregated files** (6h, 1d, etc.): Larger intervals, updated periodically by aggregation
- **Z file**: Infinite interval, never truncated, complete history

### Key Properties

1. **Each file is independent** - can be read/processed standalone
2. **Events flow up the chain** during aggregation (1h → 6h → 1d → ...)
3. **Larger files update less frequently** to reduce overhead
4. **Each file spans approximately its interval** (6h file spans ~6 hours)

---

## Event Structure

Each event records a file change:

```go
type Event struct {
    Epoch Epoch   // When the event occurred (NOT file mtime!)
    Path  string  // Relative path from localroot
    Type  string  // "new" or "delete"
}
```

### Critical: Event Epochs

**Event epochs MUST use the current time when the event is created, NOT file mtime.**

```go
// ✅ CORRECT
event := Event{
    Epoch: EpochNow(),  // Current time
    Path:  "authors/id/A/AB/ABC/Foo-1.0.tar.gz",
    Type:  "new",
}

// ❌ WRONG
event := Event{
    Epoch: EpochFromTime(fileInfo.ModTime()),  // File's mtime
    Path:  "authors/id/A/AB/ABC/Foo-1.0.tar.gz",
    Type:  "new",
}
```

**Why:** File mtime reflects when the file was last modified on disk, which may be:
- Days/weeks/months ago if file is old
- Different on different servers (rsync updates mtime)
- Irrelevant to when the event entered THIS mirror's index

The epoch represents **when we learned about this change**, not when the file was originally created.

**Exception:** The `--epoch` option in `rrr-update` allows backdating events for recovery scenarios, but:
1. It's marked as "costly to downstream servers"
2. It sets the dirtymark, forcing clients to re-sync
3. It's for fixing gaps, not normal operation

---

## Aggregation: The Chain

**CRITICAL:** Aggregation creates a CHAIN where each level merges from the previous level, not all from 1h.

### How It Works

When `Aggregate()` is called on the 1h file:

```
1. 1h merges into 6h
2. 6h merges into 1d    ← Uses the RESULT from step 1
3. 1d merges into 1W    ← Uses the RESULT from step 2
4. 1W merges into 1M    ← Uses the RESULT from step 3
... and so on
```

From Perl docs (Recentfile.pm:397-402):
```
  1h updates 1d on every call to aggregate()
  1d updates 1W earliest after 1h
  1W updates 1M earliest after 1d
  1M updates 1Q earliest after 1W
  1Q updates 1Y earliest after 1M
  1Y updates  Z earliest after 1Q
```

### Perl Implementation Pattern

```perl
# Perl: Recentfile.pm:418-444
AGGREGATOR: for my $i (0..$#aggs-1) {
    my $this = $aggs[$i]{object};      # Current level
    my $next = $this->_sparse_clone;
    $next->interval($aggs[$i+1]{interval});

    if ($want_merge) {
        $next->merge($this);           # Merge THIS into NEXT
        $aggs[$i+1]{object} = $next;   # ← SAVE result for next iteration
    }
}
```

The key is `$aggs[$i+1]{object} = $next` - this saves the merged result so the next iteration uses it.

### Go Implementation Pattern (CORRECT)

```go
// Start with the principal file (1h)
source := rf

for _, targetInterval := range targetIntervals {
    // Create target from PREVIOUS level (not always from 1h!)
    target := source.SparseClone()
    target.SetInterval(targetInterval)

    // Merge from previous level
    if err := target.MergeFrom(source); err != nil {
        return err
    }

    // Update source's merged metadata
    source.meta.Merged = &MergedInfo{
        Epoch:        target.recent[0].Epoch,
        IntoInterval: targetInterval,
    }

    // Write source file to persist merged metadata
    source.Write()

    // ← KEY: Use target as source for next iteration
    source = target
}
```

---

## Truncation: Removing Old Events

Each RECENT file should only contain events within its time window. Old events MUST be removed during aggregation.

### When Truncation Happens

**During `MergeFrom()`**, BEFORE merging events:

1. Calculate `oldestAllowed` cutoff epoch
2. Remove events from TARGET file older than cutoff
3. Remove events from SOURCE file older than cutoff
4. Merge the filtered sets

### Perl Implementation

```perl
# Perl: Recentfile.pm:914-917
# Remove old events from target BEFORE merging
while (@$my_recent && _bigfloatlt($my_recent->[-1]{epoch}, $oldest_allowed)) {
    pop @$my_recent;
    $something_done = 1;
}
```

### Go Implementation (CORRECT)

```go
// In MergeFrom(), before building mergedEvents map:

// 1. Calculate cutoff
var oldestAllowed Epoch
if rf.meta.Dirtymark != source.meta.Dirtymark {
    oldestAllowed = 0  // Keep everything if dirtymarks differ
} else {
    now := EpochNow()
    intervalSecs := rf.IntervalSecs()
    oldestAllowed = EpochFromFloat(EpochToFloat(now) - float64(intervalSecs))
}

// 2. Filter events from target BEFORE adding to merge
for _, event := range rf.recent {
    // ✅ MUST check age
    if !oldestAllowed.IsZero() && EpochLt(event.Epoch, oldestAllowed) {
        continue  // Skip old events
    }
    mergedEvents[event.Path] = event
}

// 3. Filter events from source
for _, event := range source.recent {
    // ✅ MUST check age
    if !oldestAllowed.IsZero() && EpochLt(event.Epoch, oldestAllowed) {
        continue  // Skip old events
    }
    // Update if newer than existing
    if existing, ok := mergedEvents[event.Path]; ok {
        if EpochGt(event.Epoch, existing.Epoch) {
            mergedEvents[event.Path] = event
        }
    } else {
        mergedEvents[event.Path] = event
    }
}
```

### Why Truncation is Critical

Without proper truncation:
- Files grow unbounded
- 6h file contains 10+ days of events (thousands/millions instead of dozens)
- Clients can't finish processing (hit max_files_per_connection limit)
- Clients repeatedly re-fetch files trying to catch up
- Disk space waste
- Poor performance

---

## Calculating oldestAllowed

The cutoff for keeping events is complex and follows Perl logic exactly:

```go
var oldestAllowed Epoch

if dirtymark differs {
    // Keep everything on dirtymark change
    oldestAllowed = 0
} else if merged != nil && merged.Epoch != 0 {
    // Target has been merged before
    now := EpochNow()
    intervalSecs := rf.IntervalSecs()

    // Perl: min($epoch - $secs, $merged->{epoch}||0)
    intervalCutoff := now - intervalSecs
    oldestAllowed = min(intervalCutoff, merged.Epoch)
} else {
    // No merge history, use interval-based cutoff
    now := EpochNow()
    intervalSecs := rf.IntervalSecs()
    oldestAllowed = now - intervalSecs
}
```

**Perl reference:** Recentfile.pm:906-918

The `merged.Epoch` represents the most recent event from the last aggregation. Using it as part of the cutoff ensures we don't prematurely discard events that haven't propagated up the chain yet.

---

## File Updates and Dirtymark

### When to Update Files

**Principal file (1h):** Updated on every `BatchUpdate()` call (file changes from watcher)

**Aggregated files (6h, 1d, etc.):** Updated during `Aggregate()` when:
1. `force = true`, OR
2. This is the first level after principal, OR
3. File age > previous level's interval duration

### Dirtymark

The dirtymark is a timestamp indicating when the index was fundamentally changed (e.g., by fsck repair or bulk updates).

When dirtymark changes:
- All clients must re-sync completely
- No truncation happens (keep all events)
- Expensive operation, avoid if possible

---

## Testing and Verification

### Use rrr-overview

```bash
rrr-overview /path/to/RECENT.recent
```

Expected output (healthy):
```
Ival    Cnt           Max           Min          Span    Util       Cloud
  1h     10 1760946546.82 1760946396.79        150.03    4.2%  ^^
  6h     45 1760946546.82 1760925546.82      21000.00   97.2%  ^
  1d    180 1760946546.82 1760860546.82      86000.00   99.5%   ^
  1W   1200 1760946546.82 1760342546.82     604000.00   99.9%    ^
```

- **Cnt**: Number of events (should be reasonable, not thousands in 1h/6h)
- **Span**: Time range (seconds) - should be ≈ interval
- **Util**: Span/Interval ratio - should be ≈ 100%

Bad signs:
```
  6h  59333 1760946546.82 1760007566.76     938980.05 4347.1%  ^    ^
```
- 59K events in 6h file (should be dozens)
- Span 10.8 days (should be 6 hours)
- 4347% utilization (massively over capacity)

### Unit Tests

Test these scenarios:

1. **Epoch assignment**: Verify events get `EpochNow()`, not file mtime
2. **Chained aggregation**: Verify 1d merges from 6h, not from 1h
3. **Truncation**: Verify old events removed from target before merge
4. **Cutoff calculation**: Verify oldestAllowed matches Perl logic
5. **Event ordering**: Verify events sorted by epoch descending

### Integration Tests

1. Add 1000 files, run aggregate, verify event counts reasonable
2. Wait 1 hour, run aggregate, verify 1h truncated but 6h retained
3. Wait 6 hours, run aggregate, verify 6h truncated but 1d retained

---

## Common Mistakes

### ❌ WRONG: Using file mtime for epochs

```go
// DON'T DO THIS
epoch := EpochFromTime(fileInfo.ModTime())
```

Old files will have old epochs, won't age out, files bloat.

### ❌ WRONG: All files merge from 1h

```go
// DON'T DO THIS
for _, interval := range intervals {
    target := rf.SparseClone()  // Always from rf (1h)
    target.MergeFrom(rf)        // Always from rf (1h)
}
```

This breaks the chain. Must use previous iteration's result.

### ❌ WRONG: Not removing old events from target

```go
// DON'T DO THIS
for _, event := range targetFile.recent {
    mergedEvents[event.Path] = event  // No age check!
}
```

Old events accumulate forever. Must filter by oldestAllowed.

### ❌ WRONG: Writing only at the end

```go
// DON'T DO THIS
for _, interval := range intervals {
    target.MergeFrom(source)
    source = target
}
// Only write at the very end
source.Write()
```

The Perl implementation writes each level after merging to persist the `merged` metadata. This metadata is used in the next aggregation cycle's `oldestAllowed` calculation.

---

## Performance Considerations

1. **File locking**: Use file locks to prevent concurrent writes
2. **Atomic writes**: Write to `.new` file, then rename (atomic operation)
3. **Memory efficiency**: Stream large files instead of loading entirely
4. **Aggregation frequency**: Run every 5 minutes (configurable)
5. **Batch updates**: Group file events to reduce I/O

---

## Compatibility Requirements

This Go implementation MUST remain compatible with:

1. **Perl rrr-client** - clients reading these files
2. **Perl rrr-server** - may run alongside/upstream
3. **Existing tools** - rrr-overview, rrr-fsck, etc.

To ensure compatibility:
- Match Perl behavior exactly
- Use same file format (YAML/JSON)
- Follow same epoch semantics
- Implement same aggregation chain
- Use same truncation logic

---

## References

- **Perl source**: `../rersyncrecent/lib/File/Rsync/Mirror/Recentfile.pm`
  - `aggregate()`: lines 409-445
  - `merge()`: lines 877-942
  - `_update_batch_item()`: lines 2097-2161
  - `truncate`: lines 2087-2093

- **Documentation**: `perldoc File::Rsync::Mirror::Recentfile`

- **Bug report**: `../AGGREGATION_BUGS.md` - Details of bugs found 2025-10-20

When in doubt, READ THE PERL CODE. It is the authoritative implementation.
