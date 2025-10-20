# Critical Bugs in Aggregation System

**Initial Report:** 2025-10-20 - 6h file bloat (59K events, 10+ days)
**Follow-up:** 2025-10-22 - Aggregation chain stops at 1d, won't progress to 1W

**Root Cause:** Eight distinct bugs in Go aggregation implementation

## Fix Status

- **Bug #1**: ✅ FIXED in commit `d5a6719` - "fix(fsck): use current time when adding files to index"
- **Bug #2**: ✅ FIXED in commit `891892e` - "fix(aggregation): fix event truncation and chaining"
- **Bug #3**: ✅ FIXED in commit `891892e` - "fix(aggregation): fix event truncation and chaining"
- **Bug #4**: ✅ FIXED in commit `7c66343` - "fix(recentfile): maintain 10µs quantization in epoch increment"
- **Bug #5**: ✅ FIXED - Wrong interval used for age checking (blocks chain progression)
- **Bug #6**: ✅ FIXED - Aggressive truncation on first merge (no merged metadata)
- **Bug #7**: ✅ FIXED - Dirtymark copied before comparison (always equal)
- **Bug #8**: ✅ FIXED - Calling truncate() after merge (double filtering)

## Problem Summary

The 6h RECENT file contains 59,333 events spanning 10.8 days (4347% over capacity) instead of the expected ~dozens of events spanning 6 hours. This causes:

1. Perl client's `done->covered()` returns false (can't process all 59K events in one pass)
2. Client continuously re-fetches file to continue processing
3. Appears as constant "Sync" operations every ~10 seconds

```
# Expected vs Actual
Expected:  6h file with ~50 events spanning 6 hours (21,600 seconds)
Actual:    6h file with 59,333 events spanning 10.8 days (938,980 seconds)
Utilization: 4347% (should be ~100%)
```

---

## Bug #1: fsck repair uses file mtime instead of "now" for event epochs

**Status:** ✅ FIXED in commit `d5a6719`

**File:** `fsck/repair.go:226`

**Current Code:**
```go
batch = append(batch, recentfile.BatchItem{
    Path:  relPath,
    Type:  "new",
    Epoch: recentfile.EpochFromTime(info.ModTime()),  // ❌ WRONG
})
```

**Perl Behavior:** Uses `Time::HiRes::time` (current time), NEVER file mtime
```perl
# File: Recentfile.pm:2103-2110
# you must calculate the time after having locked, of course
my $now = Time::HiRes::time;

my $epoch;
if (defined $dirty_epoch && _bigfloatgt($now,$dirty_epoch)) {
    $epoch = $dirty_epoch;
} else {
    $epoch = $self->_epoch_monotonically_increasing($now,$recent);
}
```

**Why This is Wrong:**

1. File mtime reflects when file was copied/mirrored, not when event occurred
2. When `rrr-server` starts with `--fsck-repair`, it scans all existing files
3. Files have mtimes spread across days/weeks/months
4. Events get epochs from the past, don't age out together
5. Truncation logic can't remove them (they appear to be within the time window)

**Impact:**

- Floods RECENT files with thousands of events with "old" epochs
- Events span 10+ days instead of hours
- Files become bloated (59K events in 6h file)
- Client can't finish processing

**Fix:**
```go
batch = append(batch, recentfile.BatchItem{
    Path:  relPath,
    Type:  "new",
    Epoch: recentfile.EpochNow(),  // ✅ Use current time
    // OR omit Epoch entirely and let BatchUpdate assign it
})
```

**Alternative:** Don't auto-add missing files during repair. Treat as warnings instead, or require explicit epoch specification.

---

## Bug #2: MergeFrom doesn't remove old events from target before merging

**Status:** ✅ FIXED in commit `891892e`

**File:** `recentfile/aggregation.go:163-165`

**Current Code:**
```go
// Add events from target (rf) - the 6h file
for _, event := range rf.recent {
    mergedEvents[event.Path] = event  // ❌ No age filtering
}

// Add/update events from source - the 1h file
for _, event := range source.recent {
    // Check if event is old enough to skip
    if !oldestAllowed.IsZero() && EpochLt(event.Epoch, oldestAllowed) {
        continue  // ✅ Skip old events from source
    }
    // ... add to mergedEvents
}
```

**Perl Behavior:** Removes old events from target BEFORE merging
```perl
# File: Recentfile.pm:914-917
while (@$my_recent && _bigfloatlt($my_recent->[-1]{epoch}, $oldest_allowed)) {
    pop @$my_recent;  # ✅ Remove old events
    $something_done = 1;
}
```

**Why This is Wrong:**

1. Target file (e.g., 6h) keeps ALL existing events, regardless of age
2. Only filters events coming FROM source (e.g., 1h)
3. Old events in target accumulate forever
4. No cleanup mechanism removes them

**Example Flow:**

```
Day 1: fsck repair adds 10,000 files with old mtimes → 1h file
Day 1: Aggregate runs → 10,000 old events copy to 6h file
Day 1: After 1 hour → 1h file truncates old events ✓
Day 1: After 1 hour → 6h file KEEPS old events forever ✗

Day 2: 100 new events added
Day 2: Aggregate runs → 100 new events copy to 6h file
Day 2: 6h file now has 10,100 events (should have ~100)

Day 10: 6h file has 59,333 events spanning 10+ days
```

**Impact:**

- Events never expire from larger interval files
- Files grow unbounded
- 6h file: 59,333 events (should be dozens)
- Client can't finish processing

**Fix:**
```go
// Remove old events from target BEFORE merging (like Perl)
for _, event := range rf.recent {
    // Check if event is too old
    if !oldestAllowed.IsZero() && EpochLt(event.Epoch, oldestAllowed) {
        continue  // ✅ Skip old events
    }
    mergedEvents[event.Path] = event
}
```

---

## Bug #3: Aggregate doesn't chain - all files merge directly from 1h

**Status:** ✅ FIXED in commit `891892e`

**File:** `recentfile/aggregation.go:63`

**Current Code:**
```go
// Aggregate into each target interval
for _, targetInterval := range targetIntervals {
    // Create sparse clone for target interval
    target := rf.SparseClone()  // Always clone from rf (the 1h file)
    target.SetInterval(targetInterval)

    // ...

    // Perform the merge
    if err := target.MergeFrom(rf); err != nil {  // ❌ Always merges from rf (1h)
        return fmt.Errorf("merge into %s: %w", targetInterval, err)
    }

    // ❌ target is discarded, not used for next iteration
}
```

**This means:**
- Iteration 1: 6h merges from 1h ✓
- Iteration 2: 1d merges from 1h ✗ (should merge from 6h)
- Iteration 3: 1W merges from 1h ✗ (should merge from 1d)
- Iteration 4: 1M merges from 1h ✗ (should merge from 1W)
- etc.

**Perl Behavior:** Creates a chain, each level merges from previous
```perl
# File: Recentfile.pm:418-444
AGGREGATOR: for my $i (0..$#aggs-1) {
    my $this = $aggs[$i]{object};  # Current level
    my $next = $this->_sparse_clone;
    $next->interval($aggs[$i+1]{interval});
    # ...
    if ($want_merge) {
        $next->merge($this);  # Merge current into next
        $aggs[$i+1]{object} = $next;  # ✅ Save for next iteration (creates chain!)
    }
}
```

**Documentation says (Recentfile.pm:397-402):**
```
  1h updates 1d on every call to aggregate()
  1d updates 1W earliest after 1h
  1W updates 1M earliest after 1d
  1M updates 1Q earliest after 1W
  1Q updates 1Y earliest after 1M
  1Y updates  Z earliest after 1Q
```

**Why This is Wrong:**

1. Events don't "flow up" the hierarchy properly
2. Each file independently pulls from 1h
3. Larger files miss events that have already been truncated from 1h
4. Inefficient - processes same events multiple times
5. Doesn't follow the documented aggregation model

**Impact:**

- Events may not propagate to larger interval files correctly
- Redundant work processing same events for each target
- Violates the design intent of hierarchical aggregation
- Combined with Bug #2, causes accumulation in all files

**Fix:**
```go
// Track the source for next iteration (create the chain)
source := rf
for _, targetInterval := range targetIntervals {
    target := source.SparseClone()  // ✅ Clone from PREVIOUS level
    target.SetInterval(targetInterval)

    // Decide if we should merge
    shouldMerge := force || source == rf
    if !shouldMerge {
        shouldMerge = shouldMergeByAge(target, source.Interval())
    }

    if !shouldMerge {
        break
    }

    // Perform the merge from previous level
    if err := target.MergeFrom(source); err != nil {
        return fmt.Errorf("merge into %s: %w", targetInterval, err)
    }

    // Update source merged metadata (Perl does this)
    source.mu.Lock()
    if len(target.recent) > 0 {
        source.meta.Merged = &MergedInfo{
            Epoch:        target.recent[0].Epoch,
            IntoInterval: targetInterval,
        }
    }
    source.mu.Unlock()

    // Write the source file (not just at the end)
    if err := source.Lock(); err != nil {
        return fmt.Errorf("lock source %s: %w", source.Interval(), err)
    }
    if err := source.Write(); err != nil {
        source.Unlock()
        return fmt.Errorf("write source %s: %w", source.Interval(), err)
    }
    source.Unlock()

    source = target  // ✅ Use target as source for next iteration (chain!)
}
```

---

## Bug #4: EpochIncreaseABit violates 10µs quantization

**Status:** ✅ FIXED in commit `7c66343`

**File:** `recentfile/epoch.go:94-100`

**Current Code:**
```go
func EpochIncreaseABit(e Epoch) Epoch {
    // Use math.Nextafter to get the next representable float64 value
    return Epoch(math.Nextafter(float64(e), math.Inf(1)))  // ❌ WRONG
}
```

**Why This is Wrong:**

The entire epoch system is designed to use **10-microsecond quantization** (5 decimal places) to prevent JSON float64 precision loss (see commit 11788dc). But `EpochIncreaseABit` uses `math.Nextafter`, which creates **full float64 precision** with 15+ significant digits.

**Example from actual data:**
```json
{ "epoch": 1760978849.0432436, ... }   // 7 decimal places
{ "epoch": 1760978849.0432434, ... }   // 7 decimal places
{ "epoch": 1760978849.0432432, ... }   // 7 decimal places
```

These should all be:
```json
{ "epoch": 1760978849.04324, ... }   // 5 decimal places
{ "epoch": 1760978849.04325, ... }   // 5 decimal places
{ "epoch": 1760978849.04326, ... }   // 5 decimal places
```

**When This Happens:**

1. Multiple events arrive at the same time (e.g., deleting a directory with many files)
2. First event gets quantized epoch: 1760978849.04320 ✓
3. `ensureMonotonic` is called for subsequent events
4. Calls `EpochIncreaseABit(1760978849.04320)`
5. Returns 1760978849.0432001 (NOT quantized to 10µs boundaries) ✗
6. Next event: 1760978849.0432002 ✗
7. All subsequent events have excessive precision

**Impact:**

1. **Violates quantization design** - Defeats the purpose of commit 11788dc
2. **JSON round-trip issues** - High-precision floats may not survive JSON serialization correctly
3. **Perl client issues** - Perl's `_bigfloatcmp` may have problems with these values
4. **Inconsistent data** - Some epochs have 5 decimals, others have 7+
5. **Client stuck in loop** - May contribute to client repeatedly processing same events

**Evidence from Client Logs:**

Client processes events at timestamp 1760977398, but RECENT file has delete events at 1760978849.043243X (24 minutes newer). Client never advances to see these delete events because it's stuck processing the bloated 6h file.

**Fix:**

```go
func EpochIncreaseABit(e Epoch) Epoch {
    // Increment by 10 microseconds (smallest quantized unit)
    return e + 0.00001  // ✅ Add 10µs, maintains quantization
}
```

**Alternative (if need to handle collisions within 10µs):**

```go
func EpochIncreaseABit(e Epoch) Epoch {
    // Increment by 10 microseconds
    newEpoch := e + 0.00001

    // Ensure it's properly quantized (shouldn't be needed, but defensive)
    tenMicroUnits := int64(float64(newEpoch) * 1e5)
    return Epoch(float64(tenMicroUnits) / 1e5)
}
```

**Perl Reference:**

Perl uses `_increase_a_bit` (Recentfile.pm:2195-2200):
```perl
sub _increase_a_bit {
    my($epoch, $smaller_epoch) = @_;
    # Perl automatically maintains precision, doesn't have float64 issues
    # Increases by smallest distinguishable amount
    return $epoch + 0.000001;  # 1 microsecond
}
```

Note: Perl adds 1µs, but Go's 10µs quantization is intentional to avoid JSON precision loss. We should increment by 10µs to maintain our quantization scheme.

---

## Combined Impact

These four bugs together create a perfect storm:

1. **Bug #1** adds thousands of events with old epochs (from file mtimes)
2. **Bug #2** prevents cleanup - old events never removed from target files
3. **Bug #3** fails to propagate events correctly up the chain
4. **Bug #4** creates high-precision epochs that violate quantization design
5. **Result:** All larger interval files (6h, 1d, 1W, etc.) become bloated with tens of thousands of old events
6. **Result:** Client gets stuck processing old events, never catches up to current time
7. **Result:** Deletes and new events never propagate because client is perpetually behind

## Verification

After fixing, use `rrr-overview` to verify:

```bash
rrr-overview /tank/CPAN/RECENT.recent
```

Expected output (healthy):
```
Ival    Cnt           Max           Min          Span    Util       Cloud
  1h     10 1760946546.82 1760946396.79        150.03    4.2%  ^^
  6h     45 1760946546.82 1760925546.82      21000.00   97.2%  ^
  1d    180 1760946546.82 1760860546.82      86000.00   99.5%   ^
```

Current output (broken):
```
Ival    Cnt           Max           Min          Span    Util       Cloud
  1h      8 1760946546.82 1760946396.79        150.03    4.2%  ^^
  6h  59333 1760946546.82 1760007566.76     938980.05 4347.1%  ^    ^  ← BLOATED
```

## Recovery Steps

1. **Stop the server**
2. **Delete bloated RECENT files** (backup first):
   ```bash
   cd /tank/CPAN
   cp RECENT-6h.json RECENT-6h.json.backup
   cp RECENT-1d.json RECENT-1d.json.backup
   cp RECENT-1W.json RECENT-1W.json.backup
   # etc. for all bloated files
   ```
3. **Apply fixes** to aggregation.go and repair.go
4. **Restart server** (watcher will rebuild index from live changes)
5. **Monitor** with rrr-overview to ensure files stay healthy

## Real-World Impact: Delete Events Not Propagating

**Symptom:** Client on intermediate mirror (139.178.67.112) shows delete events being processed but files not deleted.

**Example from client logs:**
```
Sync 1760977398 (2/30/1h) ports/oses/ubuntu.tt_data ...
Sync 1760977398 (3/30/1h) ports/oses/hpux.tt_data ...
...
Del  1760977330 (1h) /tank/mirrors/pub/perl/CPAN/ports/oses/linux.tt_data DONE
Del  1760977468 (1h) /tank/mirrors/pub/perl/CPAN/ports/oses/debian.tt_data DONE
[LOOP REPEATS - same timestamps again]
Sync 1760977398 (2/30/1h) ports/oses/ubuntu.tt_data ...
```

**Root cause:** Client is stuck processing events from timestamp 1760977398. The RECENT-1h.json file on server has delete events at timestamp 1760978849 (24 minutes newer). Because of bloated 6h file (59K events):

1. Client processes 1h file events at ~1760977398
2. Client moves to 6h file, gets bogged down processing 59K events
3. Client times out before finishing 6h
4. Client loops back, reprocesses SAME old events from 1h
5. **Client never advances to timestamp 1760978849 where delete events are**
6. Files remain on disk even though delete events exist in RECENT

**After fixes:** Client will finish 6h file quickly, advance to current time, see delete events, execute deletions.

---

## Bug #5: Wrong interval used for age checking (chain stops prematurely)

**Status:** ✅ FIXED in commit TBD

**File:** `recentfile/aggregation.go:58-62`

**Problem discovered:** 2025-10-22 - 1W file not updating despite 1d file being active

**Current Code (WRONG):**
```go
prevInterval := rf.interval  // "1h"
for _, targetInterval := range targetIntervals {
    shouldMerge := force || prevInterval == rf.interval
    if !shouldMerge {
        shouldMerge = shouldMergeByAge(target, prevInterval)  // ❌ Wrong interval!
    }
    // ...
    prevInterval = targetInterval  // ❌ Sets to TARGET interval
    source = target
}
```

**Issue:**
When merging 1d → 1W:
- `prevInterval = "1d"` (set from previous iteration's target)
- Checks: "Is 1W file older than 1d (86,400 seconds)?"
- 1W was updated 13 hours ago (46,800 seconds)
- 46,800 < 86,400 → **Merge blocked!**

**Perl Logic (CORRECT):**
```perl
for my $i (0..$#aggs-1) {
    my $this = $aggs[$i]{object};      # Current source
    my $next = ...;                     # Target
    my $prev = $aggs[$i-1]{object};    # ✅ Previous level (BEFORE source)

    if ($next_age > $prev->interval_secs) {
        $want_merge = 1;
    }
}
```

When merging 1d → 1W (i=2):
- `$this = $aggs[2]` = 1d file
- `$next` = 1W file
- `$prev = $aggs[1]` = **6h file**
- Checks: "Is 1W older than 6h (21,600 seconds)?"
- 46,800 > 21,600 → **Merge happens!**

**Why This Matters:**
The aggregation needs to check if the target is old enough relative to the PREVIOUS source (the level before current), not the current source. This ensures each level updates at appropriate intervals:

- 1h → 6h: Check 6h age vs 1h interval (3,600 sec) → Updates frequently
- 6h → 1d: Check 1d age vs 1h interval (3,600 sec) → Updates when 1h changes
- 1d → 1W: Check 1W age vs **6h interval** (21,600 sec) → Updates when 6h changes
- etc.

**Fix:**
```go
prevSourceInterval := rf.interval  // Track interval of level BEFORE current source

for _, targetInterval := range targetIntervals {
    shouldMerge := force || source.interval == rf.interval  // First iteration check
    if !shouldMerge {
        // ✅ Check against PREVIOUS source interval
        shouldMerge = shouldMergeByAge(target, prevSourceInterval)
    }
    // ...
    prevSourceInterval = source.interval  // ✅ Save CURRENT source before moving
    source = target
}
```

**Impact:**
- **Before fix:** Chain stops at 1d, bloat never migrates to 1W/1M/etc.
- **After fix:** Chain progresses correctly, bloat migrates up hierarchy and gets diluted

**Symptoms:**
```
1d  60110 events, 1269.6% utilization  ← Bloated, not draining
1W   3315 events, 143.7% utilization   ← Stale, not updating
```

---

## Bug #6: Aggressive truncation on first merge (no merged metadata)

**Status:** ✅ FIXED in commit TBD

**File:** `recentfile/aggregation.go:160-162`

**Current Code (WRONG):**
```go
if rf.meta.Merged != nil && !rf.meta.Merged.Epoch.IsZero() {
    oldestAllowed = min(intervalCutoff, mergedEpoch)
} else {
    oldestAllowed = intervalCutoff  // ❌ Too aggressive!
}
```

**Issue:**
When a RECENT file is first created (no merged metadata yet), `oldestAllowed` is set to `now - interval`, which immediately truncates events older than the interval. This is wrong for first merges.

**Example:**
- New 1W file created
- No merged metadata yet
- oldestAllowed = now - 604,800 (1 week)
- Events from 10 days ago get dropped even though they should be preserved during initial population

**Perl Logic (CORRECT):**
```perl
my $oldest_allowed = 0;  # Initialize to 0

if ($epoch) {
    if (($other->dirtymark||0) ne ($self->dirtymark||0)) {
        $oldest_allowed = 0;
    } elsif (my $merged = $self->merged) {  # ✅ Only if merged exists
        my $secs = $self->interval_secs();
        $oldest_allowed = min($epoch - $secs, $merged->{epoch}||0);
    }
    # ✅ If no merged, stays at 0
}
```

**Why This Matters:**
First merges should be permissive - they're populating a new file from scratch. Aggressive truncation prevents proper initialization and causes events to be lost during the first aggregation cycle.

**Fix:**
```go
var oldestAllowed Epoch  // Defaults to 0

if rf.meta.Dirtymark != source.meta.Dirtymark {
    oldestAllowed = 0
} else if rf.meta.Merged != nil && !rf.meta.Merged.Epoch.IsZero() {
    // ✅ Only calculate cutoff if merged metadata exists
    now := EpochNow()
    intervalSecs := rf.IntervalSecs()
    intervalCutoff := now - intervalSecs
    oldestAllowed = min(intervalCutoff, rf.meta.Merged.Epoch)
    // ... adjustment logic ...
} else {
    // ✅ No merged metadata - keep everything
    oldestAllowed = 0
}
```

**Impact:**
- **Before fix:** New RECENT files lose historical events on first population
- **After fix:** First merge preserves all events, subsequent merges truncate normally

---

## References

- Perl implementation: `/Users/ask/src/rersyncrecent/lib/File/Rsync/Mirror/Recentfile.pm`
  - `aggregate()`: lines 409-445
  - `merge()`: lines 877-942
  - `_update_batch_item()`: lines 2097-2161
  - `_increase_a_bit()`: lines 2195-2200
- Go quantization fix: commit 11788dc
- Perl docs: `perldoc File::Rsync::Mirror::Recentfile`
---

## Bug #7: Dirtymark copied before comparison (always equal)

**Status:** ✅ FIXED

**File:** `recentfile/aggregation.go:136-141` (before fix)

**Problem:**
MergeFrom copied the source's dirtymark to the target BEFORE comparing them, making them always equal and bypassing the "keep everything" logic.

```go
// WRONG - copy first, then compare
rf.meta.Dirtymark = source.meta.Dirtymark

if rf.meta.Dirtymark \!= source.meta.Dirtymark {  // Always false\!
    oldestAllowed = 0
}
```

**Perl Logic:**
Perl compares dirtymarks first, does all filtering, THEN copies at the end:

```perl
# Lines 902-918: Compare dirtymarks, calculate oldest_allowed

if (($other->dirtymark||0) ne ($self->dirtymark||0)) {
    $oldest_allowed = 0;
}

# ... filtering logic ...

# Lines 975-978: Copy AFTER filtering
if (\!$self->dirtymark || $other->dirtymark ne $self->dirtymark) {
    $self->dirtymark ($other->dirtymark);
}
$self->write_recent($recent);
```

**Fix:**
```go
// Compare dirtymarks BEFORE copying
if rf.meta.Dirtymark \!= source.meta.Dirtymark {
    oldestAllowed = 0
}

// ... merge logic ...

// Copy dirtymark AFTER filtering, before write
if rf.meta.Dirtymark.IsZero() || rf.meta.Dirtymark \!= source.meta.Dirtymark {
    rf.meta.Dirtymark = source.meta.Dirtymark
}
```

**Impact:**
- **Before fix:** Dirtymark changes never triggered "keep everything" mode
- **After fix:** Dirtymark mismatches correctly preserve all events during merge

---

## Bug #8: Calling truncate() after merge (double filtering)

**Status:** ✅ FIXED

**File:** `recentfile/aggregation.go:227` (before fix)

**Problem:**
MergeFrom called `truncate()` after the merge, applying interval-based filtering on top of the already-correct oldestAllowed filtering. This caused events to be dropped even when they should be kept.

```go
// Merge events with oldestAllowed filtering
rf.recent = rf.truncate(newRecent)  // WRONG - filters again\!
```

The truncate() function aggressively filters based on interval:
```go
func (rf *Recentfile) truncate(events []Event) []Event {
    if rf.meta.Merged \!= nil {
        cutoff = rf.meta.Merged.Epoch
    } else {
        cutoff = now - intervalSecs  // Aggressive for first merge\!
    }
    // Filters out events older than cutoff
}
```

**Perl Logic:**
Perl writes merged events directly without additional truncation:

```perl
# Line 980: Write directly, no truncation
$self->write_recent($recent);
```

**Fix:**
```go
// Don't truncate - filtering already happened via oldestAllowed
// Perl writes merged events directly without additional truncation
rf.recent = newRecent
```

**Impact:**
- **Before fix:** Events correctly preserved by dirtymark logic were then removed by truncate()
- **After fix:** Only oldestAllowed filtering applies, matching Perl behavior

**Test Coverage:**
`TestMergeFromFirstMergePreservesAllEvents` verifies that 10-day-old events are preserved when target has no merged metadata and dirtymarks differ.

---

