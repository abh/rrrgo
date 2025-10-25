# RRR Protocol Specification

**Version:** 1.0 (Draft)
**Date:** 2025-10-24
**Status:** Draft

## 1. Overview

The RRR (Rsync Recent Recentfile) protocol is a distributed file synchronization system that uses hierarchical time-based aggregation to efficiently track and mirror file changes. Unlike traditional mirroring approaches that scan entire directory trees, RRR maintains a series of "RECENT files" that record file events (creations and deletions) in reverse-chronological order.

The protocol enables:
- **Incremental synchronization**: Clients fetch only changed files since their last sync
- **Hierarchical aggregation**: Events flow through a chain of files with increasing time spans (e.g., 1h → 6h → 1d → 1W)
- **Efficient distribution**: Small, frequently-updated files for recent changes; larger, rarely-updated files for historical data
- **Decentralized mirroring**: Any mirror can act as a source for downstream mirrors

### Design Rationale

Hierarchical aggregation exists to balance update frequency with file size. The shortest interval file (e.g., 1h) is updated frequently but stays small. Larger interval files (e.g., 1W, 1M) are updated rarely and contain more history. This design minimizes both network traffic and processing overhead for mirrors.

## 2. Terminology

This specification uses terminology from the reference Perl implementation (`File::Rsync::Mirror::Recentfile`). Terms are defined precisely to avoid ambiguity.

### Core Concepts

- **RECENT file**: A file containing metadata and a time-ordered list of file events. Also called a "recentfile."
- **Event**: A record of a file system change, containing an epoch timestamp, file path, and event type
- **Epoch**: A floating-point timestamp representing seconds since the Unix epoch (1970-01-01 00:00:00 UTC), with microsecond precision
- **Interval**: A time span specification (e.g., "1h", "6h", "1d") that defines the maximum age of events in a RECENT file
- **Interval spec**: A string composed of an integer and a letter (s/m/h/d/W/M/Q/Y) or the special value "Z" (infinite)

### Metadata Fields

- **dirtymark**: A timestamp indicating when an out-of-band change occurred on the origin server that requires full re-synchronization. Dirtymark changes invalidate client state.
- **merged**: Metadata recording when this RECENT file was last merged into the next larger interval file, containing the epoch of the newest event merged and the target interval
- **aggregator**: An ordered list of interval specs indicating which larger interval files should be maintained
- **filenameroot**: The filename prefix for RECENT files (typically "RECENT")
- **protocol**: Protocol version number (this spec describes protocol 1)

### Operations

- **Update**: Adding new events to a RECENT file
- **Truncate**: Removing events older than the interval's time span
- **Merge**: Copying events from a smaller interval file into a larger one
- **Aggregate**: Running the full merge chain from smallest to largest interval
- **Chain progression**: The process of merging 1h→6h→1d→1W→... where each level merges from the previous

### Event Timing

- **Quantization**: Rounding epoch values to 10-microsecond (0.00001 second) boundaries to ensure safe JSON serialization
- **Monotonicity**: The requirement that events within a RECENT file have strictly decreasing epoch values
- **oldestAllowed**: The calculated epoch boundary below which events are filtered during merge operations

## 3. File Format & Invariants

### 3.1 File Naming Convention

RECENT files MUST be named using the pattern:

```
{filenameroot}-{interval}{suffix}
```

Where:
- `{filenameroot}` is typically "RECENT" (configurable)
- `{interval}` is the interval spec (e.g., "1h", "6h", "1d")
- `{suffix}` indicates serialization format (e.g., ".yaml", ".json")

Examples:
- `RECENT-1h.yaml`
- `RECENT-6h.json`
- `RECENT-Z.yaml` (the infinite/archive file)

### 3.2 JSON/YAML Structure

A RECENT file contains two top-level keys: `meta` (metadata) and `recent` (event array).

**Required structure:**

```yaml
meta:
  protocol: 1
  filenameroot: "RECENT"
  interval: "6h"
  serializer_suffix: ".yaml"
  # ... additional metadata fields

recent:
  - epoch: 1223270911.76639
    path: "id/M/MS/MSIMERSON/Mail-Toaster-5.12_01.tar.gz"
    type: "new"
  - epoch: 1223270911.76193
    path: "id/M/MS/MSIMERSON/CHECKSUMS"
    type: "new"
  # ... more events in descending epoch order
```

### 3.3 Metadata Fields

#### Required Fields

- `protocol` (integer): Protocol version number. MUST be 1 for this specification.
- `filenameroot` (string): Filename prefix for RECENT files
- `interval` (string): Interval spec for this file (see section 3.5)
- `serializer_suffix` (string): File format suffix (".yaml" or ".json" recommended)

#### Optional Fields

- `aggregator` (array of strings): Ordered list of larger intervals to aggregate into
  ```yaml
  aggregator:
    - "6h"
    - "1d"
    - "1W"
    - "1M"
    - "1Q"
    - "1Y"
    - "Z"
  ```

- `dirtymark` (epoch): Timestamp of last out-of-band change requiring full resync
- `merged` (object): Information about last merge into a larger interval
  ```yaml
  merged:
    epoch: 1223270911.76639    # Epoch of newest event merged
    into_interval: "1d"        # Target interval that was merged into
    time: 1223270920.12345     # Wall-clock time when merge occurred
  ```

- `canonize` (string): Path normalization method. Default/recommended: "naive_path_normalize"
- `comment` (string): Human-readable description
- `Producers` (object): Software version information (may use uppercase key)
- `minmax` (object): Tracking metadata for the event time span in this file
  ```yaml
  minmax:
    max: 1761290401.20441     # Epoch of newest event (recent[0].epoch)
    min: 1761275283.37334     # Epoch of oldest event (recent[-1].epoch)
    mtime: 1761290341         # File modification time (Unix timestamp)
  ```

  **Purpose**: The `minmax` object tracks the actual span of events currently in the file. This enables:
  - Observability: Verify that files maintain appropriate coverage (e.g., RECENT-6h should have ~6h span)
  - Client optimization: Determine if the file contains relevant events without parsing the entire array
  - Debugging: Identify premature truncation issues (see GitHub issue #3)

  **Calculation**:
  - `max` is set to `recent[0].epoch` (newest event)
  - `min` is set to `recent[N-1].epoch` (oldest event)
  - `mtime` is the file's modification timestamp
  - If `recent` is empty, `max` and `min` are undefined/null

#### Field Constraints

- `dirtymark` MAY increase or decrease (implementation note: compare for inequality, not direction)
- `merged.epoch` MUST be a valid epoch value from the `recent` array at the time of merge
- `interval` MUST match the interval in the filename

### 3.4 Event Fields

Each element in the `recent` array MUST contain:

- `epoch` (float): Event timestamp with 10µs quantization (5 decimal places maximum for JSON safety)
- `path` (string): Relative path from repository root, using forward slashes
- `type` (string): Event type, either "new" or "delete"

**Example events:**

```yaml
recent:
  - epoch: 1760978849.04324  # 5 decimal places (10µs quantization)
    path: "authors/id/A/AB/ABIGAIL/Acme-1.23.tar.gz"
    type: "new"

  - epoch: 1760978848.12000
    path: "authors/id/A/AB/ABIGAIL/Acme-1.22.tar.gz"
    type: "delete"
```

#### Event Ordering Invariant

Events in the `recent` array MUST be ordered by strictly decreasing `epoch` values:

```
recent[0].epoch > recent[1].epoch > recent[2].epoch > ... > recent[N-1].epoch
```

Implementations MUST enforce this invariant when writing RECENT files.

#### Multi-File Coverage Invariant

**Critical requirement**: Events MUST appear in multiple RECENT files simultaneously during their lifetime.

When a new event is added to the shortest interval file (e.g., RECENT-1h.json), it should remain visible in that file until it ages beyond the interval. As the aggregation process runs, the event gets copied "upward" into larger interval files (6h, 1d, 1W, etc.) while still remaining in the smaller files.

**Example timeline** for a file added at epoch 1761275449 (assuming aggregation runs every few minutes):

| Time | RECENT-1h | RECENT-6h | RECENT-1d | RECENT-1W |
|------|-----------|-----------|-----------|-----------|
| T+0  | ✓ present | - | - | - |
| T+5min | ✓ present | ✓ present | - | - |
| T+1h | - removed | ✓ present | ✓ present | - |
| T+6h | - | - removed | ✓ present | ✓ present |
| T+1d | - | - | - removed | ✓ present |

**Violation example** (from GitHub issue #3):

A file only 4 hours old appearing ONLY in RECENT-1d.json but not in RECENT-6h.json indicates premature removal from the 6h file. This violates the coverage invariant.

**Observability**: The `minmax` metadata provides a way to verify correct behavior:
- A RECENT-6h file should have a `minmax` span close to 21,600 seconds (6 hours)
- A RECENT-1d file should have a `minmax` span close to 86,400 seconds (1 day)
- If these spans are significantly shorter, events are being prematurely truncated

**Reference**: See `Recentfile.pm` function `merge()` for the reference implementation's event retention logic.

### 3.5 Interval Specification

An interval spec is a compact string representation of a time span.

#### Format

- **Pattern**: `{count}{unit}` where count is an integer and unit is a single letter
- **Special case**: The single letter "Z" represents infinite time (maximum integer seconds)

#### Time Units

| Unit | Seconds | Description |
|------|---------|-------------|
| `s` | 1 | Second |
| `m` | 60 | Minute |
| `h` | 3,600 | Hour |
| `d` | 86,400 | Day |
| `W` | 604,800 | Week (7 days) |
| `M` | 2,592,000 | Month (30 days) |
| `Q` | 7,776,000 | Quarter (90 days) |
| `Y` | 31,557,600 | Year (365.25 days) |

#### Examples

- `1h` = 3,600 seconds (1 hour)
- `6h` = 21,600 seconds (6 hours)
- `1d` = 86,400 seconds (1 day)
- `1W` = 604,800 seconds (1 week)
- `Z` = infinite (used for archive files)

#### Configuration Note

The specific intervals used in a deployment (e.g., 1h, 6h, 1d, 1W, 1M, 1Q, 1Y, Z) are configurable by the server operator. These are common examples, not mandatory values.

## 4. Epoch Semantics

Epoch values are central to the protocol. This section specifies their representation, precision, and comparison rules.

### 4.1 Precision & Quantization

**Requirement**: Epoch values MUST be quantized to 10-microsecond (0.00001 second) boundaries.

**Rationale**: JSON uses IEEE 754 float64 for numeric values, which cannot reliably represent arbitrary-precision decimals. Quantizing to 10µs ensures that epoch values survive JSON serialization/deserialization without precision loss.

**Implementation**:

When creating a new epoch value from the current time or incrementing an existing epoch:

```
quantized_epoch = floor(raw_epoch * 100000) / 100000
```

This results in values with at most 5 decimal places:
- ✓ Valid: `1760978849.04324`
- ✓ Valid: `1760978849.00000`
- ✗ Invalid: `1760978849.043243` (7 decimal places)
- ✗ Invalid: `1760978849.0432001` (7 decimal places)

**Test vector**:

```yaml
# Input: current time = 1760978849.04324372 (system clock)
# Output: quantized = 1760978849.04324
```

### 4.2 Monotonicity Requirements

Within a single RECENT file, epoch values MUST be strictly decreasing.

**Event insertion**: When adding a new event:

1. If the file is empty, use the quantized current time
2. If `new_epoch > most_recent_epoch`, use `new_epoch`
3. If `new_epoch <= most_recent_epoch`, use `most_recent_epoch + 0.00001` (increment by 10µs)

**Function signature** (reference: `_epoch_monotonically_increasing` in `Recentfile.pm`):

```
epoch_monotonically_increasing(new_epoch, recent_events) -> epoch:
  if recent_events is empty:
    return quantize(new_epoch)
  if new_epoch > recent_events[0].epoch:
    return quantize(new_epoch)
  else:
    return recent_events[0].epoch + 0.00001
```

**Critical requirement**: Implementations MUST NOT use `math.nextafter()` or similar functions that generate arbitrary floating-point precision. Always increment by exactly 0.00001 to maintain quantization.

### 4.3 Comparison Operations

Due to floating-point representation, epoch comparisons require care.

**String-based comparison** (reference implementation approach):

Convert epochs to strings before comparison to avoid floating-point rounding errors:

```
epoch_lt(a, b):   # a < b
  return string(a) < string(b)

epoch_gt(a, b):   # a > b
  return string(a) > string(b)

epoch_eq(a, b):   # a == b
  return string(a) == string(b)
```

**Numeric comparison** (if necessary):

Use an epsilon value of 0.000001 (1 microsecond, smaller than the 10µs quantization):

```
epoch_lt(a, b):
  return b - a > 0.000001

epoch_gt(a, b):
  return a - b > 0.000001

epoch_eq(a, b):
  return abs(a - b) < 0.000001
```

**Critical**: Never use direct float equality (`a == b`) due to representation errors.

## 5. Event Lifecycle

(To be written: event creation, aging, expiration)

## 6. Single-File Operations

(To be written: update, truncation, locking)

## 7. Multi-File Operations

(To be written: aggregation, merge, chain progression)

## 8. Client Consumption

(To be written: reading events, tracking progress, multi-file processing)

## 9. Conformance

(To be written: requirements checklist, test vectors, common pitfalls)

## 10. References

- Reference implementation: `File::Rsync::Mirror::Recentfile` (Perl)
  - Primary source: `lib/File/Rsync/Mirror/Recentfile.pm`
  - Functions of note: `aggregate()`, `merge()`, `_update_batch_item()`, `_epoch_monotonically_increasing()`
- Bug analysis: `AGGREGATION_BUGS.md` (Go implementation issues and fixes)
- GitHub Issues:
  - [Issue #3](https://github.com/abh/rrrgo/issues/3): Premature event truncation and multi-file coverage requirements

---

*This is a draft specification. Sections 5-9 are placeholders and will be completed in subsequent iterations.*
