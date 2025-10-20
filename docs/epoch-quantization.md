# Epoch Quantization and Floating-Point Precision

## Overview

The RECENT file system uses Unix timestamps with fractional seconds (Epoch type, a float64) to represent when file change events occurred. To prevent precision loss during JSON serialization/deserialization, all epochs are **quantized to 10-microsecond intervals** (5 decimal places).

## The Problem

### JSON float64 Precision Loss

JSON numbers are represented as IEEE 754 double-precision float64 (64 bits):
- 1 sign bit
- 11 exponent bits
- 52 mantissa bits → ~15-16 decimal digits of precision

Unix timestamps in nanoseconds (like `1760978849043243678`) have 19 digits, which **exceeds float64 precision**. When serialized to JSON and back, precision is lost.

### Real-World Impact

Before quantization fix (commit 11788dc), the system crashed with this error from Perl rrr-fsck:

```
disorder '1760923093.82076'>='1760923093.82076'
```

Multiple distinct nanosecond-precise epochs rounded to the **same** JSON number, violating the strict descending order requirement.

## The Solution: 10-Microsecond Quantization

### Why 10 Microseconds?

- **1 second** = 1,000,000 microseconds = 100,000 ten-microsecond units
- Unix timestamp in seconds: ~10 digits (e.g., 1760978849)
- Fractional part (10µs): 5 decimal places (e.g., 0.04324)
- **Total: 15 digits** - safely within float64 precision

10-microsecond precision:
- Sufficient for file mirroring (files don't change faster than 10µs)
- Safe from JSON precision loss until well past year 2100
- Perl implementation uses 1µs, but that's too fine for JSON safety

### Implementation

```go
// Epoch represents a timestamp as a float64
// Format: Unix timestamp with fractional seconds (e.g., 1760007882.98731)
// Precision: 10 microseconds (5 decimal places in seconds)
type Epoch float64

// EpochNow returns the current time quantized to 10µs
func EpochNow() Epoch {
    now := time.Now()
    // Quantize to 10-microsecond intervals
    tenMicroUnits := now.UnixMicro() / 10  // Integer division truncates
    return Epoch(float64(tenMicroUnits) / 1e5)
}

// EpochFromTime converts time.Time to quantized Epoch
func EpochFromTime(t time.Time) Epoch {
    tenMicroUnits := t.UnixMicro() / 10
    return Epoch(float64(tenMicroUnits) / 1e5)
}
```

### Key Operations

**Increment by 10µs** (for deduplication):
```go
func EpochIncreaseABit(e Epoch) Epoch {
    return e + 0.00001  // Add 10 microseconds
}
```

❌ **WRONG** - Do NOT use `math.Nextafter`:
```go
// This creates excessive precision (15+ decimal places)
// Violates quantization scheme!
return Epoch(math.Nextafter(float64(e), math.Inf(1)))
```

## Floating-Point Arithmetic Gotchas

### Division Introduces Rounding Error

```go
tenMicroUnits := int64(176098037719393)

// This looks like it should give an exact result:
result := float64(tenMicroUnits) / 1e5
// result = 1760980377.19393

// But when you multiply back:
check := result * 1e5
// check ≈ 176098037719392.99... or 176098037719393.01...
// NOT exactly 176098037719393.00!
```

This is **normal float64 behavior** - division and multiplication are not perfect inverses due to binary floating-point representation.

### Testing Quantization

❌ **WRONG** - Checking if `(epoch * 1e5)` is an exact integer:
```go
quantized := epoch * 1e5
remainder := quantized - float64(int64(quantized))
if remainder != 0 {  // Will often fail due to float rounding!
    // ...
}
```

✅ **CORRECT** - Test JSON round-trip instead:
```go
func TestEpochJSONRoundtrip(t *testing.T) {
    epoch := EpochNow()

    // Serialize to JSON
    data, err := json.Marshal(epoch)
    require.NoError(t, err)

    // Deserialize from JSON
    var decoded Epoch
    err = json.Unmarshal(data, &decoded)
    require.NoError(t, err)

    // Should be exactly equal after round-trip
    if epoch != decoded {
        t.Errorf("Epoch lost precision: %v != %v", epoch, decoded)
    }
}
```

✅ **ACCEPTABLE** - Check remainder with tolerance for float error:
```go
quantized := epoch * 1e5
remainder := quantized - float64(int64(quantized))

// Allow for floating-point rounding error
// Use absolute value to catch both positive and negative errors
if math.Abs(remainder) > 1e-6 {  // 1µs tolerance
    t.Errorf("Not quantized: remainder = %v", remainder)
}
```

### Extended Precision Registers

On some architectures (x87 FPU), intermediate float calculations use 80-bit registers internally, then round to 64-bit when stored to memory. This can cause confusing behavior:

```go
a := someCalculation()  // Might be 80-bit in register
b := anotherCalculation()  // Might be 80-bit in register

// Comparison in registers (80-bit) might differ from
// comparison after storing to memory (64-bit)
if a == b {  // True in 80-bit
    diff := a - b
    fmt.Println(diff)  // Might print small non-zero value!
}
```

Modern Go uses SSE2 on amd64, which avoids this issue, but be aware when testing on different architectures or compiler flags.

## Verification

### Use rrr-overview

```bash
rrr-overview /path/to/RECENT.recent
```

Expected output shows epochs with 5 decimal places:
```
  1h      8 1760946546.82314 1760946396.79123        150.03    4.2%  ^^
```

❌ **Bad** - Excessive precision (7+ decimals):
```
  1h      8 1760946546.8231436 1760946396.7912387        150.03    4.2%  ^^
```

### Check JSON Files

```bash
jq '.recent[0:3]' RECENT-1h.json
```

Expected - 5 decimal places:
```json
[
  {"epoch": 1760978849.04324, "path": "authors/...", "type": "new"},
  {"epoch": 1760978849.04323, "path": "modules/...", "type": "new"}
]
```

❌ **Bad** - 7+ decimal places:
```json
[
  {"epoch": 1760978849.0432436, "path": "authors/...", "type": "new"}
]
```

## Common Mistakes

### ❌ Using file mtime for epochs

```go
// WRONG - File mtime is when file was modified on disk,
// not when event entered the index
event := Event{
    Epoch: EpochFromTime(fileInfo.ModTime()),
    Path:  "authors/id/A/AB/ABC/Foo-1.0.tar.gz",
    Type:  "new",
}
```

Old files will have old epochs, won't age out, files bloat.

✅ **Correct** - Use current time:
```go
event := Event{
    Epoch: EpochNow(),  // When we learned about this change
    Path:  "authors/id/A/AB/ABC/Foo-1.0.tar.gz",
    Type:  "new",
}
```

### ❌ Using math.Nextafter for incrementing

```go
// WRONG - Creates 15+ decimal places, violates quantization
func EpochIncreaseABit(e Epoch) Epoch {
    return Epoch(math.Nextafter(float64(e), math.Inf(1)))
}
```

Results in epochs like `1760978849.0432436198...` which may not survive JSON round-trip.

✅ **Correct** - Increment by 10µs:
```go
func EpochIncreaseABit(e Epoch) Epoch {
    return e + 0.00001  // Add 10 microseconds
}
```

### ❌ Expecting exact integer after multiplication

```go
// This test is too strict - float arithmetic introduces rounding error
quantized := epoch * 1e5
if quantized != float64(int64(quantized)) {
    t.Error("not an integer")  // May fail due to float rounding
}
```

✅ **Correct** - Test JSON round-trip or allow tolerance:
```go
// Test what actually matters - JSON serialization
data, _ := json.Marshal(epoch)
var decoded Epoch
json.Unmarshal(data, &decoded)
if epoch != decoded {
    t.Error("precision lost in JSON")
}
```

## Historical Context

- **Original bug**: Perl rrr-fsck crashed with "disorder" error
- **Root cause**: Nanosecond-precise epochs lost precision in JSON
- **Fix commit**: 11788dc (2025-10-19)
- **Solution**: Quantize to 10µs intervals

## References

- IEEE 754 double-precision: https://en.wikipedia.org/wiki/Double-precision_floating-point_format
- Go time.Time: https://pkg.go.dev/time#Time
- JSON number precision: https://datatracker.ietf.org/doc/html/rfc8259#section-6
- Original Perl implementation: `../rersyncrecent/lib/File/Rsync/Mirror/Recentfile.pm`

## Summary

- **Always** use EpochNow() or EpochFromTime() to create epochs
- **Never** use file mtime for event epochs
- **Never** use math.Nextafter for incrementing epochs
- **Test** JSON round-tripping, not mathematical properties
- **Expect** 5 decimal places in JSON output
- **Remember** float64 arithmetic is not exact - allow tolerance in tests
