package recentfile

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Epoch represents a timestamp as a float64.
// This matches the Perl implementation and is memory efficient.
// Precision: 10 microseconds (5 decimal places in seconds)
// This ensures no precision loss when serialized to JSON as float64.
// Format: Unix timestamp with fractional seconds (e.g., 1760007882.98731)
type Epoch float64

// EpochNow returns the current time as an Epoch with 10-microsecond precision.
// The 10-microsecond granularity guarantees no two distinct events will have
// identical epoch values after JSON float64 serialization/deserialization.
// This prevents the "disorder" error in the Perl recentfile implementation.
func EpochNow() Epoch {
	now := time.Now()
	// Quantize to 10-microsecond intervals: divide microseconds by 10, then convert to seconds
	tenMicroUnits := now.UnixMicro() / 10
	return Epoch(float64(tenMicroUnits) / 1e5)
}

// EpochFromTime converts a time.Time to an Epoch with 10-microsecond precision.
func EpochFromTime(t time.Time) Epoch {
	// Quantize to 10-microsecond intervals
	tenMicroUnits := t.UnixMicro() / 10
	return Epoch(float64(tenMicroUnits) / 1e5)
}

// EpochFromFloat converts a float64 to an Epoch.
func EpochFromFloat(f float64) Epoch {
	return Epoch(f)
}

// EpochToFloat converts an Epoch to float64.
func EpochToFloat(e Epoch) float64 {
	return float64(e)
}

// EpochCompare compares two epochs.
// Returns -1 if l < r, 0 if l == r, 1 if l > r.
func EpochCompare(l, r Epoch) int {
	if l < r {
		return -1
	}
	if l > r {
		return 1
	}
	return 0
}

// EpochLt returns true if l < r.
func EpochLt(l, r Epoch) bool {
	return l < r
}

// EpochLe returns true if l <= r.
func EpochLe(l, r Epoch) bool {
	return l <= r
}

// EpochGt returns true if l > r.
func EpochGt(l, r Epoch) bool {
	return l > r
}

// EpochGe returns true if l >= r.
func EpochGe(l, r Epoch) bool {
	return l >= r
}

// EpochMax returns the larger of two epochs.
func EpochMax(l, r Epoch) Epoch {
	if l >= r {
		return l
	}
	return r
}

// EpochMin returns the smaller of two epochs.
func EpochMin(l, r Epoch) Epoch {
	if l <= r {
		return l
	}
	return r
}

// EpochIncreaseABit returns an epoch slightly larger than e.
// This is used to ensure monotonically increasing epochs when
// timestamps collide.
// Increments by 10 microseconds to maintain quantization (5 decimal places).
func EpochIncreaseABit(e Epoch) Epoch {
	// Increment by 10 microseconds (0.00001 seconds)
	// This maintains the 10µs quantization scheme and prevents
	// excessive precision that would violate JSON float64 round-trip safety
	return e + 0.00001
}

// EpochBetween returns an epoch between l and r (closer to l).
// Used when inserting events with dirty epochs.
// Assumes l > r.
func EpochBetween(l, r Epoch) Epoch {
	// Simple arithmetic mean
	mean := (float64(l) + float64(r)) / 2.0

	// Ensure the mean is actually between l and r
	// (it should be, but floating point can be tricky)
	if mean > float64(r) && mean < float64(l) {
		return Epoch(mean)
	}

	// Fallback: just slightly increment r
	return EpochIncreaseABit(r)
}

// IsZero returns true if the epoch is zero or empty.
func (e Epoch) IsZero() bool {
	return e == 0.0
}

// String returns the epoch as a string with appropriate precision.
func (e Epoch) String() string {
	// Use %.5f for 10-microsecond precision (5 decimal places)
	s := fmt.Sprintf("%.5f", float64(e))
	// Trim trailing zeros but keep at least one decimal place
	for len(s) > 0 && s[len(s)-1] == '0' && s[len(s)-2] != '.' {
		s = s[:len(s)-1]
	}
	return s
}

// UnmarshalJSON implements json.Unmarshaler for Epoch.
// It handles both JSON numbers and JSON strings (from Perl).
func (e *Epoch) UnmarshalJSON(data []byte) error {
	// Try unmarshaling as a number first
	var f float64
	if err := json.Unmarshal(data, &f); err == nil {
		*e = Epoch(f)
		return nil
	}

	// Try unmarshaling as a string
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		// Parse string as float
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("invalid epoch string %q: %w", s, err)
		}
		*e = Epoch(f)
		return nil
	}

	return fmt.Errorf("epoch must be a number or string, got: %s", string(data))
}
