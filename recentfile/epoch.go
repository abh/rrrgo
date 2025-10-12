package recentfile

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Epoch represents a high-precision timestamp as a float64.
// This matches the Perl implementation and is memory efficient.
// Format: Unix timestamp with fractional seconds (e.g., 1760007882.98731)
type Epoch float64

// EpochNow returns the current time as an Epoch.
func EpochNow() Epoch {
	now := time.Now()
	return Epoch(float64(now.Unix()) + float64(now.Nanosecond())/1e9)
}

// EpochFromTime converts a time.Time to an Epoch.
func EpochFromTime(t time.Time) Epoch {
	return Epoch(float64(t.Unix()) + float64(t.Nanosecond())/1e9)
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
func EpochIncreaseABit(e Epoch) Epoch {
	// Use math.Nextafter to get the next representable float64 value
	return Epoch(math.Nextafter(float64(e), math.Inf(1)))
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
	// Use %.9f to match typical Unix timestamp precision
	s := fmt.Sprintf("%.9f", float64(e))
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
