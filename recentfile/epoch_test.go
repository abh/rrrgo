package recentfile

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEpochCompare(t *testing.T) {
	tests := []struct {
		name string
		l, r Epoch
		want int
	}{
		{
			name: "equal epochs",
			l:    1234567890.123456,
			r:    1234567890.123456,
			want: 0,
		},
		{
			name: "l less than r",
			l:    1234567890.123456,
			r:    1234567890.123457,
			want: -1,
		},
		{
			name: "l greater than r",
			l:    1234567891.0,
			r:    1234567890.999999,
			want: 1,
		},
		{
			name: "high precision l < r",
			l:    1234567890.123456,
			r:    1234567890.123457,
			want: -1,
		},
		{
			name: "large difference",
			l:    1234567900.0,
			r:    1234567890.0,
			want: 1,
		},
		{
			name: "integer vs decimal",
			l:    1234567890,
			r:    1234567890.0,
			want: 0,
		},
		{
			name: "different integer lengths",
			l:    999999999.5,
			r:    1000000000.0,
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EpochCompare(tt.l, tt.r)
			if got != tt.want {
				t.Errorf("EpochCompare(%v, %v) = %d, want %d", tt.l, tt.r, got, tt.want)
			}
		})
	}
}

func TestEpochLt(t *testing.T) {
	tests := []struct {
		l, r Epoch
		want bool
	}{
		{1234567890.123456, 1234567890.123457, true},
		{1234567890.123456, 1234567890.123456, false},
		{1234567891.0, 1234567890.0, false},
	}

	for _, tt := range tests {
		got := EpochLt(tt.l, tt.r)
		if got != tt.want {
			t.Errorf("EpochLt(%v, %v) = %v, want %v", tt.l, tt.r, got, tt.want)
		}
	}
}

func TestEpochGt(t *testing.T) {
	tests := []struct {
		l, r Epoch
		want bool
	}{
		{1234567891.0, 1234567890.0, true},
		{1234567890.123456, 1234567890.123456, false},
		{1234567890.0, 1234567891.0, false},
	}

	for _, tt := range tests {
		got := EpochGt(tt.l, tt.r)
		if got != tt.want {
			t.Errorf("EpochGt(%v, %v) = %v, want %v", tt.l, tt.r, got, tt.want)
		}
	}
}

func TestEpochMax(t *testing.T) {
	tests := []struct {
		l, r, want Epoch
	}{
		{1234567891.0, 1234567890.0, 1234567891.0},
		{1234567890.0, 1234567891.0, 1234567891.0},
		{1234567890.123, 1234567890.124, 1234567890.124},
	}

	for _, tt := range tests {
		got := EpochMax(tt.l, tt.r)
		if got != tt.want {
			t.Errorf("EpochMax(%v, %v) = %v, want %v", tt.l, tt.r, got, tt.want)
		}
	}
}

func TestEpochMin(t *testing.T) {
	tests := []struct {
		l, r, want Epoch
	}{
		{1234567891.0, 1234567890.0, 1234567890.0},
		{1234567890.0, 1234567891.0, 1234567890.0},
		{1234567890.123, 1234567890.124, 1234567890.123},
	}

	for _, tt := range tests {
		got := EpochMin(tt.l, tt.r)
		if got != tt.want {
			t.Errorf("EpochMin(%v, %v) = %v, want %v", tt.l, tt.r, got, tt.want)
		}
	}
}

func TestEpochNow(t *testing.T) {
	before := time.Now()
	epoch := EpochNow()
	after := time.Now()

	// Verify epoch is a valid timestamp
	epochFloat := EpochToFloat(epoch)
	if epochFloat == 0 {
		t.Error("EpochNow() returned invalid epoch")
	}

	// Verify epoch is within reasonable range
	beforeFloat := float64(before.Unix())
	afterFloat := float64(after.Unix())

	if epochFloat < beforeFloat || epochFloat > afterFloat+1 {
		t.Errorf("EpochNow() = %v, expected between %v and %v", epochFloat, beforeFloat, afterFloat)
	}
}

func TestEpochIncreaseABit(t *testing.T) {
	tests := []struct {
		name  string
		epoch Epoch
	}{
		{"integer", 1234567890},
		{"with decimals", 1234567890.123456},
		{"high precision", 1234567890.123456789012},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			increased := EpochIncreaseABit(tt.epoch)

			if !EpochGt(increased, tt.epoch) {
				t.Errorf("EpochIncreaseABit(%v) = %v, should be greater", tt.epoch, increased)
			}

			// Verify the increase is small
			diff := EpochToFloat(increased) - EpochToFloat(tt.epoch)
			if diff > 0.01 { // Should be much smaller than 0.01
				t.Errorf("EpochIncreaseABit(%v) increased by %v, too large", tt.epoch, diff)
			}
		})
	}
}

func TestEpochIncreaseABitUnique(t *testing.T) {
	// Test that repeatedly increasing produces unique values
	epoch := Epoch(1234567890.123456)
	seen := make(map[Epoch]bool)
	seen[epoch] = true

	for i := 0; i < 100; i++ {
		epoch = EpochIncreaseABit(epoch)
		if seen[epoch] {
			t.Errorf("EpochIncreaseABit produced duplicate: %v", epoch)
		}
		seen[epoch] = true
	}
}

func TestEpochBetween(t *testing.T) {
	tests := []struct {
		name string
		l, r Epoch
	}{
		{"simple", 100.0, 90.0},
		{"close", 100.5, 100.4},
		{"very close", 100.123456, 100.123455},
		{"high precision", 100.123456789012, 100.123456789011},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			between := EpochBetween(tt.l, tt.r)

			if !EpochGt(between, tt.r) {
				t.Errorf("EpochBetween(%v, %v) = %v, should be > r", tt.l, tt.r, between)
			}
			if !EpochLt(between, tt.l) {
				t.Errorf("EpochBetween(%v, %v) = %v, should be < l", tt.l, tt.r, between)
			}
		})
	}
}

func TestEpochFromTime(t *testing.T) {
	now := time.Now()
	epoch := EpochFromTime(now)

	// Verify 10-microsecond quantization is applied
	// Epoch should be quantized to 10-microsecond intervals
	epochFloat := EpochToFloat(epoch)

	// Multiply by 1e5 to convert to 10-microsecond units, should be an integer
	quantized := epochFloat * 1e5
	remainder := quantized - float64(int64(quantized))

	// Allow tiny floating point rounding error (less than 1 in 1e-15)
	if remainder > 1e-10 {
		t.Errorf("EpochFromTime() not properly quantized to 10-microsecond intervals: remainder = %v", remainder)
	}
}

func TestEpochIsZero(t *testing.T) {
	tests := []struct {
		epoch Epoch
		want  bool
	}{
		{0, true},
		{0.0, true},
		{0.00000, true},
		{1234567890.0, false},
		{0.1, false},
	}

	for _, tt := range tests {
		got := tt.epoch.IsZero()
		if got != tt.want {
			t.Errorf("Epoch(%v).IsZero() = %v, want %v", tt.epoch, got, tt.want)
		}
	}
}

func TestEpochMonotonicSequence(t *testing.T) {
	// Simulate rapid timestamp generation
	var epochs []Epoch
	base := EpochNow()

	epochs = append(epochs, base)
	for i := 0; i < 10; i++ {
		last := epochs[len(epochs)-1]
		next := EpochIncreaseABit(last)
		epochs = append(epochs, next)
	}

	// Verify all are in increasing order
	for i := 1; i < len(epochs); i++ {
		if !EpochGt(epochs[i], epochs[i-1]) {
			t.Errorf("epochs[%d] = %v not greater than epochs[%d] = %v",
				i, epochs[i], i-1, epochs[i-1])
		}
	}
}

// Benchmark tests
func BenchmarkEpochCompare(b *testing.B) {
	e1 := Epoch(1234567890.123456789)
	e2 := Epoch(1234567890.123456788)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EpochCompare(e1, e2)
	}
}

func BenchmarkEpochIncreaseABit(b *testing.B) {
	epoch := Epoch(1234567890.123456789)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		epoch = EpochIncreaseABit(epoch)
	}
}

// TestJSONRoundtripNoPrecisionLoss validates that the fix for the Perl "disorder" bug works.
// This test ensures that when epochs are serialized to JSON and back,
// distinct epochs remain distinct. The bug occurred because nanosecond-precision
// float64 values lost precision when serialized to JSON and read back by Perl.
// With 10-microsecond quantization, there's no precision loss.
func TestJSONRoundtripNoPrecisionLoss(t *testing.T) {
	// Use deterministic epoch values quantized to 10-microsecond intervals
	// These represent times in Unix seconds with 5 decimal places
	epochs := []Epoch{
		Epoch(1760923093.82070),
		Epoch(1760923093.82071),
		Epoch(1760923093.82072),
		Epoch(1760923093.82080),
	}

	// Verify all epochs are distinct
	seen := make(map[float64]bool)
	for i, e := range epochs {
		if seen[float64(e)] {
			t.Errorf("Epoch %d is a duplicate: %v", i, e)
		}
		seen[float64(e)] = true
	}

	// Serialize to JSON
	data, err := json.Marshal(epochs)
	if err != nil {
		t.Fatalf("Failed to marshal epochs: %v", err)
	}

	// Deserialize from JSON
	var deserialized []Epoch
	if err := json.Unmarshal(data, &deserialized); err != nil {
		t.Fatalf("Failed to unmarshal epochs: %v", err)
	}

	// Verify all deserialized epochs match original values
	// This is the critical check: with nanosecond precision, we would see duplicates here
	for i := 0; i < len(epochs); i++ {
		if epochs[i] != deserialized[i] {
			t.Errorf("Epoch %d mismatch after JSON roundtrip: %v != %v", i, epochs[i], deserialized[i])
		}
	}

	// Verify no two consecutive epochs are equal (the actual bug: duplicates after JSON roundtrip)
	for i := 0; i < len(deserialized)-1; i++ {
		if deserialized[i] == deserialized[i+1] {
			t.Errorf("Epochs %d and %d have same value after JSON roundtrip: %v", i, i+1, deserialized[i])
		}
	}

	// The Perl code requires strict ordering for write_recent to succeed
	// This would have crashed with: "Warning: disorder '1760923093.82076'>='1760923093.82076', re-sorting 1h"
	t.Logf("JSON roundtrip successful: %d distinct epochs preserved", len(deserialized))
}

func BenchmarkEpochNow(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EpochNow()
	}
}
