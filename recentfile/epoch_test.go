package recentfile

import (
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

	// Convert back and check
	epochFloat := EpochToFloat(epoch)
	nowFloat := float64(now.Unix()) + float64(now.Nanosecond())/1e9

	diff := nowFloat - epochFloat
	if diff < -0.001 || diff > 0.001 {
		t.Errorf("EpochFromTime() precision loss: diff = %v", diff)
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

func BenchmarkEpochNow(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EpochNow()
	}
}
