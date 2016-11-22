package bloom

import (
	"bytes"
	"strconv"
	"testing"
)

func bucketsEqual(a, b *Buckets) bool {
	if bytes.Compare(a.data, b.data) != 0 || a.bucketSize != b.bucketSize || a.max != b.max || a.count != b.count {
		return false
	}

	return true
}

func classicBloomFilterEqual(a, b *ClassicBloomFilter) bool {
	if a.name != b.name || a.m != b.m || a.k != b.k || a.count != b.count {
		return false
	}

	return bucketsEqual(a.buckets, b.buckets)
}

// Ensures that Capacity returns the number of bits, m, in the Bloom filter.
func TestBloomCapacity(t *testing.T) {
	f, _ := NewClassicBloomFilter(FilterOptions{N: 100, ErrorRate: 0.1})

	if capacity := f.Capacity(); capacity != 480 {
		t.Errorf("Expected 480, got %d", capacity)
	}
}

// Ensures that K returns the number of hash functions in the Bloom Filter.
func TestBloomK(t *testing.T) {
	f, _ := NewClassicBloomFilter(FilterOptions{N: 100, ErrorRate: 0.1})

	if k := f.K(); k != 4 {
		t.Errorf("Expected 4, got %d", k)
	}
}

// Ensures that Count returns the number of items added to the filter.
func TestBloomCount(t *testing.T) {
	f, _ := NewClassicBloomFilter(FilterOptions{N: 100, ErrorRate: 0.1})
	for i := 0; i < 10; i++ {
		f.Add([]byte(strconv.Itoa(i)))
	}

	if count := f.Count(); count != 10 {
		t.Errorf("Expected 10, got %d", count)
	}
}

// Ensures that EstimatedFillRatio returns the correct approximation.
func TestBloomEstimatedFillRatio(t *testing.T) {
	f, _ := NewClassicBloomFilter(FilterOptions{N: 100, ErrorRate: 0.5})
	for i := 0; i < 100; i++ {
		f.Add([]byte(strconv.Itoa(i)))
	}

	if ratio := f.EstimatedFillRatio(); ratio > 0.5 {
		t.Errorf("Expected less than or equal to 0.5, got %f", ratio)
	}
}

// Ensures that FillRatio returns the ratio of set bits.
func TestBloomFillRatio(t *testing.T) {
	f, _ := NewClassicBloomFilter(FilterOptions{N: 100, ErrorRate: 0.1})
	f.Add([]byte(`a`))
	f.Add([]byte(`b`))
	f.Add([]byte(`c`))

	if ratio := f.FillRatio(); ratio != 0.025 {
		t.Errorf("Expected 0.025, got %f", ratio)
	}
}

// Ensures that Test, Add, and TestAndAdd behave correctly.
func TestBloomTestAndAdd(t *testing.T) {
	f, _ := NewClassicBloomFilter(FilterOptions{N: 100, ErrorRate: 0.01})

	// `a` isn't in the filter.
	if f.Test([]byte(`a`)) {
		t.Error("`a` should not be a member")
	}

	if f.Add([]byte(`a`)) != f {
		t.Error("Returned BloomFilter should be the same instance")
	}

	// `a` is now in the filter.
	if !f.Test([]byte(`a`)) {
		t.Error("`a` should be a member")
	}

	// `c` is not in the filter.
	if f.Test([]byte(`c`)) {
		t.Error("`c` should not be a member")
	}
}

// Ensures that Reset sets every bit to zero.
func TestBloomReset(t *testing.T) {
	fs, _ := NewClassicBloomFilter(FilterOptions{N: 100, ErrorRate: 0.1})
	f := fs.(*ClassicBloomFilter)
	for i := 0; i < 1000; i++ {
		f.Add([]byte(strconv.Itoa(i)))
	}

	f.Reset()

	for i := uint(0); i < f.buckets.Count(); i++ {
		if f.buckets.Get(i) != 0 {
			t.Error("Expected all bits to be unset")
		}
	}
}

func BenchmarkBloomAdd(b *testing.B) {
	b.StopTimer()
	f, _ := NewClassicBloomFilter(FilterOptions{N: 100000, ErrorRate: 0.1})
	data := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		data[i] = []byte(strconv.Itoa(i))
	}
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		f.Add(data[n])
	}
}

func BenchmarkBloomTest(b *testing.B) {
	b.StopTimer()
	f, _ := NewClassicBloomFilter(FilterOptions{N: 100000, ErrorRate: 0.1})
	data := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		data[i] = []byte(strconv.Itoa(i))
	}
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		f.Test(data[n])
	}
}
