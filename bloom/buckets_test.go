package bloom

import (
	"testing"
)

// Ensures that MaxBucketValue returns the correct maximum based on the bucket
// size.
func TestMaxBucketValue(t *testing.T) {
	b := NewBuckets(10, 2)

	if max := b.MaxBucketValue(); max != 3 {
		t.Errorf("Expected 3, got %d", max)
	}
}

// Ensures that Count returns the number of buckets.
func TestBucketsCount(t *testing.T) {
	b := NewBuckets(10, 2)

	if count := b.Count(); count != 10 {
		t.Errorf("Expected 10, got %d", count)
	}
}

// Ensures that Increment increments the bucket value by the correct delta and
// clamps to zero and the maximum, Get returns the correct bucket value, and
// Set sets the bucket value correctly.
func TestBucketsIncrementAndGetAndSet(t *testing.T) {
	b := NewBuckets(5, 2)

	if b.Increment(0, 1) != b {
		t.Error("Returned Buckets should be the same instance")
	}

	if v := b.Get(0); v != 1 {
		t.Errorf("Expected 1, got %d", v)
	}

	b.Increment(1, -1)

	if v := b.Get(1); v != 0 {
		t.Errorf("Expected 0, got %d", v)
	}

	if b.Set(2, 100) != b {
		t.Error("Returned Buckets should be the same instance")
	}

	if v := b.Get(2); v != 3 {
		t.Errorf("Expected 3, got %d", v)
	}

	b.Increment(3, 2)

	if v := b.Get(3); v != 2 {
		t.Errorf("Expected 2, got %d", v)
	}
}

// Ensures that Reset restores the Buckets to the original state.
func TestBucketsReset(t *testing.T) {
	b := NewBuckets(5, 2)
	for i := 0; i < 5; i++ {
		b.Increment(uint(i), 1)
	}

	if b.Reset() != b {
		t.Error("Returned Buckets should be the same instance")
	}

	for i := 0; i < 5; i++ {
		if c := b.Get(uint(i)); c != 0 {
			t.Errorf("Expected 0, got %d", c)
		}
	}
}
