package bloom

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"math"
)

type ClassicBloomFilter struct {
	name  string
	m     uint // filter size
	k     uint // number of hash functions
	count uint // number of items added

	buckets *Buckets    // filter data
	hash    hash.Hash64 // hash function (kernel for all k functions)
}

func NewClassicBloomFilter(options FilterOptions) (Filter, error) {
	if options.ErrorRate == 0 || options.N == 0 {
		return nil, fmt.Errorf("illegal params")
	}

	m := OptimalM(options.N, options.ErrorRate)

	return &ClassicBloomFilter{
		name:    options.Name,
		buckets: NewBuckets(m, 1),
		hash:    fnv.New64(),
		m:       m,
		k:       OptimalK(options.ErrorRate),
	}, nil
}

func (b *ClassicBloomFilter) Name() string {
	return b.name
}
func (b *ClassicBloomFilter) Capacity() uint {
	return b.m
}

func (b *ClassicBloomFilter) K() uint {
	return b.k
}

func (b *ClassicBloomFilter) Count() uint {
	return b.count
}

func (b *ClassicBloomFilter) EstimatedFillRatio() float64 {
	return 1 - math.Exp((-float64(b.count)*float64(b.k))/float64(b.m))
}

func (b *ClassicBloomFilter) FillRatio() float64 {
	sum := uint32(0)
	for i := uint(0); i < b.buckets.Count(); i++ {
		sum += b.buckets.Get(i)
	}
	return float64(sum) / float64(b.m)
}

func (b *ClassicBloomFilter) Test(data []byte) bool {
	lower, upper := hashKernel(data, b.hash)

	for i := uint(0); i < b.k; i++ {
		if b.buckets.Get((uint(lower)+uint(upper)*i)%b.m) == 0 {
			return false
		}
	}

	return true
}

func (b *ClassicBloomFilter) Add(data []byte) Filter {
	lower, upper := hashKernel(data, b.hash)

	for i := uint(0); i < b.k; i++ {
		b.buckets.Set((uint(lower)+uint(upper)*i)%b.m, 1)
	}

	b.count++
	return b
}

func (b *ClassicBloomFilter) TestAndAdd(data []byte) bool {
	lower, upper := hashKernel(data, b.hash)
	member := true

	for i := uint(0); i < b.k; i++ {
		idx := (uint(lower) + uint(upper)*i) % b.m
		if b.buckets.Get(idx) == 0 {
			member = false
		}
		b.buckets.Set(idx, 1)
	}

	b.count++
	return member
}

func (b *ClassicBloomFilter) Reset() {
	b.buckets.Reset()
}

func (b *ClassicBloomFilter) SetHash(h hash.Hash64) {
	b.hash = h
}

func (b *ClassicBloomFilter) Load(stream io.Reader) error {
	var count uint64

	err := binary.Read(stream, binary.BigEndian, &count)
	if err != nil {
		return err
	}
	_, err = b.buckets.Load(stream)
	b.count = uint(count)
	return err
}

func (b *ClassicBloomFilter) Dump(stream io.Writer) error {
	err := binary.Write(stream, binary.BigEndian, uint64(b.count))
	if err != nil {
		return err
	}

	_, err = b.buckets.Dump(stream)
	return err
}
