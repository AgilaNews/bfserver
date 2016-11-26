package bloom

import (
	"encoding/gob"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"math"

	"github.com/alecthomas/log4go"
)

type ClassicBloomFilter struct {
	name  string
	m     uint // filter size
	k     uint // number of hash functions
	count uint // number of items added

	buckets *Buckets    // filter data
	hash    hash.Hash64 // hash function (kernel for all k functions)
}

type ClassicBloomFilterDumpHeader struct {
	Name  string
	M     uint
	K     uint
	Count uint
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

func (b *ClassicBloomFilter) PeriodMaintaince(persister FilterPersister, force bool) error {
	if force {
		writer, err := persister.NewWriter(b.name)
		defer writer.Close()

		log4go.Info("period dump classic bloom filter: %s", b.name)
		if err != nil {
			log4go.Warn("create writer error:%v", err)
			return err
		}
		if err = dumpFilter(writer, b); err != nil {
			log4go.Warn("dumpfilter error:%v", err)
			return err
		}
	}

	return nil
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

func (b *ClassicBloomFilter) Load(stream io.Reader) error {
	dec := gob.NewDecoder(stream)
	header := ClassicBloomFilterDumpHeader{}
	err := dec.Decode(&header)
	if err != nil {
		log4go.Warn("read class bloom filter header error")
		return err
	}

	b.name = header.Name
	b.k = header.K
	b.m = header.M
	b.count = header.Count
	b.buckets = NewBuckets(b.m, 1)
	b.hash = fnv.New64()
	log4go.Info("loaded classic filter name:%s k:%d m:%d count:%d", b.name, b.k, b.m, b.count)

	return b.buckets.Load(stream)
}

func (b *ClassicBloomFilter) Dump(stream io.Writer) error {
	enc := gob.NewEncoder(stream)

	header := ClassicBloomFilterDumpHeader{
		Name:  b.name,
		K:     b.k,
		M:     b.m,
		Count: b.count,
	}

	err := enc.Encode(&header)
	if err != nil {
		log4go.Warn("encode error: %v", err)
		return err
	}
	log4go.Info("dumped filter header with name:%s k:%d m:%d count:%d", b.name, b.k, b.m, b.count)

	return b.buckets.Dump(stream)
}
