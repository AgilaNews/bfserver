package bloom

import (
	"encoding/gob"
	"github.com/alecthomas/log4go"
	"io"
)

type Buckets struct {
	data       []byte
	bucketSize uint8
	max        uint8
	count      uint
}

type BucketsDump struct {
	Data  []byte
	Max   uint8
	Size  uint8
	Count uint
}

func NewBuckets(count uint, bucketSize uint8) *Buckets {
	return &Buckets{
		count:      count,
		data:       make([]byte, (count*uint(bucketSize)+7)/8),
		bucketSize: bucketSize,
		max:        (1 << bucketSize) - 1,
	}
}

func (b *Buckets) MaxBucketValue() uint8 {
	return b.max
}

func (b *Buckets) Count() uint {
	return b.count
}

func (b *Buckets) Increment(bucket uint, delta int32) *Buckets {
	val := int32(b.getBits(bucket*uint(b.bucketSize), uint(b.bucketSize))) + delta

	if val > int32(b.max) {
		val = int32(b.max)
	} else if val < 0 {
		val = 0
	}

	b.setBits(uint32(bucket)*uint32(b.bucketSize), uint32(b.bucketSize), uint32(val))

	return b
}

func (b *Buckets) Set(bucket uint, value uint8) *Buckets {
	if value > b.max {
		value = b.max
	}

	b.setBits(uint32(bucket)*uint32(b.bucketSize), uint32(b.bucketSize), uint32(value))
	return b
}

func (b *Buckets) Get(bucket uint) uint32 {
	return b.getBits(bucket*uint(b.bucketSize), uint(b.bucketSize))
}

func (b *Buckets) Reset() *Buckets {
	b.data = make([]byte, (b.count*uint(b.bucketSize)+7)/8)
	return b
}

func (b *Buckets) getBits(offset, length uint) uint32 {
	return b.i_get_bits(offset, length)
}

func (b *Buckets) i_get_bits(offset, length uint) uint32 {
	byteIndex := offset / 8
	byteOffset := offset % 8
	if byteOffset+length > 8 {
		rem := 8 - byteOffset
		return b.i_get_bits(offset, rem) | (b.i_get_bits(offset+rem, length-rem) << rem)
	}
	bitMask := uint32((1 << length) - 1)
	return (uint32(b.data[byteIndex]) & (bitMask << byteOffset)) >> byteOffset
}

func (b *Buckets) setBits(offset, length, bits uint32) {
	b.i_set_bits(offset, length, bits)
}

func (b *Buckets) i_set_bits(offset, length, bits uint32) {
	byteIndex := offset / 8
	byteOffset := offset % 8
	if byteOffset+length > 8 {
		rem := 8 - byteOffset
		b.i_set_bits(offset, rem, bits)
		b.i_set_bits(offset+rem, length-rem, bits>>rem)
		return
	}
	bitMask := uint32((1 << length) - 1)
	b.data[byteIndex] = byte(uint32(b.data[byteIndex]) & ^(bitMask << byteOffset))
	b.data[byteIndex] = byte(uint32(b.data[byteIndex]) | ((bits & bitMask) << byteOffset))
}

func (b *Buckets) Dump(stream io.Writer) error {
	enc := gob.NewEncoder(stream)
	d := BucketsDump{
		Max:   b.max,
		Data:  b.data,
		Count: b.count,
		Size:  b.bucketSize,
	}

	return enc.Encode(&d)
}

func (b *Buckets) Load(stream io.Reader) error {
	d := &BucketsDump{}
	dec := gob.NewDecoder(stream)
	if err := dec.Decode(d); err != nil {
		log4go.Info("load bucket error: %+v", err)
		return err
	}

	b.max = d.Max
	b.data = d.Data
	b.bucketSize = d.Size
	b.count = d.Count

	return nil
}
