package bloom

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
)

type Buckets struct {
	sync.RWMutex

	data       []byte
	bucketSize uint8
	max        uint8
	count      uint
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
	b.Lock()
	defer b.Unlock()

	b.data = make([]byte, (b.count*uint(b.bucketSize)+7)/8)
	return b
}

func (b *Buckets) getBits(offset, length uint) uint32 {
	b.RLock()
	defer b.RUnlock()

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
	b.Lock()
	defer b.Unlock()

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

func (b *Buckets) Dump(stream io.Writer) (int64, error) {
	b.RLock()
	defer b.RUnlock()
	err := binary.Write(stream, binary.BigEndian, b.bucketSize)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, b.max)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(b.count))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(len(b.data)))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, b.data)
	if err != nil {
		return 0, err
	}
	return int64(len(b.data) + 2*binary.Size(uint8(0)) + 2*binary.Size(uint64(0))), err
}

func (b *Buckets) Load(stream io.Reader) (int64, error) {
	b.Lock()
	defer b.Unlock()

	var bucketSize, max uint8
	var count, len uint64
	err := binary.Read(stream, binary.BigEndian, &bucketSize)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &max)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &count)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &len)
	if err != nil {
		return 0, err
	}
	data := make([]byte, len)
	err = binary.Read(stream, binary.BigEndian, &data)
	if err != nil {
		return 0, err
	}
	b.bucketSize = bucketSize
	b.max = max
	b.count = uint(count)
	b.data = data
	return int64(int(len) + 2*binary.Size(uint8(0)) + 2*binary.Size(uint64(0))), nil
}

func (b *Buckets) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	_, err := b.Dump(&buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (b *Buckets) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	_, err := b.Load(buf)

	return err
}
