package bloom

/*
 *  @Date    : 2016-10-18 10:50:30
 *  @Author  : Zhao Yulong (elysium.zyl@gmail.com)
 *  @Link    : ${link}
 *  @Describe: bloomfilter with routing
 */

import (
	"bytes"
	"encoding/gob"
	"hash"
	"io"
	"sync"
	"time"

	"github.com/alecthomas/log4go"
)

const (
	MAGIC_NUM = 0x2345fe13
)

type RotatedBloomFilter struct {
	sync.RWMutex

	name string
	r    uint // R is how many rounds keeped

	current  uint
	dumpPath string

	rotateInterval time.Time
	innerFilters   []Filter
}

type RotatedBloomFilterHeader struct {
	Magic   uint
	R       uint
	Current uint
	Name    string
}

type RotatedBloomFilterChunk struct {
	BodyLen int32
	Data    []byte
}

func NewRotatedBloomFilter(options FilterOptions) (Filter, error) {
	innerFilters := make([]Filter, options.R)
	for i := 0; i < int(options.R); i++ {
		if f, err := NewClassicBloomFilter(options); err != nil {
			return nil, err
		} else {
			innerFilters[i] = f
		}
	}

	return &RotatedBloomFilter{
		name:           options.Name,
		r:              options.R,
		rotateInterval: options.RotateInterval,

		current:      0,
		innerFilters: innerFilters,
	}, nil
}

func (b *RotatedBloomFilter) Name() string {
	return b.name
}

func (b *RotatedBloomFilter) Capacity() uint {
	return b.innerFilters[b.current].Capacity()
}

func (b *RotatedBloomFilter) Count() uint {
	return b.innerFilters[b.current].Count()
}

func (b *RotatedBloomFilter) EstimatedFillRatio() float64 {
	return b.innerFilters[b.current].EstimatedFillRatio()
}

func (b *RotatedBloomFilter) FillRatio() float64 {
	return b.innerFilters[b.current].FillRatio()
}

func (b *RotatedBloomFilter) K() uint {
	return b.innerFilters[b.current].K()
}

func (b *RotatedBloomFilter) Reset() {
	for _, filter := range b.innerFilters {
		filter.Reset()
	}
}

func (b *RotatedBloomFilter) SetHash(h hash.Hash64) {
	for _, filter := range b.innerFilters {
		filter.SetHash(h)
	}
}

func (b *RotatedBloomFilter) Add(key []byte) Filter {
	chs := make([]chan bool, b.r)

	for i := 0; i < int(b.r); i++ {
		go func(ch chan bool) {
			b.innerFilters[i].Add(key)

			ch <- true
		}(chs[i])
	}

	for _, ch := range chs {
		<-ch
	}

	return b
}

func (b *RotatedBloomFilter) Test(key []byte) bool {
	return b.innerFilters[b.current].Test(key)
}

func (b *RotatedBloomFilter) DropOneRep() {
	b.innerFilters[b.current].Reset()

	b.current = (b.current + 1) % b.r
}

func (b *RotatedBloomFilter) Destroy() {
}

func (b *RotatedBloomFilter) Load(r io.Reader) error {
	dec := gob.NewDecoder(r)

	header := RotatedBloomFilterHeader{}
	if err := dec.Decode(&header); err != nil {
		log4go.Warn("write header error: %v", err)
		return ILLEGAL_LOAD_FORMAT
	}
	if header.Magic != MAGIC_NUM {
		log4go.Warn("magic number error")
		return ILLEGAL_LOAD_FORMAT
	}

	b.r = header.R
	if b.r == 0 {
		log4go.Warn("suspicous filter, r is zero")
		return ILLEGAL_LOAD_FORMAT
	}

	b.current = header.Current
	b.name = header.Name

	b.innerFilters = make([]Filter, b.r)

	for i := uint(0); i < b.r; i++ {
		chunk := RotatedBloomFilterChunk{}
		if err := dec.Decode(&chunk); err != nil {
			log4go.Warn("get chunk of %d error", i)
			return ILLEGAL_LOAD_FORMAT
		}

		if filter, err := LoadFilter(bytes.NewBuffer(chunk.Data)); err != nil {
			log4go.Warn("load filter error: %v", err)
			return ILLEGAL_LOAD_FORMAT
		} else {
			b.innerFilters[i] = filter
		}
	}

	return nil
}

func (b *RotatedBloomFilter) Dump(w io.Writer) error {
	enc := gob.NewEncoder(w)

	if err := enc.Encode(RotatedBloomFilterHeader{
		Magic:   MAGIC_NUM,
		R:       b.r,
		Current: b.current,
		Name:    b.name,
	}); err != nil {
		log4go.Warn("write header error: %v", err)
		return err
	}

	for i := 0; i < int(b.r); i++ {
		buffer := new(bytes.Buffer)

		if err := b.innerFilters[i].Dump(buffer); err != nil {
			log4go.Warn("write inner filter %d error: %v", i, err)
			return err
		}

		if err := enc.Encode(RotatedBloomFilterChunk{
			BodyLen: int32(buffer.Len()),
			Data:    buffer.Bytes(),
		}); err != nil {
			log4go.Warn("write chunked error: %v", err)
			return err
		}

		if err := enc.Encode(buffer.Bytes()); err != nil {
			log4go.Warn("write chunked error: %v", err)
			return err
		}
	}

	return nil
}
