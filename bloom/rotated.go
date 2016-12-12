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
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/alecthomas/log4go"
)

var (
	_ = fmt.Println
)

type RotatedBloomFilter struct {
	sync.RWMutex

	name string
	r    uint // R is how many rounds keeped

	current uint

	rotateInterval time.Duration
	lastRotated    time.Time
	innerFilters   []Filter
}

type RotatedBloomFilterHeader struct {
	R       uint
	Current uint
	Name    string

	RotatedInterval time.Duration
	LastRotated     time.Time
}

type RotatedBloomFilterChunk struct {
	BodyLen int32
	Data    []byte
}

func NewRotatedBloomFilter(options FilterOptions) (Filter, error) {
	if options.R <= 0 {
		return nil, fmt.Errorf("invalid r, at least one")
	}

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
		lastRotated:    time.Unix(0, 0),

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

func (b *RotatedBloomFilter) Add(key []byte) Filter {
	b.Lock()
	defer b.Unlock()

	ch := make(chan bool, b.r)

	for i := 0; i < int(b.r); i++ {
		go func(filter Filter) {
			filter.Add(key)

			ch <- true
		}(b.innerFilters[i])
	}

	for i := 0; i < int(b.r); i++ {
		<-ch
	}

	return b
}

func (b *RotatedBloomFilter) Test(key []byte) bool {
	b.RLock()
	defer b.RUnlock()
	return b.innerFilters[b.current].Test(key)
}

func (b *RotatedBloomFilter) PeriodMaintaince(persister FilterPersister, force bool) error {
	need_rotated := time.Now().Sub(b.lastRotated) >= b.rotateInterval
	if need_rotated || force {
		writer, err := persister.NewWriter(b.name)
		defer writer.Close()

		log4go.Info("period rotated bloom filter: %s", b.name)
		if err != nil {
			log4go.Warn("create writer error:%v", err)
			return err
		}
		if err = dumpFilter(writer, b); err != nil {
			log4go.Warn("dumpfilter error:%v", err)
			return err
		} else {
			if need_rotated {
				last := b.lastRotated
				b.Lock()
				if b.lastRotated == last {
					b.dropOneRep()
					b.lastRotated = time.Now()

					log4go.Info("Filter %s rotated to %d, next rotated time to %v", b.name, b.current, b.lastRotated.Add(b.rotateInterval))
				}
				b.Unlock()

			}
		}
	}

	return nil
}

func (b *RotatedBloomFilter) dropOneRep() {
	b.innerFilters[b.current].Reset()
	b.current = (b.current + 1) % b.r
}

func (b *RotatedBloomFilter) Destroy() {
}

func (b *RotatedBloomFilter) Load(r io.Reader) error {
	b.RLock()
	defer b.RUnlock()
	dec := gob.NewDecoder(r)

	header := RotatedBloomFilterHeader{}
	if err := dec.Decode(&header); err != nil {
		log4go.Warn("load header error: %v", err)
		return ILLEGAL_LOAD_FORMAT
	}

	b.r = header.R
	if b.r == 0 {
		log4go.Warn("suspicous filter, r is zero")
		return ILLEGAL_LOAD_FORMAT
	}

	b.current = header.Current
	b.name = header.Name
	b.lastRotated = header.LastRotated
	b.rotateInterval = header.RotatedInterval

	b.innerFilters = make([]Filter, b.r)

	for i := uint(0); i < b.r; i++ {
		chunk := RotatedBloomFilterChunk{}
		if err := dec.Decode(&chunk); err != nil {
			log4go.Warn("get chunk of %d error: %v", i, err)
			return ILLEGAL_LOAD_FORMAT
		}

		if len(chunk.Data) != int(chunk.BodyLen) {
			log4go.Warn("chunk %d len %d not equal to data len %d", chunk.BodyLen, len(chunk.Data))
		}
		if filter, err := loadFilter(bytes.NewBuffer(chunk.Data)); err != nil {
			log4go.Warn("load filter error: %v", err)
			return ILLEGAL_LOAD_FORMAT
		} else {
			b.innerFilters[i] = filter
		}
	}

	log4go.Info("load rotated filter, name:%s current:%d r:%d last_rotated:%+v rotated_interval:%+v next_interval_time:%+v",
		b.name, b.current, b.r, b.lastRotated, b.rotateInterval, b.lastRotated.Add(b.rotateInterval))
	return nil
}

func (b *RotatedBloomFilter) Dump(w io.Writer) error {
	//	b.Lock()
	//	defer b.Unlock()
	enc := gob.NewEncoder(w)

	header := RotatedBloomFilterHeader{
		R:               b.r,
		Current:         b.current,
		Name:            b.name,
		LastRotated:     b.lastRotated,
		RotatedInterval: b.rotateInterval,
	}

	if err := enc.Encode(&header); err != nil {
		log4go.Warn("write header error: %v", err)
		return err
	}

	for i := 0; i < int(b.r); i++ {
		buffer := new(bytes.Buffer)

		if err := dumpFilter(buffer, b.innerFilters[i]); err != nil {
			log4go.Warn("write inner filter %d error: %v", i, err)
			return err
		}

		if err := enc.Encode(&RotatedBloomFilterChunk{
			BodyLen: int32(buffer.Len()),
			Data:    buffer.Bytes(),
		}); err != nil {
			log4go.Warn("write chunked error: %v", err)
			return err
		}
	}

	return nil
}
