package bloom

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash"
	"io"
	"math"
	"sync"
	"time"

	"github.com/alecthomas/log4go"
)

const (
	DEFAULT_FILL_RATIO = 0.5

	FILTER_CLASSIC = "classic"
	FILTER_ROTATED = "rotated"
)

var (
	ILLEGAL_LOAD_FORMAT = fmt.Errorf("illegal load format")
	DUMP_ERROR          = fmt.Errorf("dump error")
)

type FilterOptions struct {
	Name string

	N         uint
	ErrorRate float64

	R              uint
	RotateInterval time.Time
}

type FilterManager struct {
	sync.RWMutex

	Filters  map[string]Filter
	TotalMem uint64
}

type DumpHeader struct {
	Magic      uint   //magic number of dump header
	FilterType string //filter type
}

type Filter interface {
	Test([]byte) bool
	Add([]byte) Filter

	Reset()
	SetHash(h hash.Hash64)

	//info interface
	Name() string
	Capacity() uint
	K() uint
	Count() uint
	EstimatedFillRatio() float64
	FillRatio() float64

	//persist
	Load(reader io.Reader) error
	Dump(writer io.Writer) error
}

func OptimalM(n uint, fpRate float64) uint {
	return uint(math.Ceil(float64(n) / ((math.Log(DEFAULT_FILL_RATIO) *
		math.Log(1-DEFAULT_FILL_RATIO)) / math.Abs(math.Log(fpRate)))))
}

func OptimalK(fpRate float64) uint {
	return uint(math.Ceil(math.Log2(1 / fpRate)))
}

func hashKernel(data []byte, hash hash.Hash64) (uint32, uint32) {
	hash.Write(data)
	sum := hash.Sum(nil)
	hash.Reset()
	return binary.BigEndian.Uint32(sum[4:8]), binary.BigEndian.Uint32(sum[0:4])
}

func BatchAdd(f Filter, keys []string, wait bool) {
	if wait {
		chs := make([]chan bool, len(keys))

		for i, key := range keys {
			go func(k string, ch chan bool) {
				f.Add([]byte(k))

				ch <- true
			}(key, chs[i])
		}

		for _, ch := range chs {
			<-ch
		}
	} else {
		for _, key := range keys {
			go f.Add([]byte(key))
		}
	}
}

func BatchTest(f Filter, keys []string) []bool {
	chs := make([]chan bool, len(keys))
	ret := make([]bool, len(keys))

	for i, str := range keys {
		go func(ch chan bool) {
			ch <- f.Test([]byte(str))
		}(chs[i])
	}

	for i, ch := range chs {
		ret[i] = <-ch
	}

	return ret
}

func NewFilterManager() (*FilterManager, error) {
	return &FilterManager{
		Filters: make(map[string]Filter),
	}, nil
}

func isOptionsValid(options FilterOptions) error {
	return nil
}

func (m *FilterManager) CreateNewBloomFilter(t string, options FilterOptions) (Filter, error) {
	var filter Filter
	var err error

	if err = isOptionsValid(options); err != nil {
		return nil, err
	}

	switch t {
	case FILTER_CLASSIC:
		filter, err = NewClassicBloomFilter(options)
	case FILTER_ROTATED:
		filter, err = NewRotatedBloomFilter(options)
	default:
		return nil, fmt.Errorf("invalid bf type: %s", t)
	}

	m.Lock()
	defer m.Unlock()
	m.Filters[options.Name] = filter

	return filter, nil
}

func (m *FilterManager) GetBloomFilter(t string) (Filter, error) {
	m.RLock()
	defer m.RUnlock()

	f, ok := m.Filters[t]
	if !ok {
		return f, fmt.Errorf("filter non exists")
	}

	return f, nil
}

func LoadFilter(reader io.Reader) (Filter, error) {
	dumpHeader := DumpHeader{}

	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&dumpHeader); err != nil {
		log4go.Warn("read dump header error : %v", err)
		return nil, ILLEGAL_LOAD_FORMAT
	}
	if dumpHeader.Magic != MAGIC_NUM {
		log4go.Warn("mismatch magic number")
		return nil, ILLEGAL_LOAD_FORMAT
	}

	greader, err := gzip.NewReader(reader)
	if err != nil {
		log4go.Warn("decompress error: %v", err)
		return nil, err
	}

	switch dumpHeader.FilterType {
	case FILTER_CLASSIC:
		f := &ClassicBloomFilter{}
		if err := f.Load(greader); err != nil {
			return nil, err
		}

		return f, nil
	case FILTER_ROTATED:
		f := &RotatedBloomFilter{}
		if err := f.Load(greader); err != nil {
			return nil, err
		}

		return f, nil
	default:
		log4go.Warn("unknown filter type :%v", dumpHeader.FilterType)
		return nil, ILLEGAL_LOAD_FORMAT
	}
}

func DumpFilter(writer io.Writer, filter Filter) error {
	dumpHeader := DumpHeader{
		Magic: MAGIC_NUM,
	}

	switch filter.(type) {
	case *ClassicBloomFilter:
		dumpHeader.FilterType = FILTER_CLASSIC
	case *RotatedBloomFilter:
		dumpHeader.FilterType = FILTER_ROTATED
	default:
		panic("what the fuck type")
	}

	enc := gob.NewEncoder(writer)
	if err := enc.Encode(&dumpHeader); err != nil {
		log4go.Warn("encode header error: %v", err)
		return DUMP_ERROR
	}

	gwriter := gzip.NewWriter(writer)
	return filter.Dump(gwriter)
}
