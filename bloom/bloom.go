package bloom

import (
	"bufio"
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
	MAGIC_NUM      = 0x123553f3
)

var (
	ILLEGAL_LOAD_FORMAT = fmt.Errorf("illegal load format")
	DUMP_ERROR          = fmt.Errorf("dump error")

	Manager *FilterManager
	UseGzip = true
)

type FilterOptions struct {
	Name string

	N         uint
	ErrorRate float64

	R              uint
	RotateInterval time.Duration
}

type FilterManager struct {
	sync.RWMutex

	stop        chan bool
	persister   FilterPersister
	persistChan chan bool

	Filters  map[string]Filter
	TotalMem uint64

	forceDumpPeriod time.Duration
	lastForce       time.Time
}

type DumpHeader struct {
	Magic          uint //magic number of dump header
	FilterUsedGzip bool
	FilterType     string //filter type
}

type Filter interface {
	Test([]byte) bool
	Add([]byte) Filter

	Reset()
	PeriodMaintaince(persister FilterPersister, force bool) error

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
	/*
		if wait {
			ch := make(chan bool, len(keys))

			for i := 0; i < len(keys); i++ {
				go func(b []byte) {
					f.Add(b)

					ch <- true
				}([]byte(keys[i]))
			}

			for i := 0; i < len(keys); i++ {
				<-ch
			}
		} else {
			for i := 0; i < len(keys); i++ {
				go func(idx int) {
					f.Add([]byte(keys[idx]))
				}(i)
			}
		}*/

	for i := 0; i < len(keys); i++ {
		f.Add([]byte(keys[i]))
	}
}

type test_result struct {
	index  int
	exists bool
}

func BatchTest(f Filter, keys []string) ([]bool, int) {
	ret := make([]bool, len(keys))

	/*
		chs := make(chan test_result, len(keys))

		for i := 0; i < len(keys); i++ {
			go func(idx int, key []byte) {
				fmt.Println(idx, key)
				chs <- test_result{
					index:  idx,
					exists: f.Test(key),
				}
			}(i, []byte(keys[i]))
		}

		trues := 0

		for i := 0; i < len(keys); i++ {
			result := <-chs
			ret[result.index] = result.exists
			if ret[result.index] {
				trues += 1
			}
		}*/

	trues := 0
	for i := 0; i < len(keys); i++ {
		ret[i] = f.Test([]byte(keys[i]))
		if ret[i] {
			trues += 1
		}
	}

	return ret, trues
}

func NewFilterManager(persister FilterPersister, forceDumpSeconds int) (*FilterManager, error) {
	return &FilterManager{
		Filters:         make(map[string]Filter),
		persister:       persister,
		stop:            make(chan bool),
		forceDumpPeriod: time.Duration(forceDumpSeconds) * time.Second,
		persistChan:     make(chan bool, 1),
	}, nil
}

func isOptionsValid(options FilterOptions) error {
	//TODO add check if future
	return nil
}

func (m *FilterManager) AddNewBloomFilter(t string, options FilterOptions) (Filter, error) {
	var filter Filter
	var err error

	if err = isOptionsValid(options); err != nil {
		return nil, err
	}

	m.RLock()
	if _, ok := m.Filters[options.Name]; ok {
		return nil, fmt.Errorf("bloom filter exists")
	}
	m.RUnlock()

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

func (m *FilterManager) RecoverFilters() error {
	m.Lock()
	defer m.Unlock()

	filterNames, err := m.persister.ListFilterNames()
	if err != nil {
		return err
	}

	for _, filterName := range filterNames {
		reader, closer, err := m.persister.NewReader(filterName)
		defer closer.Close()
		if err != nil {
			log4go.Warn("open filter reader for %s error:%v", filterName, err)
			continue
		}

		filter, err := loadFilter(reader)
		if err != nil {
			log4go.Warn("load filter for %s error:%v", filterName, err)
			continue
		}

		m.Filters[filterName] = filter
	}

	return nil
}

func (m *FilterManager) Work() {
	ticker := time.NewTicker(m.forceDumpPeriod)
	should_stop := false

	for {
		force := false

		// only force dump or stop signal received, we will force all filters to dump
		if time.Now().Sub(m.lastForce) > m.forceDumpPeriod || should_stop {
			force = true
		}

		done := make(chan bool, len(m.Filters))

		for _, filter := range m.Filters {
			go func() {
				filter.PeriodMaintaince(m.persister, force)

				done <- true
			}()
		}

		for i := 0; i < len(m.Filters); i++ {
			<-done
		}

		if should_stop {
			break
		}

		select {
		case <-m.stop:
			log4go.Info("got stop signal, exits, dump again")
			should_stop = true
		case <-ticker.C:
		}
	}
}

func (m *FilterManager) Stop() {
	m.stop <- true
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

func loadFilter(reader io.Reader) (Filter, error) {
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
	log4go.Trace("loaded header %+v", dumpHeader)

	if dumpHeader.FilterUsedGzip {
		var err error
		reader, err = gzip.NewReader(reader)
		if err != nil {
			log4go.Warn("decompress error: %v", err)
			return nil, err
		}

		reader = bufio.NewReader(reader)
	}

	switch dumpHeader.FilterType {
	case FILTER_CLASSIC:
		f := &ClassicBloomFilter{}
		if err := f.Load(reader); err != nil {
			log4go.Warn("classic fiter load error:%v", err)
			return nil, err
		}

		return f, nil
	case FILTER_ROTATED:
		f := &RotatedBloomFilter{}
		if err := f.Load(reader); err != nil {
			return nil, err
		}

		return f, nil
	default:
		log4go.Warn("unknown filter type :%v", dumpHeader.FilterType)
		return nil, ILLEGAL_LOAD_FORMAT
	}
}

func dumpFilter(writer io.Writer, filter Filter) error {
	dumpHeader := DumpHeader{
		Magic:          MAGIC_NUM,
		FilterUsedGzip: UseGzip,
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

	if UseGzip {
		gwriter := gzip.NewWriter(writer)
		defer gwriter.Close()

		return filter.Dump(gwriter)
	} else {
		return filter.Dump(writer)
	}
}
