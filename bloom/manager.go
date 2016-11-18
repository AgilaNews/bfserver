package bloom

/*
 *  @Date    : 2016-11-13 19:36:30
 *  @Author  : Zhao Yulong (elysium.zyl@gmail.com)
 *  @Link    : ${link}
 *  @Describe: rotated bloomfilter manager
 */
import (
	"fmt"
	"sync"
	"time"
)

const (
	FILTER_CLASSIC = "classic"
	FILTER_ROTATED = "rotated"
)

type BloomFilterOptions struct {
	Name     string
	DumpPath string

	N         uint
	ErrorRate float64

	R              uint
	RotateInterval time.Time
}

type BloomFilterManager struct {
	sync.RWMutex

	Filters  map[string]Filter
	TotalMem uint64
}

func NewBloomFilterManager() (*BloomFilterManager, error) {
	return &BloomFilterManager{
		Filters: make(map[string]Filter),
	}, nil
}

func isOptionsValid(options BloomFilterOptions) error {
	return nil
}

func (m *BloomFilterManager) CreateNewBloomFilter(t string, options BloomFilterOptions) (Filter, error) {
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

func (m *BloomFilterManager) GetBloomFilter(t string) (Filter, error) {
	m.RLock()
	defer m.RUnlock()

	f, ok := m.Filters[t]
	if !ok {
		return f, fmt.Errorf("filter non exists")
	}

	return f, nil
}

func (m BloomFilterManager) Persist() {

}

func (m *BloomFilterManager) Recovery() error {
	return nil
}
