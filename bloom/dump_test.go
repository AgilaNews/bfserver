package bloom

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/log4go"
)

var _ = fmt.Errorf

func initLog() {
	log4go.Global.AddFilter("stdout", log4go.DEBUG, log4go.NewConsoleLogWriter())
}

func TestDumpHeader(t *testing.T) {
	c, err := NewClassicBloomFilter(FilterOptions{Name: "test", ErrorRate: 0.05, N: 1})
	if err != nil {
		t.Errorf("new filter error: %v", err)
		return
	}

	buffer := new(bytes.Buffer)
	if err := dumpFilter(buffer, c); err != nil {
		t.Errorf("dump filter error: %v", err)
	} else {
		f, err := loadFilter(buffer)
		if err != nil {
			t.Errorf("load filter error: %v", err)
			return
		}

		loaded_filter := f.(*ClassicBloomFilter)
		if !classicBloomFilterEqual(loaded_filter, c.(*ClassicBloomFilter)) {
			t.Errorf("load filter type error")
		}

	}
}

type TestPersister struct {
	buffer TestBuffer
}

type TestBuffer struct {
	bytes.Buffer
}

func (b *TestBuffer) Close() error {
	return nil
}

func (t *TestPersister) NewWriter(filterName string) (Writer, error) {
	return &t.buffer, nil
}
func (t *TestPersister) NewReader(filterName string) (Reader, error) {
	return &t.buffer, nil
}

func TestRotated(t *testing.T) {
	c, err := NewRotatedBloomFilter(FilterOptions{
		Name:           "test",
		ErrorRate:      0.05,
		N:              10000,
		R:              7,
		RotateInterval: time.Duration(1 * time.Second),
	})
	if err != nil {
		t.Errorf("create rotated filter error:%v", err)
		return
	}

	f := c.(*RotatedBloomFilter)
	before := f.lastRotated

	if err := f.PeriodMaintaince(&TestPersister{}); err != nil {
		t.Errorf("period maintance err:%v", err)
		return
	}
	if f.lastRotated != before {
		t.Errorf("should not rotated")
		return
	}

	time.Sleep(1 * time.Second)
	if err := f.PeriodMaintaince(&TestPersister{}); err != nil {
		t.Errorf("period maitaince err: %v", err)
		return
	}

	if f.lastRotated == before {
		t.Errorf("should rotated")
		return
	}
	if f.current != 1 {
		t.Errorf("drop one error")
	}

	before = f.lastRotated
	//maintaince again, should not rotated
	if err := f.PeriodMaintaince(&TestPersister{}); err != nil {
		t.Errorf("period maitaince err: %v", err)
		return
	}

	if f.lastRotated != before {
		t.Errorf("should not rotated")
		return
	}
}

func TestLoad(t *testing.T) {
	c, err := NewRotatedBloomFilter(FilterOptions{
		Name:           "test",
		ErrorRate:      0.05,
		N:              10000,
		R:              7,
		RotateInterval: time.Duration(1 * time.Second),
	})
	if err != nil {
		t.Errorf("create rotated filter error:%v", err)
		return
	}

	buf := new(bytes.Buffer)

	if err := c.Dump(buf); err != nil {
		t.Errorf("dump filter error")
		return
	}

	other := &RotatedBloomFilter{}
	err = other.Load(buf)
	if err != nil {
		t.Errorf("load filter error")
		return
	}

	if !rotatedBloomfilterEquals(c.(*RotatedBloomFilter), other) {
		t.Errorf("load error")
		return
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	initLog()
	defer log4go.Close()
	os.Exit(m.Run())
}
