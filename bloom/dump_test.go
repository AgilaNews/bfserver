package bloom

import (
	"bytes"
	"fmt"
	"testing"
)

var _ = fmt.Errorf

func TestDumpHeader(t *testing.T) {
	c, err := NewClassicBloomFilter(FilterOptions{Name: "test", ErrorRate: 0.05, N: 1})
	if err != nil {
		t.Errorf("new filter error: %v", err)
		return
	}

	buffer := new(bytes.Buffer)
	if err := DumpFilter(buffer, c); err != nil {
		t.Errorf("dump filter error: %v", err)
	} else {
		f, err := LoadFilter(buffer)
		if err != nil {
			t.Errorf("load filter error: %v", err)
			return
		}

		loaded_filter := f.(*ClassicBloomFilter)
		if loaded_filter.name != "test" || loaded_filter.k != OptimalK(0.05) || loaded_filter.m != OptimalM(1, 0.05) {
			t.Errorf("load filter type error")
		}

	}
}
