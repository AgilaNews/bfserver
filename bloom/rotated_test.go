package bloom

import (
	"math/rand"

	"bufio"
	"os"
	"testing"
	"time"
)

func rotatedBloomfilterEquals(a, b *RotatedBloomFilter) bool {
	if a.name != b.name || a.r != b.r || a.current != b.current || a.rotateInterval != b.rotateInterval || a.lastRotated != b.lastRotated {
		return false
	}

	for i := 0; i < int(b.r); i++ {
		if !classicBloomFilterEqual(a.innerFilters[i].(*ClassicBloomFilter), b.innerFilters[i].(*ClassicBloomFilter)) {
			return false
		}
	}

	return true
}

func TestRotatedBloomAdd(t *testing.T) {
	filter, err := NewRotatedBloomFilter(FilterOptions{Name: "test", ErrorRate: 0.05, N: 100000, R: 7})
	if err != nil {
		t.Errorf("error: %v", err)
		return
	}

	filter.Add([]byte("a"))
}

func TestRotatedBloomTest(t *testing.T) {
	filter, err := NewRotatedBloomFilter(FilterOptions{Name: "test", ErrorRate: 0.05, N: 100000, R: 7})
	if err != nil {
		t.Errorf("error: %v", err)
		return
	}

	filter.Add([]byte("a"))

	if !filter.Test([]byte("a")) {
		t.Errorf("true negative error")
	}

	if filter.Test([]byte("b")) {
		t.Errorf("false positive error")
	}

}

func TestRotatedFilterDumpLoad(t *testing.T) {
	c, err := NewRotatedBloomFilter(FilterOptions{
		Name:           "test",
		ErrorRate:      0.05,
		N:              10000,
		R:              7,
		RotateInterval: time.Duration(1 * time.Second),
		persister:      &TestPersister{},
	})
	if err != nil {
		t.Errorf("create rotated filter error:%v", err)
		return
	}

	f := c.(*RotatedBloomFilter)

	f.Add([]byte("a"))

	for i := 0; i < int(f.r); i++ {
		if exists := f.innerFilters[i].Test([]byte("a")); !exists {
			t.Errorf("test of %d filter false positive", i)
		}
	}

	f.dropOneRep()
	if f.current != 1 {
		t.Errorf("drop error")
		return
	}

	if exists := f.innerFilters[0].Test([]byte("a")); exists {
		t.Errorf("reset error")
		return
	}

	for i := 1; i < int(f.r); i++ {
		if exists := f.innerFilters[i].Test([]byte("a")); !exists {
			t.Errorf("test of %d filter false positive", i)
		}
	}
}

func BenchmarkRotatedBloomAdd(b *testing.B) {
	b.StopTimer()
	words := GetDictionarys()
	filter, err := NewRotatedBloomFilter(FilterOptions{Name: "test", ErrorRate: 0.05, N: 100000, R: 7})
	if err != nil {
		b.Errorf("create rotated filter error: %v", err)
		return
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		word := words[int(rand.Int31())%len(words)]

		filter.Add(word)
	}
}

func BenchmarkRotatedBloomTest(b *testing.B) {
	b.StopTimer()
	words := GetDictionarys()
	filter, err := NewRotatedBloomFilter(FilterOptions{Name: "test", ErrorRate: 0.05, N: 100000, R: 7})
	if err != nil {
		b.Errorf("create rotated filter error: %v", err)
		return
	}

	for i := 0; i < b.N; i++ {
		word := words[int(rand.Int31())%len(words)]

		filter.Add(word)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		word := words[int(rand.Int31())%len(words)]

		filter.Test(word)
	}
}

func GetDictionarys() [][]byte {
	f, _ := os.Open("/usr/share/dict/cracklib-small")
	ret := make([][]byte, 0)
	reader := bufio.NewReader(f)

	for {
		if line, _, err := reader.ReadLine(); err != nil {
			break
		} else {
			ret = append(ret, line)
		}
	}

	return ret
}
