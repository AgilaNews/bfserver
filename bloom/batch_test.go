package bloom

import (
	"fmt"
	"testing"
)

func TestBatch(t *testing.T) {
	keys := []string{"a", "b", "c"}

	filter, err := NewRotatedBloomFilter(FilterOptions{Name: "test", ErrorRate: 0.05, N: 100000, R: 7})
	if err != nil {
		t.Errorf("create rotated filter error: %v", err)
		return
	}

	BatchAdd(filter, keys, true)
	keys = append(keys, "", "q")
	ret := BatchTest(filter, keys)
	fmt.Println(keys, ret)
	if len(ret) != len(keys) {
		t.Errorf("return length error")
	} else {
		for i := 0; i < 3; i++ {
			if ret[i] == false {
				t.Errorf("false positive")
			}
		}

		for i := 3; i < len(keys); i++ {
			if ret[i] == true {
				t.Errorf("true negative")
			}
		}
	}
}

func BenchmarkBatch(b *testing.B) {
	b.StopTimer()

	d := GetDictionarys()
	keys := make([]string, len(d))
	for i, v := range d {
		keys[i] = string(v)
	}

	filter, err := NewRotatedBloomFilter(FilterOptions{Name: "test", ErrorRate: 0.05, N: 100000, R: 7})
	if err != nil {
		b.Errorf("create rotated filter error: %v", err)
		return
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BatchAdd(filter, keys, true)
	}
}