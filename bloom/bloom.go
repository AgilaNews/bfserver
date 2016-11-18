package bloom

import (
	"encoding/binary"
	"hash"
	"io"
	"math"
)

const (
	DEFAULT_FILL_RATIO = 0.5
)

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
	Load() error
	Dump() error

	LoadTo(reader io.Reader) (int64, error)
	DumpTo(writer io.Writer) (int64, error)
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
