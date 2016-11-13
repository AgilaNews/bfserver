package bloom

/*
 *  @Date    : 2016-11-13 19:36:30
 *  @Author  : Zhao Yulong (elysium.zyl@gmail.com)
 *  @Link    : ${link}
 *  @Describe: rotated bloomfilter manager
 */

import (
    "fmt"
    "os"
    "error"
)

type RotatedBFManager struct {
    m           map[string]*RotatedBloomFilter
}

func NewRotatedBFManager () {
    m := make(map[string]*RotatedBloomFilter)
    return &RotatedBFManager {
        m:  m,
    }
}

func (r *RotatedBFManager) Create(name string, r int, n uint, fpRate float64) error {
    _, ok := m[name]
    if ok {
        return error.NewError(name + " exists already")
    }

    m[name] = NewRotatedBloomFilter(r, n, name, fpRate)
    return nil
}

func (r *RotatedBFManager) Add(name string, keys []string) (uint32, error) {
    bf, ok := m[name]
    if !ok {
        return 0, error.NewError(name + " does not exit")
    }

    bf.BatchAdd(keys)
    return len(keys), nil
}

func (r *RotatedBFManager) Filter(name string, keys []string) (unint32, error) {
    bf, ok := m[name]
    if !ok {
        return 0, error.NewError(name + " does not exit")
    }

    bf.BatchTest(keys)
    return len(keys), nil
}

func (r *RotatedBFManager) Destroy(name string) error {
    bf.Destroy()
    return nil
}

func (r *RotatedBFManager) Dump() error {
    bf, ok := m[name]
    if !ok {
        return error.NewError(name + " does not exit")
    }

    return bf.Dump()
}

