package bloom

/*
 *  @Date    : 2016-11-13 19:36:30
 *  @Author  : Zhao Yulong (elysium.zyl@gmail.com)
 *  @Link    : ${link}
 *  @Describe: rotated bloomfilter manager
 */
import (
    "errors"
)

type RotatedBFManager struct {
    m           map[string]*RotatedBloomFilter
}

func NewRotatedBFManager () *RotatedBFManager{
    m := make(map[string]*RotatedBloomFilter)
    return &RotatedBFManager {
        m:  m,
    }
}

func (r *RotatedBFManager) Create(name string, rep int, n uint, fpRate float64) error {
    _, ok := r.m[name]
    if ok {
        return errors.New(name + " exists already")
    }

    r.m[name] = NewRotatedBloomFilter(rep, n, name, fpRate)
    return nil
}

func (r *RotatedBFManager) Add(name string, keys []string) (int, error) {
    bf, ok := r.m[name]
    if !ok {
        return 0, errors.New(name + " does not exit")
    }

    bf.BatchAdd(keys)
    return len(keys), nil
}

func (r *RotatedBFManager) Filter(name string, keys []string) ([]bool, error) {
    var ret []bool
    bf, ok := r.m[name]
    if !ok {
        return ret, errors.New(name + " does not exit")
    }

    ret = bf.BatchTest(keys)
    return ret, nil
}

func (r *RotatedBFManager) Destroy(name string) error {
    bf, ok := r.m[name]
    if !ok {
        return errors.New(name + " does not exit")
    }

    bf.Destroy()
    return nil
}

func (r *RotatedBFManager) Dump(name string) error {
    bf, ok := r.m[name]
    if !ok {
        return errors.New(name + " does not exit")
    }

    return bf.Dump()
}

