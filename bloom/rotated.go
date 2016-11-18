package bloom

/*
 *  @Date    : 2016-10-18 10:50:30
 *  @Author  : Zhao Yulong (elysium.zyl@gmail.com)
 *  @Link    : ${link}
 *  @Describe: bloomfilter with routing
 */

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/log4go"
)

type RotatedBloomFilter struct {
	sync.RWMutex

	name string
	r    uint // R is how many rounds keeped

	current  int
	dumpPath string

	rotateInterval time.Time
	innerFilters   []Filter
}

func NewRotatedBloomFilter(options BloomFilterOptions) (Filter, error) {
	innerFilters := make([]Filter, options.R)
	for i := 0; i < int(options.R); i++ {
		if f, err := NewClassicBloomFilter(options); err != nil {
			return nil, err
		} else {
			innerFilters[i] = f
		}
	}

	return &RotatedBloomFilter{
		name:           options.Name,
		r:              options.R,
		rotateInterval: options.RotateInterval,

		dumpPath:     options.DumpPath,
		current:      0,
		innerFilters: innerFilters,
	}, nil
}

func (b *RotatedBloomFilter) Name() string {
	return b.name
}

func (b *RotatedBloomFilter) Capacity() uint {
	return b.innerFilters[b.current].Capacity()
}

func (b *RotatedBloomFilter) Count() uint {
	return b.innerFilters[b.current].Count()
}

func (b *RotatedBloomFilter) EstimatedFillRatio() float64 {
	return b.innerFilters[b.current].EstimatedFillRatio()
}

func (b *RotatedBloomFilter) FillRatio() float64 {
	return b.innerFilters[b.current].FillRatio()
}

func (b *RotatedBloomFilter) K() uint {
	return b.innerFilters[b.current].K()
}

func (b *RotatedBloomFilter) Reset() {
	for _, filter := range b.innerFilters {
		filter.Reset()
	}
}

func (b *RotatedBloomFilter) SetHash(h hash.Hash64) {
	for _, filter := range b.innerFilters {
		filter.SetHash(h)
	}
}

func (b *RotatedBloomFilter) Add(key []byte) Filter {
	chs := make([]chan bool, b.r)

	for i := 0; i < int(b.r); i++ {
		go func(ch chan bool) {
			b.innerFilters[i].Add(key)

			ch <- true
		}(chs[i])
	}

	for _, ch := range chs {
		<-ch
	}

	return b
}

func (b *RotatedBloomFilter) Test(key []byte) bool {
	return b.innerFilters[b.current].Test(key)
}

func (b *RotatedBloomFilter) DropOneRep() {
	b.innerFilters[b.current].Reset()

	b.current = (b.current + 1) % int(b.r)
}

func (b *RotatedBloomFilter) Destroy() {
	dumpPath := path.Join(b.dumpPath, b.name)

	if exists(dumpPath) {
		err := os.RemoveAll(dumpPath)
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	for i := 0; i < int(b.r); i++ {
		b.innerFilters[i] = nil
	}
}

func (b *RotatedBloomFilter) DumpTo(io.Writer) (int64, error) {
	return 0, nil
}

func (b *RotatedBloomFilter) LoadTo(io.Reader) (int64, error) {
	return 0, nil
}

func (b *RotatedBloomFilter) Dump() error {
	dumpPath := path.Join(b.dumpPath, b.name)
	if !exists(dumpPath) {
		err := os.MkdirAll(dumpPath, os.ModePerm)
		if err != nil {
			return err
		}
	}

	err := b.dumpMeta()
	if err != nil {
		return err
	}

	if fileInfo, _ := os.Stat(dumpPath); fileInfo.IsDir() {
		chs := make([]chan bool, b.r)

		for i := 0; i < int(b.r); i++ {
			go func(ch chan bool) {
				dumpFile := path.Join(b.dumpPath, b.name, fmt.Sprintf("%d.dump", i))
				if exists(dumpFile) {
					if exists(dumpFile + ".old") {
						os.Remove(dumpFile + ".old")
					}
					os.Rename(dumpFile, dumpFile+".old")
				}

				zipbuf := new(bytes.Buffer)
				w := zlib.NewWriter(zipbuf)

				buf := new(bytes.Buffer)
				b.innerFilters[i].DumpTo(buf)
				_, err := w.Write(buf.Bytes())
				w.Close()

				if err != nil {
					return
				}

				ioutil.WriteFile(dumpFile, zipbuf.Bytes(), os.ModePerm)
				ch <- true
			}(chs[i])
		}

		for _, ch := range chs {
			<-ch
		}
	}

	return nil
}

func (b *RotatedBloomFilter) dumpMeta() error {
	dumpFile := path.Join(b.dumpPath, b.name, "meta.dump")
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.BigEndian, int32(b.r))
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.BigEndian, int32(b.current))
	if err != nil {
		return err
	}

	return ioutil.WriteFile(dumpFile, buf.Bytes(), os.ModePerm)
}

func (b *RotatedBloomFilter) Load() error {
	if !exists(b.dumpPath) {
		fmt.Println("File path doesn't exist" + "filepath")
		return nil
	}

	file, _ := os.Stat(b.dumpPath)
	if !file.IsDir() {
		return nil
	}

	dir, err := ioutil.ReadDir(b.dumpPath)
	if err != nil {
		return err
	}

	err = b.loadMeta(path.Join(b.dumpPath, "meta.dump"))
	if err != nil {
		return err
	}
	var dumpfiles []string
	for _, fi := range dir {
		if fi.IsDir() {
			continue
		}

		if strings.HasSuffix(strings.ToLower(fi.Name()), ".dump") {
			indexStr := strings.SplitN(fi.Name(), ".", 2)[0]
			if index, err := strconv.Atoi(indexStr); err == nil {
				if index < 0 {
					log4go.Warn("index error")
					continue
				}

				dumpfiles = append(dumpfiles, path.Join(b.dumpPath, strconv.Itoa(index)+".dump"))
			}
		}
	}

	if len(dumpfiles) != int(b.r) {
		return fmt.Errorf("Different replication numbers")
	}

	chs := make([]chan error, len(dumpfiles))

	for i, dumpfile := range dumpfiles {
		go func(ch chan error) {
			if !exists(dumpfile) {
				ch <- fmt.Errorf("non-exist file name: %s", dumpfile)
			}

			file, _ := os.Open(dumpfile)
			defer file.Close()
			r, _ := zlib.NewReader(bufio.NewReader(file))
			defer r.Close()

			buf := new(bytes.Buffer)
			io.Copy(buf, r)
			_, err := b.innerFilters[i].LoadTo(buf)

			ch <- err
		}(chs[i])
	}

	for _, ch := range chs {
		e := <-ch
		if e != nil {
			return e
		}
	}

	return nil
}

func (b *RotatedBloomFilter) loadMeta(filename string) error {
	var r, index int32

	if !exists(filename) {
		return errors.New("File doesn't exist: " + filename)
	}

	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	tmp := bytes.NewReader(buf)

	err = binary.Read(tmp, binary.BigEndian, &r)
	if err != nil {
		return err
	}

	err = binary.Read(tmp, binary.BigEndian, &index)
	if err != nil {
		return err
	}

	b.r = uint(r)
	b.current = int(index)
	return nil
}

func exists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}
