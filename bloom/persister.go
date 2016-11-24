package bloom

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/alecthomas/log4go"
)

type FilterPersister interface {
	ListFilterNames() ([]string, error)
	NewWriter(filterName string) (Writer, error)
	NewReader(filterName string) (*bufio.Reader, error)
}

type Writer interface {
	Write([]byte) (int, error)
	Close() error
}

type fileWriter struct {
	f *os.File

	basePath string
	baseName string
	fullpath string
}

type LocalFileFilterPersister struct {
	basePath string
	useGzip  bool
}

func (fw *fileWriter) Write(b []byte) (int, error) {
	bytes, err := fw.f.Write(b)
	return bytes, err
}

func (fw *fileWriter) Close() error {
	fw.f.Close()

	linkName := filepath.Join(fw.basePath, fw.baseName)
	return os.Symlink(fw.fullpath, linkName)
}

func NewLocalFileFilterPersister(path string) (FilterPersister, error) {
	fs, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("open path error")
	}
	if !fs.IsDir() {
		return nil, fmt.Errorf("path is not dir error")
	}

	return &LocalFileFilterPersister{basePath: path}, nil
}

func (p *LocalFileFilterPersister) ListFilterNames() ([]string, error) {
	ret := make([]string, 0)

	files, err := ioutil.ReadDir(p.basePath)
	if err != nil {
		log4go.Warn("read dir of %s error:%v", p.basePath, err)
		return nil, err
	}

	for _, file := range files {
		if 0 != (file.Mode() & os.ModeSymlink) {
			ret = append(ret, file.Name())
		}
	}

	log4go.Info("get current filter list: %v", ret)

	return ret, nil
}

func (p *LocalFileFilterPersister) NewWriter(name string) (Writer, error) {
	fullpath := filepath.Join(p.basePath, name+"."+strconv.FormatInt(time.Now().Unix(), 10))
	if f, err := os.OpenFile(fullpath, os.O_WRONLY|os.O_CREATE, os.ModePerm); err != nil {
		log4go.Info("get writer from %s error :%v", fullpath, err)
		return nil, err
	} else {
		w := &fileWriter{
			f:        f,
			basePath: p.basePath,
			baseName: name,
			fullpath: fullpath,
		}
		return w, nil
	}
}

func (p *LocalFileFilterPersister) NewReader(name string) (*bufio.Reader, error) {
	fullpath := filepath.Join(p.basePath, name)

	if f, err := os.OpenFile(fullpath, os.O_RDONLY, os.ModePerm); err != nil {
		log4go.Warn("get reader from %s error :%v", fullpath, err)
		return nil, err
	} else {
		log4go.Trace("got reader of %s from %s", name, fullpath)

		return bufio.NewReader(f), nil
	}
}
