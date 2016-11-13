package bloom

/*
 *  @Date    : 2016-10-18 10:50:30
 *  @Author  : Zhao Yulong (elysium.zyl@gmail.com)
 *  @Link    : ${link}
 *  @Describe: bloomfilter with routing
 */

import (
    "fmt"
    "os"
    "strconv"
    "compress/zlib"
    "bufio"
    "io"
    "bytes"
    "path"
    "io/ioutil"
    "strings"
    "errors"
    "encoding/binary"
)

type RotatedBloomFilter struct {
    r           int         // Number of repliates
    n           uint        // Number of items
    fpRate      float64     // false-positive rate
    blName      string      // The name of this bl
    index       int       // which replicate to drop
    reps        []*BloomFilter
    chs         []chan int
}

var defaultDumpPath string = "./BLDumpPath/"
var max_days int = 100

func NewRotatedBloomFilter(r int, n uint, name string, fpRate float64) *RotatedBloomFilter {
    if r > max_days {
        fmt.Println("[replicats limit]:" + strconv.Itoa(max_days))
        return nil
    }
    rep := make([]*BloomFilter, r)
    ch := make([]chan int, r) 
    for i := 0; i < r; i++ {
        rep[i] = NewBloomFilter(n, fpRate)
        ch[i] = make(chan int)
    }

    return &RotatedBloomFilter{
            r:      r,
            n:      n,
            fpRate: fpRate, 
            blName: name, 
            index:  0, 
            reps:   rep,
            chs:     ch,
    }
}


func (b *RotatedBloomFilter) BatchAdd(data []string) {
    for _, str := range(data) {
        b.Add(str)
    }
}


func (b *RotatedBloomFilter) Add(data string) {
    //fmt.Println(data)
    for i := 0; i < b.r; i++ {
        go b._add([]byte(data), i)
    }

    for _, ch := range(b.chs) {
        <-ch
    }
}


func (b *RotatedBloomFilter) _add(data []byte, i int) {
    b.reps[i].Add(data)
    b.chs[i] <- i
}


func (b *RotatedBloomFilter) DropOneRep() {
    b.reps[b.index].Reset()
    b.index = (b.index + 1) % b.r
}


func (b *RotatedBloomFilter) BatchTest(data []string) []bool {
    ret := make([]bool, len(data))
    for i, str := range(data) {
        ret[i] = b.Test(str)
    }
    return ret
}


func (b *RotatedBloomFilter) Test(data string) bool {
    return b.reps[b.index].Test([]byte(data))
}


func (b *RotatedBloomFilter) Info() {
    fmt.Println(
        "Number of items added: " + strconv.FormatUint(uint64(b.reps[b.index].Count()),
        10))
    fmt.Println(
        "Number of hash func: " + strconv.FormatUint(uint64(b.reps[b.index].K()),
        10))
    fmt.Println(
        "Filter size:" + strconv.FormatUint(uint64(b.reps[b.index].Capacity()),
        10))
}


func Exists(filename string) bool {
    _, err := os.Stat(filename)
    return err == nil || os.IsExist(err)
}


func (b *RotatedBloomFilter) Destroy() {
    dumpPath := path.Join(defaultDumpPath, b.blName)
    if Exists(dumpPath) {
        err := os.RemoveAll(dumpPath)
        if err != nil {
            fmt.Println(err.Error())
        }
    }

    for i := 0; i < b.r; i++ {
        b.reps[i] = nil
    }
}


func (b *RotatedBloomFilter) SetName(name string) {
    b.blName = name
}


func (b *RotatedBloomFilter) Dump() error {
    dumpPath := path.Join(defaultDumpPath, b.blName)
    if !Exists(dumpPath) {
        err := os.MkdirAll(dumpPath, os.ModePerm)
        if err != nil {
            return err
        }
    }

    err := b.dumpMeta()
    if err != nil {
        fmt.Println(err.Error())
    }

    if fileInfo, _ := os.Stat(dumpPath); fileInfo.IsDir() {
        for i := 0; i < b.r; i++ {
            go b.dumpOne(i)
        }

        for _, ch := range(b.chs) {
            <-ch
        }
    }

    return nil
}


func (b *RotatedBloomFilter) dumpMeta() error {
    dumpFile := path.Join(defaultDumpPath, b.blName, "meta.dump")
    buf := new(bytes.Buffer)

    err := binary.Write(buf, binary.BigEndian, int32(b.r))
    if err != nil {
        return err
    }

    err = binary.Write(buf, binary.BigEndian, uint64(b.n))
    if err != nil {
        return err
    }

    err = binary.Write(buf, binary.BigEndian, b.fpRate)
    if err != nil {
        return err
    }

    err = binary.Write(buf, binary.BigEndian, int32(b.index))
    if err != nil {
        return err
    }

    return ioutil.WriteFile(dumpFile, buf.Bytes(), os.ModePerm)
}


func (b *RotatedBloomFilter) dumpOne(index int) {
    dumpFile := path.Join(defaultDumpPath, b.blName, strconv.Itoa(index) + ".dump")
    if Exists(dumpFile) {
        if Exists(dumpFile + ".old") {
            os.Remove(dumpFile + ".old")
        }
        os.Rename(dumpFile, dumpFile + ".old")
    } 

    zipbuf := new(bytes.Buffer)
    w := zlib.NewWriter(zipbuf)

    buf := new(bytes.Buffer)
    b.reps[index].WriteTo(buf)
    _, err := w.Write(buf.Bytes())
    w.Close()

    if err != nil {
        return
    }

    ioutil.WriteFile(dumpFile, zipbuf.Bytes(), os.ModePerm)
    b.chs[index] <- index
}


func (b *RotatedBloomFilter) Load(filepath string) error {
    if !Exists(filepath) {
        fmt.Println("File path doesn't exist" + "filepath")
        return nil
    }

    file, _ := os.Stat(filepath)
    if !file.IsDir() {
        return nil
    }

    dir, err := ioutil.ReadDir(filepath)
    if err != nil {
        return err
    }

    err = b.loadMeta(path.Join(filepath, "meta.dump"))
    if err != nil {
        fmt.Println(err.Error())
        return err
    }
    var dumpfiles []string
    for _, fi := range dir {
        if fi.IsDir() {
            continue
        }

        if strings.HasSuffix(strings.ToLower(fi.Name()), ".dump") {
            // index.xx.dump is ok
            indexStr := strings.SplitN(fi.Name(), ".", 2)[0]
            if index, err := strconv.Atoi(indexStr); err == nil {
                if index < 0 {
                    fmt.Println("index error")
                    continue
                }

                dumpfiles = append(dumpfiles, path.Join(filepath, strconv.Itoa(index) + ".dump"))
            }
        }
    }

    if len(dumpfiles) != b.r {
        fmt.Println("Different replication numbers")
        return errors.New("Different replication numbers")
    }

    for i, dumpfile := range(dumpfiles) {
        go b.loadOne(dumpfile, i)
    }

    for _, ch := range(b.chs) {
        <-ch
    }

    return nil
}


func (b *RotatedBloomFilter) loadMeta(filename string) error {
    var r, index int32
    var n uint64
    var fpRate float64

    if !Exists(filename) {
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

    err = binary.Read(tmp, binary.BigEndian, &n) 
    if err != nil {
        return err
    }

    err = binary.Read(tmp, binary.BigEndian, &fpRate)
    if err != nil {
        return err
    }

    err = binary.Read(tmp, binary.BigEndian, &index)
    if err != nil {
        return err
    }
    b.r = int(r)
    b.n = uint(n)
    b.fpRate = fpRate
    b.index = int(index)
    return nil
}


func (b *RotatedBloomFilter) loadOne(filename string, index int) error {
    if !Exists(filename){
        return errors.New("File doesn't exist: " + filename)
    }

    file, _ := os.Open(filename)
    defer file.Close()
    r, _ := zlib.NewReader(bufio.NewReader(file))
    defer r.Close()

    buf := new(bytes.Buffer)
    io.Copy(buf, r)
    _, err := b.reps[index].ReadFrom(buf)

    b.chs[index] <- index
    return err
}
