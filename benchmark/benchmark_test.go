package benchmark

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/AgilaNews/bfserver/bloom"
	pb "github.com/AgilaNews/bfserver/bloomiface"
	"github.com/AgilaNews/bfserver/service"
	"github.com/alecthomas/log4go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	BF_NAME = "abc"
)

type BenchmarkClient struct {
	conn *grpc.ClientConn
}

func getManagerFixture() *bloom.FilterManager {
	m, _ := bloom.NewFilterManager(nil, 6000)
	m.AddNewBloomFilter(bloom.FILTER_CLASSIC, bloom.FilterOptions{
		Name:      BF_NAME,
		N:         10000,
		ErrorRate: 0.05,
	})

	return m
}

func createClient(addr string) *BenchmarkClient {
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	return &BenchmarkClient{conn: conn}
}

func callTest(client *BenchmarkClient, keys []string) {
	bclient := pb.NewBloomFilterServiceClient(client.conn)
	bclient.Test(context.Background(), &pb.TestRequest{Name: BF_NAME, Keys: keys})
}

func runTest(b *testing.B, requestsCount int, keys []string) int64 {
	s, err := service.NewBloomFilterServer(":", getManagerFixture())
	if err != nil {
		b.Errorf("create server error : %v", err)
		return 0
	}
	var m sync.Mutex
	var wg sync.WaitGroup

	totalTime := int64(0)
	go s.Work()

	c := createClient(s.Listener.Addr().String())
	ch := make(chan int, requestsCount*4)

	wg.Add(requestsCount)
	for i := 0; i < requestsCount; i++ {
		go func() {
			for range ch {
				start := time.Now()
				callTest(c, keys)

				m.Lock()
				totalTime += time.Now().Sub(start).Nanoseconds()
				m.Unlock()
			}

			wg.Done()
		}()
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		ch <- i
	}
	b.StopTimer()
	close(ch)
	wg.Wait()

	return totalTime
}

func Benchmark5000LenKey100Client(b *testing.B) {
	log4go.Close()
	keylen := 500
	keys := make([]string, keylen)
	for i := 0; i < keylen; i++ {
		keys = append(keys, strconv.Itoa(i))
	}

	start := time.Now()
	total := runTest(b, 100, keys)
	end := time.Now().Sub(start).Nanoseconds()

	qps := int64(b.N) * 1e9 / end
	response_time := float64(total) / float64(b.N) / 1e9
	fmt.Printf("N:%d\ttotal time %v ns\tclient:100\tkeys len:%d\tqps:%v\tresp:%v\n", b.N, end, keylen, qps, response_time)
}
