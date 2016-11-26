package service

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/AgilaNews/bfserver/bloom"
	pb "github.com/AgilaNews/bfserver/bloomiface"
	"github.com/alecthomas/log4go"
)

type BloomFilterService struct {
	Manager *bloom.FilterManager
}

func NewBloomFilterService(manager *bloom.FilterManager) (*BloomFilterService, error) {
	return &BloomFilterService{
		Manager: manager,
	}, nil
}

func (b *BloomFilterService) Add(ctx context.Context, req *pb.AddRequest) (*pb.EmptyMessage, error) {
	t := StartTimer()

	resp := &pb.EmptyMessage{}

	if len(req.Name) == 0 {
		return nil, fmt.Errorf("empty request name")
	}
	if len(req.Keys) == 0 {
		return nil, fmt.Errorf("keys count can't be zero")
	}
	filter, err := b.Manager.GetBloomFilter(req.Name)
	if err != nil {
		log4go.Warn("get bloomfilter name [%s] error", req.Name)
		return nil, err
	}

	bloom.BatchAdd(filter, req.Keys, !req.Async)

	log4go.Trace("keys: %+v", req.Keys)
	log4go.Info("%s add %d keys,  duration:%v", req.Name, len(req.Keys), t.Stop())
	return resp, nil
}

func (b *BloomFilterService) Test(ctx context.Context, req *pb.TestRequest) (*pb.TestResponse, error) {
	resp := &pb.TestResponse{}
	t := StartTimer()

	if len(req.Name) == 0 {
		return nil, fmt.Errorf("empty request name")
	}
	if len(req.Keys) == 0 {
		return nil, fmt.Errorf("keys count can't be zero")
	}

	filter, err := b.Manager.GetBloomFilter(req.Name)
	if err != nil {
		log4go.Warn("get bloomfilter name [%s] error", req.Name)
		return nil, err
	}

	exists := 0
	resp.Exists, exists = bloom.BatchTest(filter, req.Keys)
	log4go.Trace("keys: %+v", req.Keys)
	log4go.Info("%s, test %d, left:%d duration:%v", req.Name, len(req.Keys), len(req.Keys)-exists, t.Stop())
	return resp, nil
}

func (b *BloomFilterService) Info(ctx context.Context, req *pb.EmptyMessage) (*pb.InfoResponse, error) {
	return nil, nil
}

func (b *BloomFilterService) Create(ctx context.Context, req *pb.NewBloomFilterRequest) (*pb.EmptyMessage, error) {
	resp := &pb.EmptyMessage{}

	options := bloom.FilterOptions{}
	t := ""

	switch req.Type {
	case pb.NewBloomFilterRequest_CLASSIC:
		t = bloom.FILTER_CLASSIC
	case pb.NewBloomFilterRequest_ROTATED:
		t = bloom.FILTER_ROTATED
	default:
		return nil, fmt.Errorf("unknown filter type :%v", req.Type)
	}

	if len(req.Name) == 0 {
		return nil, fmt.Errorf("don't allow null filter name")
	}
	options.Name = req.Name
	if req.N < 1 {
		return nil, fmt.Errorf("empty N")
	}
	options.N = uint(req.N)
	if req.ErrorRate < 0 || req.ErrorRate > 0.1 {
		return nil, fmt.Errorf("only permit error_rate between (0,0.1)")
	}
	options.ErrorRate = req.ErrorRate

	if t == bloom.FILTER_ROTATED {
		if req.R < 2 || req.R > 30 {
			return nil, fmt.Errorf("rotated filter r must between [2,30]")
		}
		options.R = uint(req.R)

		if req.Interval < 1 || req.Interval > 144 {
			return nil, fmt.Errorf("rotated filter interval must between [1,144]")
		}

		options.RotateInterval = time.Hour * time.Duration(req.Interval)
	}

	if _, err := b.Manager.AddNewBloomFilter(t, options); err != nil {
		log4go.Warn("create filter of %v error: %v", req, err)
		return nil, fmt.Errorf("create filter error: %v", err)
	} else {
		log4go.Info("add filter %v success ", options.Name)
		return resp, nil
	}
}
