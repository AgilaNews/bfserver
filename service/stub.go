package service

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/AgilaNews/bfserver/bloom"
	pb "github.com/AgilaNews/bfserver/iface"
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
	resp := &pb.EmptyMessage{}

	filter, err := b.Manager.GetBloomFilter(req.Name)
	if err != nil {
		log4go.Warn("get bloomfilter name [%s] error", req.Name)
		return nil, err
	}

	bloom.BatchAdd(filter, req.Keys, false)
	return resp, nil
}

func (b *BloomFilterService) Test(ctx context.Context, req *pb.TestRequest) (*pb.TestResponse, error) {
	resp := &pb.TestResponse{}

	filter, err := b.Manager.GetBloomFilter(req.Name)
	if err != nil {
		log4go.Warn("get bloomfilter name [%s] error", req.Name)
		return nil, err
	}

	resp.Exists = bloom.BatchTest(filter, req.Keys)
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

	if _, err := b.Manager.CreateNewBloomFilter(t, options); err != nil {
		log4go.Warn("create filter of %v error: %v", req, err)
		return nil, fmt.Errorf("create filter error: %v", err)
	} else {
		log4go.Info("add filter %v success", req)
		return resp, nil
	}
}
