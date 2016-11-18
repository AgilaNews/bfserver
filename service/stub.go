package service

import (
	"golang.org/x/net/context"

	"github.com/AgilaNews/bfserver/bloom"
	pb "github.com/AgilaNews/bfserver/iface"
	"github.com/alecthomas/log4go"
)

type BloomFilteService struct {
	Manager *bloom.BloomFilterManager
}

func NewBloomFilterService(manager *BloomFilterManager) (*BloomFilteService, error) {
	return nil, nil
}

func (b *BloomFilterService) Add(ctx context.Context, req *pb.AddRequest) (*pb.AddResponse, error) {
	resp := &pb.AddResponse{}

	filter, err := b.Manager.GetBloomFilter(req.BloomFilterName)
	if err != nil {
		log4go.Warn("get bloomfilter name [%s] error", req.BloomFilterName)
		return nil, err
	}

	bloom.BatchAdd(filter, req.Keys)
	return resp, nil
}

func (b *BloomFilterService) Test(ctx context.Context, req *pb.TestRequest) (*pb.TestResponse, error) {
	resp := &pb.AddResponse{}

	filter, err := b.Manager.GetBloomFilter(req.BloomFilterName)
	if err != nil {
		log4go.Warn("get bloomfilter name [%s] error", req.BloomFilterName)
		return nil, err
	}

	resp.Exists = bloom.BatchTest(filter, req.Keys)
	return resp, nil
}

func (b *BloomFilterService) Info(ctx context.Context, req *pb.EmptyMessage) (*pb.InfoRespone, error) {

}
