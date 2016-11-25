package service

import (
	"fmt"
	"net"

	"github.com/AgilaNews/bfserver/bloom"
	pb "github.com/AgilaNews/bfserver/bloomiface"
	"github.com/alecthomas/log4go"
	"google.golang.org/grpc"
)

type BloomFilterServer struct {
	Listener  net.Listener
	rpcServer *grpc.Server
}

func NewBloomFilterServer(addr string, manager *bloom.FilterManager) (*BloomFilterServer, error) {
	var err error

	c := &BloomFilterServer{}
	if c.Listener, err = net.Listen("tcp", addr); err != nil {
		return nil, fmt.Errorf("bind rpc %s server error: %v", addr, err)
	}

	log4go.Info("listened on rpc server :%s success", addr)

	c.rpcServer = grpc.NewServer()

	service, _ := NewBloomFilterService(manager)
	log4go.Info("registering rpc service")
	pb.RegisterBloomFilterServiceServer(c.rpcServer, service)

	return c, nil
}

func (c *BloomFilterServer) Work() {
	c.rpcServer.Serve(c.Listener)
}

func (c *BloomFilterServer) Stop() {
	c.rpcServer.GracefulStop()
}
