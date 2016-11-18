package service

import (
	"fmt"
	"net"

	"github.com/AgilaNews/bfserver/g"
	pb "github.com/AgilaNews/bfserver/iface"
	"github.com/alecthomas/log4go"
	"google.golang.org/grpc"
)

type BloomFilterServer struct {
	listener  net.Listener
	rpcServer *grpc.Server
}

func NewBloomFilterServer(model classify.Classification) (*BloomFilterServer, error) {
	var err error

	c := &BloomFilterServer{}

	if c.listener, err = net.Listen("tcp", g.Config.Rpc.BF.Addr); err != nil {
		return nil, fmt.Errorf("bind rpc %s server error", g.Config.Rpc.BF.Addr)
	}

	log4go.Info("listened on rpc server :%s success", g.Config.Rpc.BF.Addr)

	c.rpcServer = grpc.NewServer()

	service, _ := NewBloomFilterService(model)
	log4go.Info("registering rpc service")
	pb.RegisterClassificationServiceServer(c.rpcServer, service)

	return c, nil
}

func (c *BloomFilterServer) Work() {
	c.rpcServer.Serve(c.listener)
}

func (c *BloomFilterServer) Stop() {
	c.rpcServer.GracefulStop()
}
