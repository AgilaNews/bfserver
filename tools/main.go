package main

import (
	"flag"
	"fmt"
	pb "github.com/AgilaNews/bfserver/iface"
	jsonpb "github.com/golang/protobuf/jsonpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"strings"
)

func main() {
	var addr, cmd, ctx string

	flag.StringVar(&addr, "addr", ":6066", "rpc server address")
	flag.StringVar(&cmd, "cmd", "", "sub command")
	flag.StringVar(&ctx, "ctx", "", "sub command")

	//for create
	flag.Parse()

	if cmd == "" {
		panic("please set cmd")
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(fmt.Sprintf("dial error :%v", err))
	}
	defer conn.Close()
	client := pb.NewBloomFilterServiceClient(conn)

	switch cmd {
	case "create":
		req := &pb.NewBloomFilterRequest{}

		if err := jsonpb.Unmarshal(strings.NewReader(ctx), req); err != nil {
			panic(fmt.Sprintf("get context error:%v", err))
		}
		_, err := client.Create(context.Background(), req)

		if err != nil {
			panic(fmt.Sprintf("error: %v", err))
		}
		fmt.Println("create bloomfilter success")
	case "add":
		req := &pb.AddRequest{}

		if err := jsonpb.Unmarshal(strings.NewReader(ctx), req); err != nil {
			panic(fmt.Sprintf("get context error:%v", err))
		}
		_, err := client.Add(context.Background(), req)

		if err != nil {
			panic(fmt.Sprintf("error: %v", err))
		}

	case "test":
		req := &pb.TestRequest{}

		if err := jsonpb.Unmarshal(strings.NewReader(ctx), req); err != nil {
			panic(fmt.Sprintf("get context error:%v", err))
		}
		resp, err := client.Test(context.Background(), req)

		if err != nil {
			panic(fmt.Sprintf("error: %v", err))
		}

		for i := 0; i < len(req.Keys); i++ {
			fmt.Println("test %s: %v", req.Keys[i], resp.Exists[i])
		}
	}

}
