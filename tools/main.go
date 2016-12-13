package main

import (
	"flag"
	"fmt"
	"github.com/AgilaNews/bfserver/bloom"
	pb "github.com/AgilaNews/bfserver/bloomiface"
	jsonpb "github.com/golang/protobuf/jsonpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"os"
	"strings"
    "bufio"
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
	case "check":
		f, err := os.Open(ctx)
		if err == nil {
			if err := bloom.CheckFilter(bufio.NewReader(f)); err != nil {
				fmt.Println("filter format error :%v", err)
			} else {
				fmt.Println("file format check ok")
			}
		} else {
			fmt.Println("open file error")
		}
	case "dump":
		req := &pb.DumpRequest{}
		if err := jsonpb.Unmarshal(strings.NewReader(ctx), req); err != nil {
			panic(fmt.Sprintf("get context error:%v", err))
		}
		_, err := client.Dump(context.Background(), req)

		if err != nil {
			panic(fmt.Sprintf("error: %v", err))
		}
	case "reload":
		req := &pb.ReloadRequest{}
		if err := jsonpb.Unmarshal(strings.NewReader(ctx), req); err != nil {
			panic(fmt.Sprintf("get context error:%v", err))
		}
		_, err := client.Reload(context.Background(), req)

		if err != nil {
			panic(fmt.Sprintf("error: %v", err))
		}
	}

}
