package main

import (
	"context"
	"fmt"
	"github.com/general252/etcd_demo/my_api"
	"github.com/general252/etcd_demo/my_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/resolver"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const svcName = "project/test"

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)

	wg := &sync.WaitGroup{}
	s1 := server(wg)
	s2 := server(wg)
	s3 := server(wg)

	time.Sleep(time.Second * 3)
	go client()

	quitChan := make(chan os.Signal)
	signal.Notify(quitChan,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGHUP,
	)

	<-quitChan

	s1.Stop()
	s2.Stop()
	s3.Stop()

	wg.Wait()
}

func client() {
	var r = my_api.NewResolver("127.0.0.1:2379", svcName)
	resolver.Register(r)

	var js = fmt.Sprintf(`{ "loadBalancingConfig": [{"%v": {}}] }`, roundrobin.Name)
	conn, err := grpc.Dial(
		fmt.Sprintf("%v:///%v", r.Scheme(), svcName),
		grpc.WithDefaultServiceConfig(js),
		grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	client := my_pb.NewHelloServiceClient(conn)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		resp, err := client.Echo(ctx, &my_pb.Payload{Data: "hello"})
		cancel()
		if err != nil {
			log.Println(err)
		} else {
			log.Println(resp.Data)
		}

		<-time.After(time.Second)
	}
}

func server(wg *sync.WaitGroup) *grpc.Server {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	var port = lis.Addr().(*net.TCPAddr).Port

	// gRPC server
	s := grpc.NewServer()

	// 添加服务实现
	my_pb.RegisterHelloServiceServer(s, &serviceHello{port: port})

	var target = "localhost:2379;localhost:2379"
	var serviceInfo = &my_api.ServiceInfo{
		Name: svcName,
		Host: "127.0.0.1",
		Port: port,
	}

	// 将服务注册到ETCD中
	if err = my_api.Register(target, serviceInfo, 30); err != nil {
		log.Printf("register fail %v", err)
		return nil
	}

	wg.Add(1)
	go func() {
		// 删除
		defer func() {
			_ = my_api.UnRegister(target, serviceInfo)
			wg.Done()
		}()

		// 启动服务
		log.Printf("listener on %v", port)
		if err := s.Serve(lis); err != nil {
			log.Fatal(err)
		}
		log.Print("============================\n")
	}()

	return s
}

type serviceHello struct {
	port int
}

func (c *serviceHello) Echo(ctx context.Context, req *my_pb.Payload) (*my_pb.Payload, error) {
	req.Data = fmt.Sprintf("%v %v %v", req.Data, time.Now().Format(time.RFC3339), c.port)
	return req, nil
}
