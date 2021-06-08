package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/manager/stream_manager"
	"google.golang.org/grpc"

	"github.com/journeymidnight/autumn/xlog"
	"go.uber.org/zap/zapcore"
)

func main() {
	config := manager.NewConfig()

	xlog.InitLog([]string{"manager.log"}, zapcore.InfoLevel)

	cfg, err := config.GetEmbedConfig()
	if err != nil {
		xlog.Logger.Fatal(err)
	}

	etcd, client, err := etcd_utils.ServeETCD(cfg)
	if err != nil {
		panic(err.Error())
	}

	sm := stream_manager.NewStreamManager(etcd, client, config)
	go sm.LeaderLoop()

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(8<<20),
		grpc.MaxSendMsgSize(8<<20),
		grpc.MaxConcurrentStreams(1000),
	)

	sm.RegisterGRPC(grpcServer)

	/*
		if err = pm.ServeGRPC(grpc); err != nil {
			xlog.Logger.Fatalf(err.Error())
		}
	*/

	/*
		if err = sm.ServeGRPC(); err != nil {
			xlog.Logger.Fatalf(err.Error())
		}
	*/
	listener, err := net.Listen("tcp", config.GrpcUrl)
	if err != nil {
		xlog.Logger.Fatalf(err.Error())
	}
	go func() {
		grpcServer.Serve(listener)
	}()

	xlog.Logger.Infof("manager is ready!")
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGUSR1)
	for {
		select {
		case err := <-etcd.Err():
			xlog.Logger.Fatal(err)
		case <-sc:
			sm.Close()
			etcd.Close()
			return
		}
	}

}
