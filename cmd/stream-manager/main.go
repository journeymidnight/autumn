package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/manager/streammanager"
	"github.com/journeymidnight/autumn/xlog"
	"go.uber.org/zap/zapcore"
)

func main() {
	config := manager.NewConfig()

	xlog.InitLog([]string{"sm.log"}, zapcore.DebugLevel)

	etcd, client, err := manager.ServeETCD(config)
	if err != nil {
		panic(err.Error())
	}
	sm := streammanager.NewStreamManager(etcd, client, config)
	go sm.LeaderLoop()

	err = sm.ServeGRPC()
	if err != nil {
		xlog.Logger.Fatalf(err.Error())
	}

	xlog.Logger.Infof("node is ready!")
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
