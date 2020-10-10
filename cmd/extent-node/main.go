package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/manager/sm"
	"github.com/journeymidnight/autumn/xlog"
	"go.uber.org/zap"
)

func main() {
	config := manager.NewConfig()

	xlog.InitLog([]string{fmt.Sprintf("sm_%s.log", config.Name)}, zap.DebugLevel)

	etcd, client, err := manager.ServeETCD(config)
	if err != nil {
		xlog.Logger.Fatal(err.Error())

	}

	sm := sm.NewStreamManager(etcd, client, config)

	sm.ServeGRPC()

	xlog.Logger.Infof("node is ready!")
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGUSR1)
	for {
		select {
		case <-sc:
			sm.Close()
			etcd.Close()
			return
		}
	}

}
