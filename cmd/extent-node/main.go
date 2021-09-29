package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/journeymidnight/autumn/node"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"

	"go.uber.org/zap"
)

func main() {
	config, err := node.NewConfig()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("config: %+v\n", config)
	xlog.InitLog([]string{fmt.Sprintf("node_%d.log", config.ID)}, zap.DebugLevel)

	node := node.NewExtentNode(config.ID, config.Dirs, config.WalDir, config.ListenURL, config.SmURLs, config.EtcdURLs)

	//open all extent files

	err = node.LoadExtents()
	utils.Check(err)

	//start grpc service
	err = node.ServeGRPC()
	utils.Check(err)

	fmt.Println("node is ready")
	xlog.Logger.Infof("node is ready!")

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGUSR1)
	for {
		select {
		case <-sc:
			node.Shutdown()
			return
		}
	}

}
