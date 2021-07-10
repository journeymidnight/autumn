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

	//FIXME: sm address
	node := node.NewExtentNode(config.ID, config.Dirs, config.WalDir, config.ListenUrl, []string{"127.0.0.1:3401"}, []string{"127.0.0.1:2379"})

	//open all extent files
	err = node.LoadExtents()
	utils.Check(err)


	//start grpc service
	err = node.ServeGRPC()
	utils.Check(err)

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
