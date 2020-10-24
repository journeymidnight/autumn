package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/journeymidnight/autumn/node"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {

	var listen string
	var dir string
	var ID uint64
	app := &cli.App{
		HelpName: "",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "listen",
				Usage:       "grpc listen url",
				Destination: &listen,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "dir",
				Usage:       "dir",
				Destination: &dir,
				Required:    true,
			},
			&cli.Uint64Flag{
				Name:        "ID",
				Destination: &ID,
				Required:    true,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err.Error())
	}

	xlog.InitLog([]string{fmt.Sprintf("node_%d.log", ID)}, zap.DebugLevel)

	//FIXME: sm address
	node := node.NewExtentNode(dir, listen, []string{"127.0.0.1:3401"})

	//open all extent files
	err := node.LoadExtents()
	utils.Check(err)

	//open 'node_id' file, if 'node_id' doesn't exist. register to sm
	node.RegisterNode()

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
