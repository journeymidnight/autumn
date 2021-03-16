package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/journeymidnight/autumn/partitionserver"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {

	var listen string
	var dir string
	var smAddr string
	var pmAddr string

	app := &cli.App{
		HelpName: "",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "listen",
				Usage:       "ps grpc listen url",
				Destination: &listen,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "dir",
				Usage:       "dir",
				Destination: &dir,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "smAddr",
				Destination: &smAddr,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "pmAddr",
				Destination: &pmAddr,
				Required:    true,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err.Error())
	}

	smAddrs := utils.SplitAndTrim(smAddr, ",")
	pmAddrs := utils.SplitAndTrim(pmAddr, ",")
	xlog.InitLog([]string{fmt.Sprintf("ps.log")}, zap.DebugLevel)

	//FIXME: sm address
	//
	ps := partitionserver.NewPartitionServer(smAddrs, pmAddrs, dir, "127.0.0.1:9951")

	ps.Init()

	utils.Check(ps.ServeGRPC())

	xlog.Logger.Infof("PS is ready!")

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGUSR1)

	for {
		select {
		case <-sc:
			ps.Shutdown()
			return
		}
	}

}
