package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/journeymidnight/autumn/partition_server"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {

	var listen string
	var psID string
	var smAddr string
	var etcdAddr string

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
				Name:        "psID",
				Usage:       "psID",
				Destination: &psID,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "smAddr",
				Destination: &smAddr,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "etcdAddr",
				Destination: &etcdAddr,
				Required:    true,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err.Error())
	}

	smAddrs := utils.SplitAndTrim(smAddr, ",")
	etcdAddrs := utils.SplitAndTrim(etcdAddr, ",")
	xlog.InitLog([]string{fmt.Sprintf("ps.log")}, zap.DebugLevel)
	id, err := strconv.ParseUint(psID, 10, 64)
	if err != nil || id == 0 {
		panic(fmt.Sprint("psid can not be zero"))
	}
	ps := partition_server.NewPartitionServer(smAddrs, etcdAddrs, id, listen)

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
