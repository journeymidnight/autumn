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

	var advertiseListen string
	var psID string
	var smURLs string
	var etcdURLs string
	var listen string
	var noSync bool

	app := &cli.App{
		HelpName: "",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "advertise-listen",
				Usage:       "ps grpc advertise listen url, tell cluste the connection",
				Destination: &advertiseListen,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "listen",
				Usage:       "ps grpc listen url",
				Destination: &listen,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "psid",
				Usage:       "psID",
				Destination: &psID,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "sm-urls",
				Destination: &smURLs,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "etcd-urls",
				Destination: &etcdURLs,
				Required:    true,
			},
			&cli.BoolFlag{
				Name:      "nosync",
				Destination: &noSync,
				Value:    false,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err.Error())
	}

	xlog.InitLog([]string{fmt.Sprintf("ps.log")}, zap.DebugLevel)
	id, err := strconv.ParseUint(psID, 10, 64)
	if err != nil || id == 0 {
		panic(fmt.Sprint("psid can not be zero"))
	}

	fmt.Printf("smURL is %v\n", utils.SplitAndTrim(smURLs, ","))
	fmt.Printf("etcdURL is %v\n", utils.SplitAndTrim(etcdURLs, ","))

	config := partition_server.Config {
		PSID: id,
		AdvertiseURL: advertiseListen,
		ListenURL: listen,
		SmURLs: utils.SplitAndTrim(smURLs, ","),
		EtcdURLs: utils.SplitAndTrim(etcdURLs, ","),
		MustSync: !noSync,
		CronTimeGC: "0 0 * * 1",
		CronTimeMajorCompact: "0 3 * * 2",
	}

	ps := partition_server.NewPartitionServer(config)

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
