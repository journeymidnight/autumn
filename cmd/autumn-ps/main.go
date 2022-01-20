/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

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
	var maxExtentMBString string //in the unit of MB
	var skiplistMBString string  //in the unit of MB
	var traceSampler float64
	var compression string
	var assertKeys bool

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
				Name:        "nosync",
				Destination: &noSync,
				Value:       false,
			},
			&cli.StringFlag{
				Name:        "max-extent-size",
				Destination: &maxExtentMBString,
				Required:    true,
				Usage:       "max extent size in MB",
			},
			&cli.StringFlag{
				Name:        "skiplist-size",
				Destination: &skiplistMBString,
				Required:    true,
				Usage:       "skiplist size in MB",
			},
			&cli.Float64Flag{
				Name:        "trace-sampler",
				Destination: &traceSampler,
			},
			&cli.StringFlag{
				Name:        "compression",
				Destination: &compression,
				Usage:       "compression type, none, snappy, zstd",
				Value:       "snappy",
			},
			&cli.BoolFlag{
				Name:        "assert-keys",
				Destination: &assertKeys,
				Value:       false,
				Usage:       "assert keys in all table(debug only)",
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

	maxExtentMB, err := strconv.Atoi(maxExtentMBString)
	if err != nil {
		panic(fmt.Sprint("max-extent-size is not a integer"))
	}
	if maxExtentMB <= 0 || maxExtentMB > 3072 { //3GB is the hard limit
		panic(fmt.Sprint("max-extent-size must be greater than zero"))
	}

	skiplistSizeMB, err := strconv.Atoi(skiplistMBString)
	if err != nil {
		panic(fmt.Sprint("skiplist-sizeis not a integer"))
	}
	if skiplistSizeMB <= 0 || skiplistSizeMB > 120 { //120MB is the hard limit
		panic(fmt.Sprint("skiplist-size must be greater than zero and less than 120MB"))
	}

	if compression != "snappy" && compression != "none" && compression != "zstd" {
		panic("compression type must be snappy, none or zstd")
	}

	config := partition_server.Config{
		PSID:                 id,
		AdvertiseURL:         advertiseListen,
		ListenURL:            listen,
		SmURLs:               utils.SplitAndTrim(smURLs, ","),
		EtcdURLs:             utils.SplitAndTrim(etcdURLs, ","),
		MustSync:             !noSync,
		CronTimeGC:           "0 0 * * 1",
		CronTimeMajorCompact: "0 3 * * 2",
		MaxExtentSize:        uint32((maxExtentMB << 20)),
		MaxMetaExtentSize:    (4 << 20),
		SkipListSize:         uint32((skiplistSizeMB << 20)),
		TraceSampler:         traceSampler,
		Compression:          compression,
		AssertKeys:           assertKeys,
	}

	ps := partition_server.NewPartitionServer(config)

	ps.Init()

	utils.Check(ps.ServeGRPC(config.TraceSampler))

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
