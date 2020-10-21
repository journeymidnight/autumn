package main

import (
	"context"
	"fmt"
	"os"

	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap/zapcore"
)

func info(c *cli.Context) error {
	cluster := c.String("cluster")
	client := smclient.NewSMClient([]string{cluster})
	if err := client.Connect(); err != nil {
		return err
	}
	streams, extents, err := client.StreamInfo(context.Background(), nil)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", streams)
	fmt.Printf("%v\n", extents)
	return nil
}

func alloc(c *cli.Context) error {
	cluster := c.String("cluster")
	client := smclient.NewSMClient([]string{cluster})
	if err := client.Connect(); err != nil {
		return err
	}
	s, e, err := client.CreateStream(context.Background())
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", s)
	fmt.Printf("%v\n", e)
	return nil
}

type StreamOp struct {
	streamInfo *pb.StreamInfo
	ExtentInfo *pb.ExtentInfo
}

func main() {
	xlog.InitLog([]string{"client.log"}, zapcore.DebugLevel)
	app := cli.NewApp()
	app.Name = "stream"
	app.Usage = "stream subcommand"
	app.Commands = []*cli.Command{
		{
			Name:  "alloc",
			Usage: "alloc --cluster <path>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3401"},
			},
			Action: alloc,
		},
		{
			Name:  "info",
			Usage: "info --cluster <path>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3401"},
			},
			Action: info,
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}

}
