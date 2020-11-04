package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap/zapcore"
)

type Result struct {
	ExtentID  uint64
	Offset    uint32
	StartTime float64 //time.Now().Second
	Elapsed   float64
}

type BenchType string

const (
	benchRead  BenchType = "read"
	benchWrite           = "write"
)

func benchmark(smAddr []string, op BenchType, threadNum int, duration int, size int) error {

	sm := smclient.NewSMClient(smAddr)
	if err := sm.Connect(); err != nil {
		return err
	}
	stopper := utils.NewStopper()
	var scs []*streamclient.StreamClient
	for i := 0; i < threadNum; i++ {
		s, _, err := sm.CreateStream(context.Background())
		if err != nil {
			return err
		}
		sc := streamclient.NewStreamClient(sm, s.StreamID, 32)
		if err = sc.Connect(); err != nil {
			return err
		}
		scs = append(scs, sc)
	}

	//prepare data
	data := make([]byte, size)
	utils.SetRandStringBytes(data)
	blocks := []*pb.Block{
		{
			CheckSum:    utils.AdlerCheckSum(data),
			Data:        data,
			BlockLength: uint32(size),
		},
	}

	var lock sync.Mutex
	var results []Result
	benchStartTime := time.Now()
	var count uint64
	var totalSize uint64

	done := make(chan struct{})

	start := time.Now()
	livePrint := func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		fmt.Print("\033[s") // save the cursor position

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				//https://stackoverflow.com/questions/56103775/how-to-print-formatted-string-to-the-same-line-in-stdout-with-go
				//how to print in one line
				fmt.Print("\033[u\033[K")
				ops := atomic.LoadUint64(&count) / uint64(time.Now().Sub(start).Seconds())
				throughput := float64(atomic.LoadUint64(&totalSize)) / time.Now().Sub(start).Seconds()
				fmt.Printf("ops:%d/s  throughput:%s", ops, utils.HumanReadableThroughput(throughput))
			}
		}
	}

	go func() {
		for i := 0; i < threadNum; i++ {
			loop := 0 //sample to record lantency
			t := i
			stopper.RunWorker(func() {
				var ctx context.Context
				var cancel context.CancelFunc
				for {
					select {
					case <-stopper.ShouldStop():
						if cancel != nil {
							cancel()
						}
						return
					default:
						write := func(t int) {
							ctx, cancel = context.WithCancel(context.Background())
							if err := scs[t].Append(ctx, blocks, time.Now()); err != nil {
								fmt.Println(err)
								return
							}
							cancel()

							end := time.Now()
							ios := scs[t].TryComplete()

							for _, io := range ios {
								if io.Err != nil {
									fmt.Println(io.Err)
									continue
								}
								if loop%10 == 0 {
									lock.Lock()
									start := io.UserData.(time.Time)
									results = append(results, Result{
										ExtentID:  io.ExtentID,
										Offset:    io.Offsets[0], //only one block per op
										StartTime: start.Sub(benchStartTime).Seconds(),
										Elapsed:   end.Sub(start).Seconds(),
									})
									lock.Unlock()
								}
								atomic.AddUint64(&totalSize, uint64(size))
								atomic.AddUint64(&count, 1)
								loop++
							}

						}

						read := func(num int) {
							//
						}
						switch op {
						case "read":
							read(t)
						case "write":
							write(t)
						default:
							fmt.Println("bench type is wrong")
							return
						}
					}
				}

			})
		}
		stopper.Wait()
		close(done)
	}()

	go livePrint()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1)
	select {
	case <-signalCh:
		stopper.Stop()
	case <-time.After(time.Duration(duration) * time.Second):
		stopper.Stop()
	case <-done:
		break
	}
	//write down result

	sort.Slice(results, func(i, j int) bool {
		return results[i].StartTime < results[i].StartTime
	})

	var fileName string
	switch op {
	case benchRead:
		fileName = "rresult.json"
	case benchWrite:
		fileName = "result.json"
	default:
		return errors.Errorf("benchtype error")
	}
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	defer f.Close()
	if err == nil {
		out, err := json.Marshal(results)
		if err == nil {
			f.Write(out)
		} else {
			fmt.Println("failed to write result.json")
		}
	}
	printSummary(time.Now().Sub(start), atomic.LoadUint64(&count), atomic.LoadUint64(&totalSize), threadNum, size)

	return nil
}

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

	nodes, err := client.NodesInfo(context.Background())
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", streams)
	fmt.Printf("%v\n", extents)
	fmt.Printf("%v\n", nodes)
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
		{
			Name:  "wbench",
			Usage: "wbench --cluster <path> --thread <num> --duration <duration>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3401, 127.0.0.1:3402, 127.0.0.1:3403", Aliases: []string{"c"}},
				&cli.IntFlag{Name: "thread", Value: 4, Aliases: []string{"t"}},
				&cli.IntFlag{Name: "duration", Value: 10, Aliases: []string{"d"}},
				&cli.IntFlag{Name: "size", Value: 8192, Aliases: []string{"s"}},
			},
			Action: wbench,
		},
		{
			Name:   "plot",
			Usage:  "plot <file.json>",
			Action: plot,
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}

}

func wbench(c *cli.Context) error {
	threadNum := c.Int("thread")
	cluster := c.String("cluster")
	duration := c.Int("duration")
	size := c.Int("size")
	addrs := utils.SplitAndTrim(cluster, ",")
	return benchmark(addrs, benchWrite, threadNum, duration, size)
}

func printSummary(elapsed time.Duration, totalCount uint64, totalSize uint64, threadNum int, size int) {
	if elapsed.Seconds() < 1e-9 {
		return
	}
	fmt.Printf("\nSummary\n")
	fmt.Printf("Threads :%d\n", threadNum)
	fmt.Printf("Size    :%d\n", size)
	fmt.Printf("Time taken for tests :%v seconds\n", elapsed.Seconds())
	fmt.Printf("Complete requests :%d\n", totalCount)
	fmt.Printf("Total transferred :%d bytes\n", totalSize)
	fmt.Printf("Requests per second :%.2f [#/sec]\n", float64(totalCount)/elapsed.Seconds())
	t := float64(totalSize) / elapsed.Seconds()
	fmt.Printf("Thoughput per sencond :%s\n", utils.HumanReadableThroughput(t))
}
