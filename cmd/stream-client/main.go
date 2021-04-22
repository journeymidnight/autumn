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

type writeRequest struct {
	blocks []*pb.Block
	wg     sync.WaitGroup
	err    error
}

func threadedOp(threadNum int, stopper *utils.Stopper, done chan struct{}, op BenchType, sc *streamclient.AutumnStreamClient, blocks []*pb.Block,
	callback func(s time.Time, loop int, err error)) {

	for i := 0; i < threadNum; i++ {
		stopper.RunWorker(func() {
			for {
				loop := 0 //sample to record lantency
				select {
				case <-stopper.ShouldStop():
					return
				default:
					write := func() {
						start := time.Now()
						//fmt.Printf("good %s")
						_, _, _, err := sc.Append(context.Background(), blocks)

						//fmt.Println("not bad")

						callback(start, loop, err)
						loop++
					}

					read := func() {}
					switch op {
					case "read":
						read()
					case "write":
						write()
					default:
						fmt.Println("bench type is wrong")
						return
					}

				}

			}
		})

	}

	stopper.Wait()
}

var kCapacity = 64

//batch op
func batchOp(stopper *utils.Stopper, done chan struct{}, op BenchType,
	sc *streamclient.AutumnStreamClient, blocks []*pb.Block, callback func(s time.Time, loop int, err error)) {
	//single thread, no batch

	writeCh := make(chan *writeRequest, kCapacity)

	go doWrites(writeCh, done, sc)

	stopper.RunWorker(func() {
		loop := 0 //sample to record lantency
		for {
			select {
			case <-stopper.ShouldStop():
				return
			default:
				switch op {
				case "read":
					break
				case "write":
					start := time.Now()
					r := &writeRequest{
						blocks: blocks,
					}

					r.wg.Add(1)
					writeCh <- r

					go func() {
						r.wg.Wait()
						callback(start, loop, r.err)
					}()
					loop++
				default:
					fmt.Println("bench type is wrong")
					return
				}
			}

		}

	})
}

func doWrites(writeCh chan *writeRequest, done chan struct{}, sc *streamclient.AutumnStreamClient) {
	pendingCh := make(chan struct{}, 1)
	writeRequests := func(reqs []*writeRequest) {
		var blocks []*pb.Block
		for _, req := range reqs {
			blocks = append(blocks, req.blocks...)
		}
		_, _, _, err := sc.Append(context.Background(), blocks)
		for _, req := range reqs {
			req.err = err
			req.wg.Done()
		}
		<-pendingCh
	}

	reqs := make([]*writeRequest, 0, kCapacity)
	for {
		var r *writeRequest

		select {
		case r = <-writeCh:
		case <-done:
			return
		}

		for {
			reqs = append(reqs, r)

			if len(reqs) > kCapacity {
				pendingCh <- struct{}{}
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-done:
				return
			}

		}
	writeCase:
		go writeRequests(reqs)
		reqs = make([]*writeRequest, 0, kCapacity)
	}
}

func benchmark(smAddr []string, op BenchType, duration int, size int, threadNum int) error {

	sm := smclient.NewSMClient(smAddr)
	if err := sm.Connect(); err != nil {
		return err
	}
	stopper := utils.NewStopper()
	s, _, err := sm.CreateStream(context.Background())
	if err != nil {
		return err
	}

	em := streamclient.NewAutomnExtentManager(sm)

	sc := streamclient.NewStreamClient(sm, em, s.StreamID)
	if err = sc.Connect(); err != nil {
		return err
	}

	//prepare data
	data := make([]byte, size)
	utils.SetRandStringBytes(data)
	blocks := []*pb.Block{
		{
			Data: data,
		},
	}

	var lock sync.Mutex //protect results
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

	writeDone := func(start time.Time, loop int, err error) {
		if err != nil {
			panic(fmt.Sprintf(err.Error()))
		}
		atomic.AddUint64(&totalSize, uint64(size))
		atomic.AddUint64(&count, 1)
		if loop%3 == 0 {
			lock.Lock()
			results = append(results, Result{
				StartTime: start.Sub(benchStartTime).Seconds(),
				Elapsed:   time.Now().Sub(start).Seconds(),
			})
			lock.Unlock()
		}
	}

	if threadNum == 1 {
		go batchOp(stopper, done, op, sc, blocks, writeDone)
	} else {
		go threadedOp(threadNum, stopper, done, op, sc, blocks, writeDone)
	}

	go livePrint()

	go func() {
		stopper.Wait()
		close(done)
	}()

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
				&cli.IntFlag{Name: "thread", Value: 1, Aliases: []string{"t"}},
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
	cluster := c.String("cluster")
	duration := c.Int("duration")
	size := c.Int("size")
	addrs := utils.SplitAndTrim(cluster, ",")
	threadNum := c.Int("thread")
	return benchmark(addrs, benchWrite, duration, size, threadNum)
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
