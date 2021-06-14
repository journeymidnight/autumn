package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/journeymidnight/autumn/manager/pmclient"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/node"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	_ "github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap/zapcore"
	_ "go.uber.org/zap/zapcore"
)

type Result struct {
	Key       string
	StartTime float64 //time.Now().Second
	Elapsed   float64
}

type BenchType string

const (
	READ_T  BenchType = "read"
	WRITE_T           = "write"
)

func benchmark(etcdAddrs []string, op BenchType, threadNum int, duration int, size int) error {

	pm := pmclient.NewAutumnPMClient(etcdAddrs)
	if err := pm.Connect(); err != nil {
		return err
	}
	client := NewAutumnLib(etcdAddrs)
	//defer client.Close()

	if err := client.Connect(); err != nil {
		return err
	}

	stopper := utils.NewStopper()

	//prepare data
	data := make([]byte, size)
	utils.SetRandStringBytes(data)

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

	go func() {
		for i := 0; i < threadNum; i++ {
			loop := 0 //sample to record lantency
			t := i
			stopper.RunWorker(func() {
				j := 0
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
							key := fmt.Sprintf("test%d_%d", t, j)
							ctx, cancel = context.WithCancel(context.Background())
							start := time.Now()
							err := client.Put(ctx, []byte(key), data)
							cancel()
							end := time.Now()
							j++
							if err != nil {
								fmt.Printf("%v\n", err)
								return
							}
							if loop%3 == 0 {
								lock.Lock()
								results = append(results, Result{
									Key:       key,
									StartTime: start.Sub(benchStartTime).Seconds(),
									Elapsed:   end.Sub(start).Seconds(),
								})
								lock.Unlock()
							}
							atomic.AddUint64(&totalSize, uint64(size))
							atomic.AddUint64(&count, 1)
							loop++
						}
						read := func(t int) {}
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
	case READ_T:
		fileName = "rresult.json"
	case WRITE_T:
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

func bootstrap(c *cli.Context) error {
	smAddrs := utils.SplitAndTrim(c.String("smAddr"), ",")

	smc := smclient.NewSMClient(smAddrs)
	if err := smc.Connect(); err != nil {
		return err
	}

	pmc := pmclient.NewAutumnPMClient(smAddrs)
	if err := pmc.Connect(); err != nil {
		return err
	}
	//choose the first one

	log, _, err := smc.CreateStream(context.Background(),3,0)
	if err != nil {
		return err
	}
	row, _, err := smc.CreateStream(context.Background(),3,0)
	if err != nil {
		return err
	}

	partID, err := pmc.Bootstrap(log.StreamID, row.StreamID)
	if err != nil {
		return err
	}
	fmt.Printf("bootstrap succeed, created new range partition %d on %d\n", partID)
	return nil
}

func del(c *cli.Context) error {
	etcdAddr := utils.SplitAndTrim(c.String("etcdAddr"), ",")
	client := NewAutumnLib(etcdAddr)
	//defer client.Close()
	if err := client.Connect(); err != nil {
		return err
	}
	key := c.Args().First()
	if len(key) == 0 {
		return errors.New("no key")
	}

	return client.Delete(context.Background(), []byte(key))

}

func get(c *cli.Context) error {
	etcdAddr := utils.SplitAndTrim(c.String("etcdAddr"), ",")
	client := NewAutumnLib(etcdAddr)
	//defer client.Close()

	if err := client.Connect(); err != nil {
		return err
	}
	key := c.Args().First()
	if len(key) == 0 {
		return errors.New("no key")
	}

	value, err := client.Get(context.Background(), []byte(key))
	if err != nil {
		return errors.Errorf(("get key:%s failed: reason:%s"), key, err)
	}
	//print the raw data to stdout, fmt.Println does not work
	binary.Write(os.Stdout, binary.LittleEndian, value)

	return nil
}

func autumnRange(c *cli.Context) error {
	etcdAddr := utils.SplitAndTrim(c.String("etcdAddr"), ",")
	if len(etcdAddr) == 0 {
		return errors.Errorf("etcdAddr is nil")
	}
	client := NewAutumnLib(etcdAddr)
	//defer client.Close()

	if err := client.Connect(); err != nil {
		return err
	}
	prefix := c.Args().First()
	/*
		if len(prefix) == 0 {
			return errors.New("no key")
		}
	*/
	out, err := client.Range(context.Background(), []byte(prefix), []byte(prefix))
	if err != nil {
		return err
	}
	for i := range out {
		fmt.Printf("%s\n", out[i])
	}
	return nil
}

//FIXME: grpc stream is better to send big values
func put(c *cli.Context) error {
	etcdAddr := utils.SplitAndTrim(c.String("etcdAddr"), ",")
	if len(etcdAddr) == 0 {
		return errors.Errorf("etcdAddr is nil")
	}
	client := NewAutumnLib(etcdAddr)
	//defer client.Close()

	if err := client.Connect(); err != nil {
		return err
	}
	key := c.Args().First()
	if len(key) == 0 {
		return errors.New("no key")
	}
	fileName := c.Args().Get(1)
	if len(fileName) == 0 {
		return errors.New("no fileName")
	}
	value, err := ioutil.ReadFile(fileName)
	if err != nil {
		return errors.Errorf("read file %s: err: %s", fileName, err.Error())
	}
	if err := client.Put(context.Background(), []byte(key), value); err != nil {
		return errors.Errorf(("put key:%s failed: reason:%s"), key, err)
	}
	fmt.Println("success")
	return nil
}

func info(c *cli.Context) error {
	smAddrs := utils.SplitAndTrim(c.String("smAddr"), ",")
	client := smclient.NewSMClient(smAddrs)
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

func main() {
	xlog.InitLog([]string{"client.log"}, zapcore.DebugLevel)
	app := cli.NewApp()
	app.Name = "autumn"
	app.Usage = "autumn subcommand"
	app.Commands = []*cli.Command{
		{
			Name:  "info",
			Usage: "info --smAddr <path>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "smAddr", Value: "127.0.0.1:3401"},
			},
			Action: info,
		},

		{
			Name:  "bootstrap",
			Usage: "bootstrap -smAddr <addrs>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "smAddr", Value: "127.0.0.1:3401"},
			},
			Action: bootstrap,
		},

		{
			Name:  "put",
			Usage: "put --etcdAddr <addrs> <KEY> <FILE>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdAddr", Value: "127.0.0.1:2379"},
			},
			Action: put,
		},
		{
			Name:  "get",
			Usage: "get --etcdAddr <addrs> <KEY>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdAddr", Value: "127.0.0.1:2379"},
			},
			Action: get,
		},
		{
			Name:  "del",
			Usage: "del --etcdAddr <addrs> <KEY>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdAddr", Value: "127.0.0.1:2379"},
			},
			Action: del,
		},
		{
			Name:  "wbench",
			Usage: "wbench --etcdAddr <addrs> --thread <num> --duration <duration>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdAddr", Value: "127.0.0.1:2379"},
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
		{
			Name:  "ls",
			Usage: "ls --etcdAddr <addrs> <prefix>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdAddr", Value: "127.0.0.1:2379"},
			},
			Action: autumnRange,
		},
		{
			Name: "format",
			Usage: "format --walDir <dir> --listenUrl <addr> --smAddr <addrs> <dir list> ",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdAddr", Value: "127.0.0.1:3401"},
				&cli.StringFlag{Name: "listenUrl"},
				&cli.StringFlag{Name: "walDir"},

			},
			Action :format,
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}

}

func format(c *cli.Context) error {
	//if any error happend, revert.
	revert := func(dirList []string) {
		for _, dir := range dirList {
			d, _ := os.Open(dir)
			defer d.Close()
			names, _ := d.Readdirnames(-1)
			for _, name := range names {
				if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
					fmt.Printf(err.Error())
				}
			}
		}
		return 
	}
	smAddr := utils.SplitAndTrim(c.String("smAddr"), ",")
	listenUrl := c.String("listenUrl")
	

	walDir := c.String("walDir")
	if len(walDir) > 0 {
		_, err := os.Stat(walDir)
		if err != nil {
			return err
		}
	}

	dirList := c.Args().Slice()

	if len(listenUrl) == 0 {
		return errors.New("listenUrl can not be empty")
	}
	if len(dirList) == 0 {
		return errors.New("dir List can not be empty")
	}

	sm := smclient.NewSMClient(smAddr)

	if err := sm.Connect(); err != nil {
		return err
	}

	for _, dir := range dirList {
		if err := node.FormatDisk(dir); err != nil {
			revert(dirList)
			return err
		}
	}


	//register a new NodeID

	fmt.Printf("format on disks : %+v", dirList)

	fmt.Printf("register node on stream manager ..\n")
	nodeID, err := sm.RegisterNode(context.Background(), listenUrl)
	if err != nil {
		revert(dirList)
		return err
	}

	fmt.Printf("node %d is registered\n", nodeID)
	for _, dir := range dirList {
		storeIDPath := path.Join(dir, "node_id")
		if err := ioutil.WriteFile(storeIDPath, []byte(fmt.Sprintf("%d", nodeID)), 0644); err != nil {
			revert(dirList)
			return err
		}
	}


	//generate config file for node
	var config node.Config
	config.Dirs  = dirList
	config.ID = nodeID
	config.ListenUrl = listenUrl
	config.WalDir = walDir
	
	f , err := os.Create(fmt.Sprintf("en_%d.toml", nodeID))
	if err != nil {
		return err
	}
	defer f.Close()
	if err = toml.NewEncoder(f).Encode(config); err != nil {
		return err
	}

	return nil
}

func wbench(c *cli.Context) error {
	threadNum := c.Int("thread")
	etcdAddr := c.String("etcdAddr")
	duration := c.Int("duration")
	size := c.Int("size")
	etcdAddrs := utils.SplitAndTrim(etcdAddr, ",")
	return benchmark(etcdAddrs, WRITE_T, threadNum, duration, size)
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
