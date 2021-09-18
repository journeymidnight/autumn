package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/journeymidnight/autumn/autumn_clientv1"
	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/manager/stream_manager"
	"github.com/journeymidnight/autumn/node"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	_ "github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	clientv3 "go.etcd.io/etcd/client/v3"
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

func benchmark(etcdUrlss []string, op BenchType, threadNum int, duration int, size int) error {

	client := autumn_clientv1.NewAutumnLib(etcdUrlss)
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

	//in the unit of millisecond
	hist := utils.NewLantencyStatus(0, 1000)

	n := rand.Int31()
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
							key := fmt.Sprintf("test%d_%d_%d", n, t, j)
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
							hist.Record(int64(time.Since(start).Milliseconds()))
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
	printSummary(time.Now().Sub(start), atomic.LoadUint64(&count), atomic.LoadUint64(&totalSize), threadNum, size, hist)

	return nil
}

func bootstrap(c *cli.Context) error {

	etcdUrls := utils.SplitAndTrim(c.String("etcdUrls"), ",")
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdUrls,
		DialTimeout: time.Second,
	})
	if err != nil {
		return err
	}

	smUrlss := utils.SplitAndTrim(c.String("smUrls"), ",")

	smc := smclient.NewSMClient(smUrlss)
	if err := smc.Connect(); err != nil {
		return err
	}

	//valid no PART exists...
	kv, _, _ := etcd_utils.EtcdRange(etcdClient, "PART/")
	if len(kv) > 0 {
		return errors.New("partition already exists")
	}

	r, s, err := utils.ParseReplicationString(c.String("replication"))
	if err != nil {
		return err
	}
	//choose the first one
	log, _, err := smc.CreateStream(context.Background(), uint32(r), uint32(s))
	if err != nil {
		fmt.Printf("can not create log stream\n")
		return err
	}
	fmt.Printf("log stream %d created, replication is [%d+%d]\n", log.StreamID, r, s)

	row, _, err := smc.CreateStream(context.Background(), uint32(r), uint32(s))
	if err != nil {
		fmt.Printf("can not create row stream\n")
		return err
	}
	fmt.Printf("row stream %d created, replication is [%d+%d]\n", row.StreamID, r, s)

	partID, _, err := etcd_utils.EtcdAllocUniqID(etcdClient, stream_manager.IdKey, 1)
	if err != nil {
		fmt.Printf("can not create partID %v\n", err)
		return err
	}

	zeroMeta := pspb.PartitionMeta{
		LogStream: log.StreamID,
		RowStream: row.StreamID,
		Rg:        &pspb.Range{StartKey: []byte(""), EndKey: []byte("")},
		PartID:    partID,
	}

	err = etcd_utils.EtcdSetKV(etcdClient, fmt.Sprintf("PART/%d", partID), utils.MustMarshal(&zeroMeta))
	if err != nil {
		return err
	}

	fmt.Printf("bootstrap succeed, created new range partition %d\n", partID)
	return nil
}

func del(c *cli.Context) error {
	client, err := connectToAutumn(c)
	if err != nil {
		return err
	}
	defer client.Close()

	key := c.Args().First()
	if len(key) == 0 {
		return errors.New("no key")
	}

	return client.Delete(context.Background(), []byte(key))

}

func get(c *cli.Context) error {
	client, err := connectToAutumn(c)
	if err != nil {
		return err
	}
	defer client.Close()

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
	client, err := connectToAutumn(c)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := client.Connect(); err != nil {
		return err
	}
	start := c.String("start")
	prefix := c.String("prefix")
	limit := c.Int("limit")

	if len(start) == 0 && len(prefix) > 0 {
		start = prefix
	}

	if !strings.HasPrefix(start, prefix) {
		return errors.Errorf("start :[%s] does not have prefix [%s]", start, prefix)
	}

	out, _, err := client.Range(context.Background(), []byte(prefix), []byte(start), uint32(limit))
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
	client, err := connectToAutumn(c)
	if err != nil {
		return err
	}
	defer client.Close()

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

func gc(c *cli.Context) error {
	client, err := connectToAutumn(c)
	if err != nil {
		return err
	}
	defer client.Close()
	partIDString := c.Args().First()

	if len(partIDString) == 0 {
		return errors.New("partID is nil")

	}
	partID, err := strconv.ParseUint(partIDString, 10, 64)
	if err != nil {
		return errors.Errorf("partID is not int: %s", partIDString)
	}
	return client.Maintenance(context.Background(), partID, true, false)
}


func compact(c *cli.Context) error {
	client, err := connectToAutumn(c)
	if err != nil {
		return err
	}
	defer client.Close()
	partIDString := c.Args().First()

	if len(partIDString) == 0 {
		return errors.New("partID is nil")

	}
	partID, err := strconv.ParseUint(partIDString, 10, 64)
	if err != nil {
		return errors.Errorf("partID is not int: %s", partIDString)
	}
	return client.Maintenance(context.Background(), partID, false, true)
}

func splitPartition(c *cli.Context) error {
	client, err := connectToAutumn(c)
	if err != nil {
		return err
	}
	defer client.Close()

	partIDString := c.Args().First()

	if len(partIDString) == 0 {
		return errors.New("partID is nil")

	}
	partID, err := strconv.ParseUint(partIDString, 10, 64)
	if err != nil {
		return errors.Errorf("partID is not int: %s", partIDString)
	}
	return client.SplitPart(context.Background(), partID)
}

func info(c *cli.Context) error {
	smUrlss := utils.SplitAndTrim(c.String("smUrls"), ",")
	client := smclient.NewSMClient(smUrlss)
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
			Usage: "info --smUrls <path>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "smUrls", Value: "127.0.0.1:3401"},
			},
			Action: info,
		},

		{
			Name:  "bootstrap",
			Usage: "bootstrap -smUrls <addrs> --etcdUrls <addrs>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "smUrls", Value: "127.0.0.1:3401"},
				&cli.StringFlag{Name: "etcdUrls", Value: "127.0.0.1:2379"},
				&cli.StringFlag{Name: "replication", Value: "2+1"},
			},
			Action: bootstrap,
		},

		{
			Name:  "put",
			Usage: "put --etcdUrls <addrs> <KEY> <FILE>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdUrls", Value: "127.0.0.1:2379"},
			},
			Action: put,
		},
		{
			Name:  "get",
			Usage: "get --etcdUrls <addrs> <KEY>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdUrls", Value: "127.0.0.1:2379"},
			},
			Action: get,
		},
		{
			Name:  "del",
			Usage: "del --etcdUrls <addrs> <KEY>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdUrls", Value: "127.0.0.1:2379"},
			},
			Action: del,
		},
		{
			Name:  "split",
			Usage: "split --etcdUrls <addrs> <PARTID>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdUrls", Value: "127.0.0.1:2379"},
			},
			Action: splitPartition,
		},
		{
			Name:  "gc",
			Usage: "gc --etcdUrls <addrs> <PARTID>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdUrls", Value: "127.0.0.1:2379"},
			},
			Action: gc,
		},
		{
			Name:  "compact",
			Usage: "compact --etcdUrls <addrs> <PARTID>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdUrls", Value: "127.0.0.1:2379"},
			},
			Action: compact,
		},

		{
			Name:  "wbench",
			Usage: "wbench --etcdUrls <addrs> --thread <num> --duration <duration>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdUrls", Value: "127.0.0.1:2379"},
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
			Usage: "ls --etcdUrls <addrs> <prefix>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcdUrls", Value: "127.0.0.1:2379"},
				&cli.StringFlag{Name: "start", Value: ""},
				&cli.StringFlag{Name: "prefix", Value: ""},
				&cli.Int64Flag{Name: "limit", Value: math.MaxUint32},
			},
			Action: autumnRange,
		},
		{
			Name:  "format",
			Usage: "format --output file.toml --waldir <dir> --listen-url <URL> --sm-urls <URLS> --etcd-urls <URLS> <dir list> ",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "etcd-urls", Value: "127.0.0.1:2379"},
				&cli.StringFlag{Name: "sm-urls", Value: "127.0.0.1:3401"},
				&cli.StringFlag{Name: "listen-url"},
				&cli.StringFlag{Name: "advertise-url"},
				&cli.StringFlag{Name: "waldir"},
				&cli.StringFlag{Name: "output"},
			},
			Action: format,
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	os.Exit(0)
}

//FIXME: detect disk and verify , then register first, then write down uuid, node_id, directory level
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
	var err error
	smURLs := utils.SplitAndTrim(c.String("sm-urls"), ",")
	etcdURLs := utils.SplitAndTrim(c.String("etcd-urls"), ",")
	listenURL := c.String("listen-url")
	advertiseURL := c.String("advertise-url")
	output := c.String("output")

	walDir := c.String("waldir")
	if len(walDir) > 0 {
		_, err := os.Stat(walDir)
		if err != nil {
			return err
		}
	}

	dirList := c.Args().Slice()

	if len(advertiseURL) == 0 {
		return errors.New("listenUrl can not be empty")
	}
	if len(listenURL) == 0 {
		return errors.New("listenUrl can not be empty")
	}
	if len(dirList) == 0 {
		return errors.New("dir List can not be empty")
	}

	sm := smclient.NewSMClient(smURLs)

	if err = sm.Connect(); err != nil {
		return err
	}
	dirToUuid := make(map[string]string)
	uuids := make([]string, len(dirList))
	for i, dir := range dirList {
		uuid, err := node.FormatDisk(dir)
		dirToUuid[dir] = uuid
		uuids[i] = uuid
		if err != nil {
			revert(dirList)
			return err
		}
	}

	//register a new NodeID

	fmt.Printf("format on disks : %+v", dirList)

	fmt.Printf("register node on stream manager ..\n")
	nodeID, uuidToDiskID, err := sm.RegisterNode(context.Background(), uuids, advertiseURL)
	if err != nil {
		revert(dirList)
		return err
	}

	fmt.Printf("node %d is registered\n", nodeID)

	for _, dir := range dirList {
		nodeIDPath := path.Join(dir, "node_id")
		if err := ioutil.WriteFile(nodeIDPath, []byte(fmt.Sprintf("%d", nodeID)), 0644); err != nil {
			revert(dirList)
			return err
		}

		diskIDPath := path.Join(dir, "disk_id")
		diskID := uuidToDiskID[dirToUuid[dir]]
		if diskID == 0 {
			return errors.Errorf("diskID is nil")
		}
		if err := ioutil.WriteFile(diskIDPath, []byte(fmt.Sprintf("%d", diskID)), 0644); err != nil {
			revert(dirList)
			return err
		}
	}

	//generate config file for node
	var config node.Config
	config.Dirs = dirList
	config.ID = nodeID
	config.WalDir = walDir
	config.SmURLs = smURLs
	config.EtcdURLs = etcdURLs
	config.ListenURL = listenURL

	if len(output) == 0 {
		fmt.Printf("display config \n")
		fmt.Printf("%+v\n", config)
		return nil
	}

	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = toml.NewEncoder(f).Encode(config); err != nil {
		return err
	}
	f.Sync()
	return nil
}

func wbench(c *cli.Context) error {
	threadNum := c.Int("thread")
	etcdUrls := c.String("etcdUrls")
	duration := c.Int("duration")
	size := c.Int("size")
	etcdUrlss := utils.SplitAndTrim(etcdUrls, ",")
	return benchmark(etcdUrlss, WRITE_T, threadNum, duration, size)
}

func printSummary(elapsed time.Duration, totalCount uint64, totalSize uint64, threadNum int, size int, hist *utils.HistogramStatus) {
	if elapsed.Seconds() < 1e-9 {
		return
	}
	t := float64(totalSize) / elapsed.Seconds()
	fmt.Printf("\nSummary\n")
	fmt.Printf("Threads :%d\n", threadNum)
	fmt.Printf("Size    :%d\n", size)
	fmt.Printf("Time taken for tests :%v seconds\n", elapsed.Seconds())
	fmt.Printf("Complete requests :%d\n", totalCount)
	fmt.Printf("Total transferred :%d bytes\n", totalSize)
	fmt.Printf("Requests per second :%.2f [#/sec]\n", float64(totalCount)/elapsed.Seconds())
	fmt.Printf("Throughput per second :%s\n", utils.HumanReadableThroughput(t))
	fmt.Printf("Latency in millisecond p50, p95, p99: %v\n", hist.Histgram([]float64{50, 95, 99}, nil))
}

func connectToAutumn(c *cli.Context) (*autumn_clientv1.AutumnLib, error) {
	etcdUrls := utils.SplitAndTrim(c.String("etcdUrls"), ",")
	if len(etcdUrls) == 0 {
		return nil, errors.Errorf("etcdUrls is nil")
	}
	client := autumn_clientv1.NewAutumnLib(etcdUrls)
	if err := client.Connect(); err != nil {
		return nil, err
	}
	return client, nil
}