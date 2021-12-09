package partition_server

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/range_partition"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/opentracing/opentracing-go"
	"github.com/robfig/cron/v3"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"google.golang.org/grpc"
)

type Config struct {
	PSID                 uint64
	AdvertiseURL         string
	ListenURL            string
	SmURLs               []string
	EtcdURLs             []string
	MustSync             bool
	CronTimeGC           string
	CronTimeMajorCompact string
	MaxExtentSize        uint32 //in the unit of Bytes
	MaxMetaExtentSize    uint32 //in the unit of Bytes
	SkipListSize         uint32 //in the unit of Bytes
}

type PartitionServer struct {
	utils.SafeMutex     //protect rangePartitions
	rangePartitions     map[uint64]*range_partition.RangePartition
	rangePartitionLocks map[uint64]*concurrency.Mutex
	PSID                uint64
	smClient            *smclient.SMClient
	etcdClient          *clientv3.Client
	config              Config
	extentManager       *smclient.ExtentManager
	grcpServer          *grpc.Server
	session             *concurrency.Session
	watchCh             *clientv3.WatchChan
	closeWatchCh        func()
	cron                *cron.Cron
}

func NewPartitionServer(config Config) *PartitionServer {
	return &PartitionServer{
		rangePartitions:     make(map[uint64]*range_partition.RangePartition),
		rangePartitionLocks: make(map[uint64]*concurrency.Mutex),
		smClient:            smclient.NewSMClient(config.SmURLs),
		PSID:                config.PSID,
		config:              config,
		cron:                cron.New(cron.WithLogger(xlog.CronLogger{})),
	}
}

func formatPartLock(partID uint64) string {
	return fmt.Sprintf("partLock/%d", partID)
}

func (ps *PartitionServer) getPartitionMeta(partID uint64) (int64, *pspb.PartitionMeta, error) {
	/*
		PART/{PartID} => {id, id <startKey, endKEY>} //immutable
	*/

	var rev int64
	data, newRev, err := etcd_utils.EtcdGetKV(ps.etcdClient, fmt.Sprintf("PART/%d", partID))
	if err != nil {
		return 0, nil, err
	}
	rev = utils.Max64(newRev, rev)
	var meta pspb.PartitionMeta
	if err = meta.Unmarshal(data); err != nil {
		return 0, nil, err
	}

	return rev, &meta, nil
}

func (ps *PartitionServer) parseRegionAndStart(regions *pspb.Regions) int64 {
	var rev int64
	var meta *pspb.PartitionMeta
	var err error
	fmt.Printf("current region: %+v\n", regions.Regions)

	for _, region := range regions.Regions {
		//ok : if we have activated PART
		var ok bool
		ps.RLock()
		_, ok = ps.rangePartitions[region.PartID]
		ps.RUnlock()

		utils.AssertTrue(region.PartID != 0)

		if region.PSID != ps.PSID {
			if ok {
				//如果merge或者split存在, 会先close range_partion, 然后再修改regions.
				//但是允许close range_partition超时或者失败
				ps.Lock()
				ps.rangePartitions[region.PartID].Close()
				delete(ps.rangePartitionLocks, region.PartID)
				delete(ps.rangePartitions, region.PartID)
				ps.Unlock()
			}
			continue
		}

		//if PART already activated
		if ok {
			continue
		}

		//if PART did not activate, lock and activate
		fmt.Printf("locking part %d\n", region.PartID)
		var mutex *concurrency.Mutex
		for {
			mutex = concurrency.NewMutex(ps.session, formatPartLock(region.PartID))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if mutex.Lock(ctx) == nil {
				cancel()
				break
			}
			cancel()
			time.Sleep(time.Second)
			fmt.Printf("locking part %d timout..\n", region.PartID)
		}

		fmt.Printf("getPartionMeta %d\n", region.PartID)
		if rev, meta, err = ps.getPartitionMeta(region.PartID); err != nil {
			xlog.Logger.Errorf(err.Error())
			mutex.Unlock(context.Background())
			continue
		}

		xlog.Logger.Infof("range partition %d starting, meta: %v", meta.PartID, meta)
		fmt.Printf("range partition %d starting, meta: %v", meta.PartID, meta)
		rp, err := ps.startRangePartition(meta, mutex)
		if err != nil {
			xlog.Logger.Errorf(err.Error())
			mutex.Unlock(context.Background())
			continue
		}
		utils.AssertTrue(meta.PartID == region.PartID)
		ps.Lock()
		ps.rangePartitionLocks[meta.PartID] = mutex
		ps.rangePartitions[meta.PartID] = rp
		ps.Unlock()
		xlog.Logger.Infof("range partition %d started", meta.PartID)

	}
	return rev

}
func (ps *PartitionServer) Init() {

	utils.AssertTrue(xlog.Logger != nil)

	//

	ps.extentManager = smclient.NewExtentManager(ps.smClient, ps.config.EtcdURLs, nil)

	if err := ps.smClient.Connect(); err != nil {
		xlog.Logger.Fatalf(err.Error())
	}

	//share the connection with extentManager
	ps.etcdClient = ps.extentManager.EtcdClient()

	session, err := concurrency.NewSession(ps.etcdClient, concurrency.WithTTL(60))
	utils.Check(err)

	ps.session = session

	//if session is Done, quit
	go func() {
		for {
			select {
			case <-session.Done():
				fmt.Println("session closed")
				xlog.Logger.Fatalf("session closed")
				os.Exit(0)
			}
		}
	}()

	//session create PSSERVER/{PSID} => {PSDETAIL}
	var detail = pspb.PSDetail{
		Address: ps.config.AdvertiseURL,
		PSID:    ps.PSID,
	}

	keyName := fmt.Sprintf("PSSERVER/%d", ps.PSID)
	cmp := []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(keyName), "=", 0)}
	ops := []clientv3.Op{
		clientv3.OpPut(keyName, string(utils.MustMarshal(&detail)), clientv3.WithLease(session.Lease())),
	}
	for {
		err = etcd_utils.EtcdSetKVS(ps.etcdClient, cmp, ops)
		if err == nil {
			break
		}
		fmt.Printf("create session %s timeout..retying..\n", keyName)
		time.Sleep(time.Second)
	}

	//read regions/config
	data, rev, err := etcd_utils.EtcdGetKV(ps.etcdClient, "regions/config")
	if err != nil {
		xlog.Logger.Fatalf(err.Error())
	}

	var config pspb.Regions
	utils.MustUnMarshal(data, &config)

	//start partitions
	newRev := ps.parseRegionAndStart(&config)
	if newRev > rev {
		rev = newRev
	}

	//startWatch
	watchConfigCh, closeWatchCh := etcd_utils.EtcdWatchEvents(ps.etcdClient, "regions/config", "", rev)
	go func() {
		for res := range watchConfigCh {
			//skip to the last, only cares about latest config
			e := res.Events[len(res.Events)-1]
			var regions pspb.Regions
			if err = regions.Unmarshal(e.Kv.Value); err != nil {
				xlog.Logger.Errorf(err.Error())
				continue
			}
			ps.parseRegionAndStart(&regions)
		}
	}()
	ps.watchCh = &watchConfigCh
	ps.closeWatchCh = closeWatchCh

	//start cron task:

	ps.cron.AddFunc(ps.config.CronTimeGC, ps.CronTaskGC)

	ps.cron.AddFunc(ps.config.CronTimeMajorCompact, ps.CronTaskCompact)
}

func (ps *PartitionServer) CronTaskGC() {
	//copy range partitions
	ps.RLock()
	rangePartitions := make([]*range_partition.RangePartition, len(ps.rangePartitions))
	for _, rp := range ps.rangePartitions {
		rangePartitions = append(rangePartitions, rp)
	}
	ps.RUnlock()
	for i := 0; i < len(rangePartitions); i++ {
		fmt.Printf("submit auto gc on range partion %d\n", rangePartitions[i].PartID)
		rangePartitions[i].SubmitGC(range_partition.GcTask{}) //auto task
	}
}

func (ps *PartitionServer) CronTaskCompact() {
	//copy range partitions
	ps.RLock()
	rangePartitions := make([]*range_partition.RangePartition, len(ps.rangePartitions))
	for _, rp := range ps.rangePartitions {
		rangePartitions = append(rangePartitions, rp)
	}
	ps.RUnlock()
	for i := 0; i < len(rangePartitions); i++ {
		fmt.Printf("submit major compaction on range partion %d\n", rangePartitions[i].PartID)
		rangePartitions[i].SubmitCompaction()
	}
}

func (ps *PartitionServer) startRangePartition(meta *pspb.PartitionMeta, mutex *concurrency.Mutex) (*range_partition.RangePartition, error) {
	//1. pmclient get info
	//2. streamclient connect
	//3. open RangePartition
	var row, log, metaLog *streamclient.AutumnStreamClient

	cleanup := func() {
		if row != nil {
			row.Close()
		}
		if log != nil {
			log.Close()
		}
		if metaLog != nil {
			metaLog.Close()
		}
	}

	row = streamclient.NewStreamClient(ps.smClient, ps.extentManager, ps.config.MaxExtentSize, meta.RowStream, streamclient.MutexToLock(mutex))
	if err := row.Connect(); err != nil {
		return nil, err
	}

	log = streamclient.NewStreamClient(ps.smClient, ps.extentManager, ps.config.MaxExtentSize, meta.LogStream, streamclient.MutexToLock(mutex))
	if err := log.Connect(); err != nil {
		cleanup()
		return nil, err
	}

	metaLog = streamclient.NewStreamClient(ps.smClient, ps.extentManager, ps.config.MaxMetaExtentSize, meta.MetaStream, streamclient.MutexToLock(mutex))
	if err := metaLog.Connect(); err != nil {
		cleanup()
		return nil, err
	}

	utils.AssertTrue(meta.Rg != nil)
	utils.AssertTrue(meta.PartID != 0)

	rp, err := range_partition.OpenRangePartition(meta.PartID, metaLog, row, log, meta.Rg.StartKey, meta.Rg.EndKey,
		range_partition.DefaultOption(),
		range_partition.WithMaxSkipList(int64(ps.config.SkipListSize)),
		range_partition.WithSync(ps.config.MustSync),
	)

	xlog.Logger.Infof("open range partition %d, StartKey:[%s], EndKey:[%s]: err is %v", meta.PartID, meta.Rg.StartKey, meta.Rg.EndKey, err)
	return rp, err
}

func (ps *PartitionServer) ServeGRPC() error {

	cfg, err := config.FromEnv()

	cfg.ServiceName = fmt.Sprintf("PS-%d", ps.config.PSID)
	cfg.Sampler.Type = "const"
	cfg.Sampler.Param = 1
	cfg.Reporter.LogSpans = true

	tracer, _, err := cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(err)
	}
	opentracing.SetGlobalTracer(tracer)

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(64<<20),
		grpc.MaxSendMsgSize(64<<20),
		grpc.MaxConcurrentStreams(1000),
	)

	pspb.RegisterPartitionKVServer(grpcServer, ps)
	listener, err := net.Listen("tcp", ps.config.ListenURL)
	if err != nil {
		return err
	}
	go func() {
		grpcServer.Serve(listener)
	}()
	ps.grcpServer = grpcServer
	return nil
}

func (ps *PartitionServer) Shutdown() {

	//1. close grpc server
	if ps.grcpServer != nil {
		ps.grcpServer.GracefulStop()
	}

	//1.5 close all crontab tasks
	ps.cron.Stop()

	//2. close all range partition
	ps.Lock()
	for _, rp := range ps.rangePartitions {
		rp.Close()
	}
	ps.Unlock()
	time.Sleep(300 * time.Millisecond)
	//FIXME, will release all mutex as well
	ps.session.Close()
}
