package partition_server

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"

	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/range_partition"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"google.golang.org/grpc"
)

type partID_t = uint64
type psID_t = uint64

type PartitionServer struct {
	utils.SafeMutex //protect rangePartitions
	rangePartitions map[partID_t]*range_partition.RangePartition
	rangePartitionLocks map[partID_t]*concurrency.Mutex
	PSID            uint64
	smClient        *smclient.SMClient
	etcdClient      *clientv3.Client     
	address       string
	smAddr        []string
	etcdAddr      []string
	extentManager *smclient.ExtentManager
	blockReader   *streamclient.AutumnBlockReader
	grcpServer    *grpc.Server
	session       *concurrency.Session
	watchCh       *clientv3.WatchChan
	closeWatchCh  func()
}

func NewPartitionServer(smAddr []string, etcdAddr []string, PSID uint64, address string) *PartitionServer {
	return &PartitionServer{
		rangePartitions: make(map[partID_t]*range_partition.RangePartition),
		rangePartitionLocks: make(map[uint64]*concurrency.Mutex),
		smClient:        smclient.NewSMClient(smAddr),
		PSID:            PSID,
		address:         address,
		smAddr: smAddr,
		etcdAddr: etcdAddr,
	}
}


func formatPartLock(partID uint64) string {
	return fmt.Sprintf("partLock/%d", partID)
}


//FIXME: discard implement
//
func (ps *PartitionServer) getPartitionMeta(partID uint64) (int64, *pspb.PartitionMeta, []*pspb.Location, []uint64, error) {
	/*
	PART/{PartID} => {id, id <startKey, endKEY>} //immutable

    PARTSTATS/{PartID}/tables => [(extentID,offset),...,(extentID,offset)]
    PARTSTATS/{PartID}/blobStreams => [id,...,id]
    PARTSTATS/{PartID}/discard => <DATA>
    */

	var rev int64
	data, newRev, err := etcd_utils.EtcdGetKV(ps.etcdClient, fmt.Sprintf("PART/%d", partID))
	if err != nil {
		return 0, nil, nil, nil, err
	}
	rev = utils.Max64(newRev, rev)
	var meta pspb.PartitionMeta
	if err = meta.Unmarshal(data); err != nil {
		return 0, nil, nil, nil, err
	}
	
	kvs, newRev, err := etcd_utils.EtcdRange(ps.etcdClient, fmt.Sprintf("PARTSTATS/%d", partID))
	if err != nil {
		return 0, nil, nil, nil, err
	}
	rev = utils.Max64(newRev, rev)

	var rlocs pspb.TableLocations
	var locs []*pspb.Location

	var rblob pb.BlobStreams
	var blob []uint64


	for _, kv := range kvs {
		if strings.HasSuffix(string(kv.Key), "tables") {
			if err = rlocs.Unmarshal(kv.Value) ; err != nil {
				xlog.Logger.Warnf("parse %s error", kv.Key)
				continue
			}
			locs = rlocs.Locs
		} else if strings.HasSuffix(string(kv.Key), "blobStreams") {
			if err = rblob.Unmarshal(kv.Value); err != nil {
				xlog.Logger.Warnf("parse %s error", kv.Key)
				continue
			}
			blob = make([]uint64, 0, len(rblob.Blobs))
			for k := range rblob.Blobs {
				blob = append(blob, k)
			}
		} else if strings.HasSuffix(string(kv.Key), "discard") {
			//TODO
		} else {
			xlog.Logger.Warnf("unkown key %s", kv.Key)
		}
	}

	return rev, &meta, locs, blob, nil
}

func (ps *PartitionServer) parseRegionAndStart(regions *pspb.Regions) int64{
	var rev int64
	var meta *pspb.PartitionMeta
	var blobs []uint64
	var locs []*pspb.Location
	var err error
	fmt.Printf("current region: %+v\n", regions.Regions)

	for _, region := range regions.Regions {
		//ok : if we have activated PART
		var ok bool
		ps.RLock()
		_, ok = ps.rangePartitions[region.PartID]
		ps.RUnlock()

		utils.AssertTrue(region.PartID != 0)
		
		if region.PSID != ps.PSID{
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
		mutex := concurrency.NewMutex(ps.session, formatPartLock(region.PartID))
		utils.Check(mutex.Lock(context.Background()))

		if rev, meta, locs ,blobs, err = ps.getPartitionMeta(region.PartID) ; err != nil {
			xlog.Logger.Errorf(err.Error())
			mutex.Unlock(context.Background())
			continue
		}

		xlog.Logger.Infof("range partition %d starting, meta: %v", meta.PartID, meta)

		rp, err := ps.startRangePartition(meta, locs, blobs, mutex)
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
	if err := ps.smClient.Connect(); err != nil {
		xlog.Logger.Fatalf(err.Error())
	}
	ps.extentManager = smclient.NewExtentManager(ps.smClient, ps.etcdAddr, nil)
	ps.blockReader = streamclient.NewAutumnBlockReader(ps.extentManager, ps.smClient)

	//share the connection with extentManager
	ps.etcdClient = ps.extentManager.EtcdClient()
	
	session , err := concurrency.NewSession(ps.etcdClient, concurrency.WithTTL(30))
	utils.Check(err)

	ps.session = session


	//session create PSSERVER/{PSID} => {PSDETAIL}
	var detail = pspb.PSDetail{
		Address: ps.address,
		PSID: ps.PSID,
	}

	keyName := fmt.Sprintf("PSSERVER/%d", ps.PSID)
	cmp := []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(keyName), "=", 0)}
	ops := []clientv3.Op{
		clientv3.OpPut(keyName, string(utils.MustMarshal(&detail)), clientv3.WithLease(session.Lease())),
	}
	utils.Check(etcd_utils.EtcdSetKVS(ps.etcdClient, cmp, ops))



	//read regions/config
	data, rev, err := etcd_utils.EtcdGetKV(ps.etcdClient, "regions/config")
	if err != nil {
		xlog.Logger.Fatalf(err.Error())
	}
	
	var config pspb.Regions
	utils.MustUnMarshal(data, &config)

	fmt.Printf("regions config is %+v", config.Regions)

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
			if err = regions.Unmarshal(e.Kv.Value) ; err != nil {
				xlog.Logger.Errorf(err.Error())
				continue
			}
			ps.parseRegionAndStart(&regions)
		}
	}()
	ps.watchCh = &watchConfigCh
	ps.closeWatchCh = closeWatchCh

}

func (ps *PartitionServer) startRangePartition(meta *pspb.PartitionMeta, locs []*pspb.Location, blobs []uint64, mutex *concurrency.Mutex) (*range_partition.RangePartition, error) {
	//1. pmclient get info
	//2. streamclient connect
	//3. open RangePartition
	var row, log *streamclient.AutumnStreamClient

	cleanup := func() {
		if row != nil {
			row.Close()
		}
		if log != nil {
			log.Close()
		}
	}

	row = streamclient.NewStreamClient(ps.smClient, ps.extentManager, meta.RowStream, streamclient.MutexToLock(mutex))
	if err := row.Connect(); err != nil {
		return nil, err
	}

	log = streamclient.NewStreamClient(ps.smClient, ps.extentManager, meta.LogStream, streamclient.MutexToLock(mutex))

	if err := log.Connect(); err != nil {
		cleanup()
		return nil, err
	}

	openStream := func(si pb.StreamInfo) streamclient.StreamClient {
		return streamclient.NewStreamClient(ps.smClient, ps.extentManager, si.StreamID, streamclient.MutexToLock(mutex))
	}

	setRowStreamTables :=  func(id uint64, tables []*pspb.Location) error {
		utils.AssertTrue(id == meta.PartID)

		var locations pspb.TableLocations
		locations.Locs = tables
		data := utils.MustMarshal(&locations)
		//make sure lock
	
		return etcd_utils.EtcdSetKVS(ps.etcdClient, 
			[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(mutex.Key()), "=", mutex.Header().Revision)},
		    []clientv3.Op{clientv3.OpPut(fmt.Sprintf("PARTSTATS/%d/tables", meta.PartID), string(data)),
		})
	}



	utils.AssertTrue(meta.Rg != nil)
	utils.AssertTrue(meta.PartID != 0)


	rp, err := range_partition.OpenRangePartition(meta.PartID, row, log, ps.blockReader, meta.Rg.StartKey, meta.Rg.EndKey, locs,
		blobs, setRowStreamTables, openStream, range_partition.DefaultOption())
	
	xlog.Logger.Infof("open range partition %d, StartKey:[%s], EndKey:[%s]: err is %v", meta.PartID, meta.Rg.StartKey, meta.Rg.EndKey, err)
	return rp, err
}

func (ps *PartitionServer) Close() {
	ps.closeWatchCh()
	ps.Lock()
	defer ps.Unlock()
	for _, rp := range ps.rangePartitions {
		rp.Close()
		if mutex, ok := ps.rangePartitionLocks[rp.PartID]; ok {
			ctx , cancel := context.WithTimeout(context.Background(), time.Second)
			mutex.Unlock(ctx)
			cancel()
		}
	}
}

func (ps *PartitionServer) ServeGRPC() error {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(65<<20),
		grpc.MaxSendMsgSize(65<<20),
		grpc.MaxConcurrentStreams(1000),
	)

	pspb.RegisterPartitionKVServer(grpcServer, ps)
	listener, err := net.Listen("tcp", ps.address)
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
	//FIXME
	ps.session.Close()
}
