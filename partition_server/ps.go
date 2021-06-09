package partition_server

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"

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
	PSID            uint64
	smClient        *smclient.SMClient
	etcdClient      *clientv3.Client     
	address       string
	smAddr        []string
	etcdAddr      []string
	extentManager *smclient.ExtentManager
	blockReader   *streamclient.AutumnBlockReader
	grcpServer    *grpc.Server
}

func NewPartitionServer(smAddr []string, etcdAddr []string, PSID uint64, address string) *PartitionServer {
	return &PartitionServer{
		rangePartitions: make(map[partID_t]*range_partition.RangePartition),
		smClient:        smclient.NewSMClient(smAddr),
		PSID:            PSID,
		address:         address,
		smAddr: smAddr,
		etcdAddr: etcdAddr,
	}
}


func formatPartLock(partID uint64) string {
	return fmt.Sprintf("lock/%d", partID)
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

	data, err := etcd_utils.EtcdGetKV(ps.etcdClient, fmt.Sprintf("PART/%d", partID))
	if err != nil {
		return 0, nil, nil, nil, err
	}
	var meta *pspb.PartitionMeta
	if err = meta.Unmarshal(data); err != nil {
		return 0, nil, nil, nil, err
	}

	kvs, rev, err := etcd_utils.EtcdRange(ps.etcdClient, fmt.Sprintf("PARTSTATS/%d", partID))
	if err != nil {
		return 0, nil, nil, nil, err
	}

	var rlocs pspb.TableLocations
	var locs []*pspb.Location

	var rblob pspb.BlobStreams
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
			blob = rblob.Blob
		} else if strings.HasSuffix(string(kv.Key), "discard") {
			//TODO
		} else {
			xlog.Logger.Warnf("unkown key %s", kv.Key)
		}
	}

	return rev, meta, locs, blob, nil
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
	
	//read regions/config
	session , err := concurrency.NewSession(ps.etcdClient, concurrency.WithTTL(30))
	utils.Check(err)


	data, err := etcd_utils.EtcdGetKV(ps.etcdClient, "regions/config")
	if err != nil {
		xlog.Logger.Fatalf(err.Error())
	}
	
	var config pspb.Regions
	utils.MustUnMarshal(data, &config)



	var rev int64
	var meta *pspb.PartitionMeta
	var blobs []uint64
	var locs []*pspb.Location
	for _, region := range config.Regions {
		if region.PSID != ps.PSID {
			continue
		}
		//lock
		var mutex *concurrency.Mutex
		mutex = concurrency.NewMutex(session, formatPartLock(region.PartID))
		utils.Check(mutex.Lock(context.Background()))

		if rev, meta, locs ,blobs, err = ps.getPartitionMeta(region.PartID) ; err != nil {
			xlog.Logger.Errorf(err.Error())
			continue
		}
		if err = ps.startRangePartition(meta, locs, blobs) ; err != nil {
			xlog.Logger.Errorf(err.Error())
			mutex.Unlock(context.Background())
		}

	}

	//startWatch()

}

func (ps *PartitionServer) startRangePartition(meta *pspb.PartitionMeta, locs []*pspb.Location, blobs []uint64) error {
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

	row = streamclient.NewStreamClient(ps.smClient, ps.extentManager, meta.RowStream)
	if err := row.Connect(); err != nil {
		return err
	}

	log = streamclient.NewStreamClient(ps.smClient, ps.extentManager, meta.LogStream)

	if err := log.Connect(); err != nil {
		cleanup()
		return err
	}

	openStream := func(si pb.StreamInfo) streamclient.StreamClient {
		return streamclient.NewStreamClient(ps.smClient, ps.extentManager, si.StreamID)
	}

	setRowStreamTables :=  func(id uint64, tables []*pspb.Location) error {

		return nil
	}



	utils.AssertTrue(meta.Rg != nil)
	utils.AssertTrue(meta.PartID != 0)

	rp := range_partition.OpenRangePartition(meta.PartID, row, log, ps.blockReader, meta.Rg.StartKey, meta.Rg.EndKey, locs,
		blobs, setRowStreamTables, openStream, range_partition.DefaultOption())

	//FIXME: check each partID is uniq
	ps.Lock()
	ps.rangePartitions[meta.PartID] = rp
	ps.Unlock()
	xlog.Logger.Infof("open range partition %d, StartKey:[%s], EndKey:[%s]", meta.PartID, meta.Rg.StartKey, meta.Rg.EndKey)
	return nil
}

func (ps *PartitionServer) stopRangePartition() {
	//FIXME
}

func (ps *PartitionServer) Close() {

}

func (ps *PartitionServer) ServeGRPC() error {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(65<<20),
		grpc.MaxSendMsgSize(65<<20),
		grpc.MaxConcurrentStreams(1000),
	)

	//FIXME: register manager service
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
}
