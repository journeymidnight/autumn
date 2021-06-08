package partition_server

import (
	"net"

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
	//grcpServer  *grpc.Server //FIXME: get command from managers
	address       string
	extentManager *smclient.ExtentManager
	blockReader   *streamclient.AutumnBlockReader
	grcpServer    *grpc.Server
}

func NewPartitionServer(smAddr []string, pmAddr []string, PSID uint64, address string) *PartitionServer {
	return &PartitionServer{
		rangePartitions: make(map[partID_t]*range_partition.RangePartition),
		smClient:        smclient.NewSMClient(smAddr),
		PSID:            PSID,
		address:         address,
	}
}

func (ps *PartitionServer) Init() {

	utils.AssertTrue(xlog.Logger != nil)

	//
	if err := ps.smClient.Connect(); err != nil {
		xlog.Logger.Fatalf(err.Error())
	}
	ps.extentManager = smclient.NewExtentManager(ps.smClient)
	ps.blockReader = streamclient.NewAutumnBlockReader(ps.extentManager, ps.smClient)

	//create session

	
	loadPartitions()
	startWatch()

	/*
		metas := ps.pmClient.GetPartitionMeta(ps.PSID)
		xlog.Logger.Infof("get all partitions for PS :%+v: RangePartitions", metas)

		for _, partMeta := range metas {
			xlog.Logger.Infof("start %+v\n", partMeta)
			if err := ps.startRangePartition(partMeta); err != nil {
				xlog.Logger.Fatal(err.Error())
			}
		}
	*/

}

func (ps *PartitionServer) startRangePartition(meta *pspb.PartitionMeta) error {
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

	setRowStreamTables := func(){

	}

	var locs []*pspb.Location
	if meta.Locs != nil {
		locs = meta.Locs.Locs
	}

	var blobs []uint64
	if meta.Blobs != nil {
		blobs = meta.Blobs.Blob
	}

	utils.AssertTrue(meta.Rg != nil)
	utils.AssertTrue(meta.PartID != 0)

	rp := range_partition.OpenRangePartition(meta.PartID, row, log, ps.blockReader, meta.Rg.StartKey, meta.Rg.EndKey, locs,
		blobs, ps.pmClient, openStream, range_partition.DefaultOption())

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
