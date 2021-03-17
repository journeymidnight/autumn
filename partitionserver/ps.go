package partitionserver

import (
	"fmt"
	"io/ioutil"
	"net"
	"path"
	"strconv"

	"github.com/journeymidnight/autumn/manager/pmclient"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/rangepartition"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"google.golang.org/grpc"
)

type partID_t = uint64
type psID_t = uint64

type PartitionServer struct {
	utils.SafeMutex //protect rangePartitions
	rangePartitions map[partID_t]*rangepartition.RangePartition
	PSID            uint64
	pmClient        *pmclient.AutumnPMClient
	smClient        *smclient.SMClient
	//grcpServer  *grpc.Server //FIXME: get command from managers
	baseFileDir   string //store UUID
	address       string
	extentManager *streamclient.AutumnExtentManager
	blockReader   *streamclient.AutumnBlockReader
	grcpServer    *grpc.Server
}

func NewPartitionServer(smAddr []string, pmAddr []string, baseDir string, address string) *PartitionServer {
	return &PartitionServer{
		rangePartitions: make(map[partID_t]*rangepartition.RangePartition),
		smClient:        smclient.NewSMClient(smAddr),
		pmClient:        pmclient.NewAutumnPMClient(pmAddr),
		baseFileDir:     baseDir,
		address:         address,
	}
}

func (ps *PartitionServer) Init() {
	utils.AssertTrue(xlog.Logger != nil)

	//
	if err := ps.smClient.Connect(); err != nil {
		xlog.Logger.Fatalf(err.Error())
	}

	if err := ps.pmClient.Connect(); err != nil {
		xlog.Logger.Fatalf(err.Error())
	}

	ps.registerPS()

	//get lease:FIXME

	ps.extentManager = streamclient.NewAutomnExtentManager(ps.smClient)
	ps.blockReader = streamclient.NewAutumnBlockReader(ps.extentManager, ps.smClient)

	metas := ps.pmClient.GetPartitionMeta(ps.PSID)
	xlog.Logger.Infof("get all partitions for PS :%d: RangePartitions", metas)

	for _, partMeta := range metas {
		xlog.Logger.Infof("start %+v\n", partMeta)
		if err := ps.startRangePartition(partMeta); err != nil {
			xlog.Logger.Fatal(err.Error())
		}
	}

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

	var startKey []byte
	var endKey []byte
	if meta.Rg != nil {
		startKey = meta.Rg.StartKey
		endKey = meta.Rg.EndKey
	}

	var locs []*pspb.Location
	if meta.Locs != nil {
		locs = meta.Locs.Locs
	}

	var blobs []uint64
	if meta.Blobs != nil {
		blobs = meta.Blobs.Blob
	}

	rp := rangepartition.OpenRangePartition(meta.PartID, row, log, ps.blockReader, startKey, endKey, locs,
		blobs, ps.pmClient, openStream, nil)

	//FIXME: check each partID is uniq
	ps.Lock()
	ps.rangePartitions[meta.PartID] = rp
	ps.Unlock()
	return nil
}

func (ps *PartitionServer) stopRangePartition() {
	//FIXME
}

func (ps *PartitionServer) Close() {

}

func (ps *PartitionServer) registerPS() {
	utils.AssertTrue(ps.address != "")

	//two lines//PSID相同但是address修改的情况
	storeIDPath := path.Join(ps.baseFileDir, "PSID")
	//read file
	idString, err := ioutil.ReadFile(storeIDPath)

	if err == nil {
		id, err := strconv.ParseUint(string(idString), 10, 64)
		if err != nil {
			xlog.Logger.Fatalf("can not read ioString")
		}
		ps.PSID = id
		return
	}

	xlog.Logger.Infof("Register Partition Server")
	id, err := ps.pmClient.RegisterSelf(ps.address)
	if err != nil {
		xlog.Logger.Fatalf(err.Error())
	}

	ps.PSID = id
	if err = ioutil.WriteFile(storeIDPath, []byte(fmt.Sprintf("%d", id)), 0644); err != nil {
		xlog.Logger.Fatalf("try to write file %s, %d, but failed, try to save it manually", storeIDPath, id)
	}
	xlog.Logger.Infof("success to register to sm")
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
