package streamclient

import (
	"context"
	"sync"
	"time"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

//for single stream
type StreamClient struct {
	smClient     *smclient.SMClient
	sync.RWMutex //protect streamInfo/extentInfo when called in read/write
	streamInfo   *pb.StreamInfo
	extentInfo   map[uint64]*pb.ExtentInfo
	streamID     uint64
	writeCh      chan *Op
	completeCh   chan AppendResult
	stopper      *utils.Stopper
	iodepth      int
}

var (
	batchSize = 4
)

func NewStreamClient(sm *smclient.SMClient, streamID uint64, iodepth int) *StreamClient {
	utils.AssertTrue(xlog.Logger != nil)
	utils.AssertTrue(iodepth > batchSize)
	return &StreamClient{
		smClient:   sm,
		streamID:   streamID,
		writeCh:    make(chan *Op, iodepth),
		completeCh: make(chan AppendResult, iodepth),
		stopper:    utils.NewStopper(),
		iodepth:    iodepth,
	}
}

type AppendResult struct {
	ExtentID    uint64
	Offset      uint32
	NumOfBlocks int
	Err         error
	UserData    interface{}
}

type Op struct {
	blocks   []*pb.Block
	userData interface{}
}

func (op Op) Size() uint32 {
	ret := uint32(0)
	for i := range op.blocks {
		ret += op.blocks[i].BlockLength
	}
	return ret
}

func (sc *StreamClient) getPeers(extentID uint64) []string {
	sc.RLock()
	defer sc.RUnlock()
	extentInfo := sc.extentInfo[extentID]
	var ret []string
	for _, id := range extentInfo.Replicates {
		n := sc.smClient.LookupNode(id)
		utils.AssertTrue(n != nil)
		ret = append(ret, n.Address)
	}
	return ret
}

func (sc *StreamClient) getExtentConn(extentID uint64) *grpc.ClientConn {
	sc.RLock()
	defer sc.RUnlock()
	extentInfo := sc.extentInfo[extentID]
	nodeInfo := sc.smClient.LookupNode(extentInfo.Replicates[0])

	pool := conn.GetPools().Connect(nodeInfo.Address)
	return pool.Get()
}

func (sc *StreamClient) getLastExtentConn() (uint64, *grpc.ClientConn) {
	sc.RLock()
	defer sc.RUnlock()
	extentID := sc.streamInfo.ExtentIDs[len(sc.streamInfo.ExtentIDs)-1] //last extentd
	extentInfo := sc.extentInfo[extentID]
	nodeInfo := sc.smClient.LookupNode(extentInfo.Replicates[0])

	pool := conn.GetPools().Connect(nodeInfo.Address)
	return extentID, pool.Get()
}

//TODO: 有可能需要append是幂等的
func (sc *StreamClient) streamBlocks() {
	for {
		select {
		case <-sc.stopper.ShouldStop():
			return
		case op := <-sc.writeCh:
			size := uint32(0)
			blocks := []*pb.Block{}
			var ops []*Op
		slurpLoop:
			for {
				ops = append(ops, op)
				blocks = append(blocks, op.blocks...)
				size += op.Size()
				if size > (20 << 20) { //batch data
					break slurpLoop
				}
				select {
				case op = <-sc.writeCh:
				default:
					break slurpLoop
				}
			}
			xlog.Logger.Debugf("len of ops is %d, write is %d", len(blocks), size)

		retry:
			extentID, conn := sc.getLastExtentConn()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			c := pb.NewExtentServiceClient(conn)
			res, err := c.Append(ctx, &pb.AppendRequest{
				ExtentID: extentID,
				Blocks:   blocks,
				Peers:    sc.getPeers(extentID),
			})
			cancel()

			if err != nil {
				var newExInfo *pb.ExtentInfo
				for err != nil {
					//loop forever to seal and alloc new extent
					xlog.Logger.Warn(err.Error())
					newExInfo, err = sc.smClient.StreamAllocExtent(ctx, sc.streamID, extentID)
				}
				sc.Lock()
				sc.streamInfo.ExtentIDs = append(sc.streamInfo.ExtentIDs, newExInfo.ExtentID)
				sc.extentInfo[newExInfo.ExtentID] = newExInfo
				sc.Unlock()
				goto retry
			} else {
				i := 0
				for _, op := range ops {
					sc.completeCh <- AppendResult{extentID, res.Offsets[i], len(op.blocks), nil, op.userData}
					i += len(op.blocks)
				}
			}
		}
	}
}

func (sc *StreamClient) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	s, e, err := sc.smClient.StreamInfo(ctx, []uint64{sc.streamID})
	cancel()
	if err != nil {
		return err
	}
	sc.streamInfo = s[sc.streamID]
	sc.extentInfo = e

	sc.stopper.RunWorker(sc.streamBlocks)

	return nil
}

func (sc *StreamClient) Close() {
	sc.stopper.Stop()
}

//GetAppendComplete block

func (sc *StreamClient) GetComplete() AppendResult {
	return <-sc.completeCh
}

//TryComplete will block when writeCh is full
func (sc *StreamClient) TryComplete() []AppendResult {
	var result []AppendResult
	if len(sc.writeCh) == sc.iodepth {
		for len(sc.writeCh)+batchSize >= sc.iodepth {
			select {
			case ret := <-sc.completeCh:
				result = append(result, ret)
			}
		}
		return result
	}

slurpLoop:
	for {
		select {
		case ret := <-sc.completeCh:
			for {
				result = append(result, ret)
				select {
				case ret = <-sc.completeCh:
				default:
					break slurpLoop
				}
			}
		default:
			break slurpLoop
		}
	}
	return result

}

//Append blocks, it should never blocked
func (sc *StreamClient) Append(ctx context.Context, blocks []*pb.Block, userData interface{}) error {
	for i := range blocks {
		if len(blocks[i].Data)%4096 != 0 {
			return errors.Errorf("not aligned")
		}
	}
	op := Op{
		blocks:   blocks,
		userData: userData,
	}
	select {
	case <-ctx.Done():
		return context.Canceled
	case sc.writeCh <- &op:
		return nil
	}
}

func (sc *StreamClient) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, error) {
	conn := sc.getExtentConn(extentID)
	if conn == nil {
		return nil, errors.Errorf("no such extent")
	}
	c := pb.NewExtentServiceClient(conn)
	res, err := c.ReadBlocks(ctx, &pb.ReadBlocksRequest{
		ExtentID:    extentID,
		Offset:      offset,
		NumOfBlocks: numOfBlocks,
	})
	return res.Blocks, err
}
