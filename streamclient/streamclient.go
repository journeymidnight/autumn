package streamclient

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"google.golang.org/grpc"
)

//for single stream
type StreamClient struct {
	smClient   *smclient.SMClient
	streamInfo *pb.StreamInfo
	extentInfo map[uint64]*pb.ExtentInfo
	streamID   uint64
	writeCh    chan Op
	stopper *utils.Stopper
}


type result struct {
	offset uint32
}

type Op struct {
	blocks []*pb.Block
	resultCh chan result
}

func (op Op) Size() uint32{
	ret := 0
	for i := range op.blocks {
		ret += op.blocks[i].BlockLength
	}
	return ret
}

func NewStreamClient(sm *smclient.SMClient, streamID uint64) *StreamClient {
	utils.AssertTrue(xlog.Logger != nil)
	return &StreamClient{
		smClient: sm,
		streamID: streamID,
		rand:rand.New(&lockedSource{src: rand.NewSource(time.Now().UnixNano())})
	}
}





func (sc *StreamClient) getLastExtentConn() *grpc.ClientConn{
	extentID := sc.streamInfo.ExtentIDs[len(sc.streamInfo.ExtentIDs) - 1] //last extentd
	extentInfo := sc.extentInfo[extentID]
	nodeInfo := sc.smClient.LookupNode(extentInfo.Replicates[0])

	pool := conn.GetPools().Connect(nodeInfo.Address)
	return pool.Get()
}


//TODO: 有可能需要append是幂等的
func (sc *StreamClient) streamBlocks() {
	for {
		select {
		case <- sc.stopper.ShouldStop():
			return
		case op := <- sc.writeCh:
			size := uint32(0)
			blocks := []*pb.Block{}
			ops :=  []Op{}
		slurpLoop:
			for {
				ops = append(ops, op)
				blocks = append(blocks, op.blocks...)
				size += op.Size()
				if size > (20<<20) {//batch data
					break slurpLoop
				}
				select {
				case op = <- sc.writeCh:
				default:
					break slurpLoop
				}
			}
			xlog.Logger.Debugf("len of ops is %d, write is %d", len(ops), size)
			extentID := sc.streamInfo.ExtentIDs[len(sc.streamInfo.ExtentIDs) - 1] 
			ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
			c := pb.NewExtentServiceClient(sc.getLastExtentConn())
			res, err := c.Append(ctx, &pb.AppendRequest{
				ExtentID: extentID,
				Blocks: blocks,
			})
			cancel()

			if err != nil {
				//early retry?
				
				//seal
				//allocNewExtent
				//retry..
			} else {
				i := 0
				for _, op := range ops {
					op.resultCh <- res.Offsets[i]
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

func (sc *StreamClient) Close()

//Append blocks if it meth
func (sc *StreamClient) Append(blocks []*pb.Block) (uint64, uint32) {
	id := sc.uniqueKey()
	op{

	}

}

func (sc *StreamClient) Read(extentID uint64, offset uint32, numOfBlocks uint32) {

}
