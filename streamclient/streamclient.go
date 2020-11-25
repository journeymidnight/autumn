package streamclient

import (
	"context"
	"fmt"
	"io"
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
	batchSize     = 4
	maxExtentSize = uint32(2 << 30)
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
	Offsets     []uint32
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

func (sc *StreamClient) getExtentConnFromIndex(extendIdIndex int) (*grpc.ClientConn, uint64, error) {
	sc.RLock()
	defer sc.RUnlock()
	if extendIdIndex == len(sc.streamInfo.ExtentIDs) {
		return nil, 0, io.EOF
	}

	if extendIdIndex > len(sc.streamInfo.ExtentIDs) {
		return nil, 0, errors.Errorf("extentID too big %d", extendIdIndex)
	}
	id := sc.streamInfo.ExtentIDs[extendIdIndex]
	extentInfo, ok := sc.extentInfo[id]
	if !ok {
		return nil, id, errors.Errorf("not found extentID %d", id)
	}
	nodeInfo := sc.smClient.LookupNode(extentInfo.Replicates[0])
	if nodeInfo == nil {
		return nil, id, errors.Errorf("not found node address %s", extentInfo.Replicates[0])
	}

	pool := conn.GetPools().Connect(nodeInfo.Address)
	//FIXME:
	//确保pool是healthy, 然后return
	return pool.Get(), id, nil

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

func (sc *StreamClient) mustAllocNewExtent(oldExtentID uint64) {
	var newExInfo *pb.ExtentInfo
	var err error
	for {
		//loop forever to seal and alloc new extent
		newExInfo, err = sc.smClient.StreamAllocExtent(context.Background(), sc.streamID, oldExtentID)
		if err == nil {
			break
		}
		xlog.Logger.Warn(err.Error())
		time.Sleep(100 * time.Millisecond)
	}
	sc.Lock()
	sc.streamInfo.ExtentIDs = append(sc.streamInfo.ExtentIDs, newExInfo.ExtentID)
	sc.extentInfo[newExInfo.ExtentID] = newExInfo
	sc.Unlock()
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

			//FIXME
			if err == context.DeadlineExceeded { //timeout
				sc.mustAllocNewExtent(extentID)
				/*
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
				*/
				goto retry
			} else {
				i := 0
				for _, op := range ops {
					opBlocks := len(op.blocks)
					opStart := i
					if err != nil {
						sc.completeCh <- AppendResult{Err: err, UserData: op.userData}
					} else {
						sc.completeCh <- AppendResult{extentID, res.Offsets[opStart : opStart+opBlocks], len(op.blocks), err, op.userData}

					}
					i += len(op.blocks)
				}
				//检查offset结果, 如果已经超过2GB, 调用StreamAllocExtent
				if res.Offsets[len(res.Offsets)-1] > maxExtentSize {
					sc.mustAllocNewExtent(extentID)
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

func (sc *StreamClient) GetComplete() chan AppendResult {
	return sc.completeCh
}

func (sc *StreamClient) InfightIO() int {
	return len(sc.writeCh)
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

//TODO: add urgent flag
func (sc *StreamClient) Append(ctx context.Context, blocks []*pb.Block, userData interface{}) error {
	if len(blocks) == 0 {
		return errors.Errorf("blocks can not be nil")
	}
	for i := range blocks {
		if len(blocks[i].Data)%4096 != 0 {
			return errors.Errorf("not aligned")
		}
	}
	//TODO: get op from sync.Pool
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

//Read is random read inside a block
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

type SeqReader struct {
	sc                 *StreamClient
	currentExtentIndex int
	currentOffset      uint32
}

/* Read could return
1. (blocks, io.EOF)
2. (nil, io.EOF)
3. (blocks, nil)
4. (nil, Other Error)
*/
func (reader *SeqReader) Read(ctx context.Context) ([]*pb.Block, error) {
	conn, extentID, err := reader.sc.getExtentConnFromIndex(reader.currentExtentIndex)
	if err != nil {
		return nil, err
	}
	fmt.Printf("read extentID %d, offset : %d\n", extentID, reader.currentOffset)
	//FIXME: load balance read when connecton error

	c := pb.NewExtentServiceClient(conn)
	res, err := c.ReadBlocks(ctx, &pb.ReadBlocksRequest{
		ExtentID:    extentID,
		Offset:      reader.currentOffset,
		NumOfBlocks: 8,
	})

	if err != nil {
		return nil, err
	}

	fmt.Printf("res code is %v, len of blocks %d\n", res.Code, len(res.Blocks))
	switch res.Code {
	case pb.Code_OK:
		reader.currentOffset += utils.SizeOfBlocks(res.Blocks)
		return res.Blocks, nil
	case pb.Code_EndOfExtent:
		reader.currentOffset = 512 //skip extent header
		reader.currentExtentIndex++
		return res.Blocks, nil
	case pb.Code_EndOfStream:
		return res.Blocks, io.EOF
	case pb.Code_ERROR:
		return nil, err
	default:
		return nil, errors.Errorf("unexpected error")
	}

}

//SeqRead read all the blocks in stream
func (sc *StreamClient) NewSeqReader() *SeqReader {
	/*
		sc.RLock()
		defer sc.RUnlock()
		if len(sc.streamInfo.ExtentIDs) == 0 {
			return nil, errors.Errorf("stream is nil")
		}
	*/
	return &SeqReader{
		sc:                 sc,
		currentExtentIndex: 0,
		currentOffset:      512, //skip extent header
	}
}
