package streamclient

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

//read from start
//or
//read from offset

//read critical blocks
//read all blockss

//有2个要决定, 1, 从stream哪里开始, 2, 是否只读关键blocks,是否读关键blocks
//如果当前是关键Block
//就读出block_meta + block_data(全量)
//如果不是关键block
//就只读出block_meta

type ReaderOption struct {
	ReadFromStart bool
	OnlyCritical  bool
	ExtentID      uint64
	Offset        uint32
}

func (opt ReaderOption) WithReadFromStart() ReaderOption {
	opt.ReadFromStart = true
	return opt
}

func (opt ReaderOption) WithReadFrom(extentID uint64, offset uint32) ReaderOption {
	opt.ReadFromStart = false
	opt.ExtentID = extentID
	opt.Offset = offset
	return opt
}

func (opt ReaderOption) WithOnlyCritical(a bool) {
	opt.OnlyCritical = a
}

type SeqReader interface {
	Read(ctx context.Context) ([]*pb.Block, error)
}

type StreamClient interface {
	Connect() error
	Close()
	Append(ctx context.Context, blocks []*pb.Block, userData interface{}) (*Op, error)
	Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, error)
	NewSeqReader(opt ReaderOption) SeqReader
}

//for single stream
type AutumnStreamClient struct {
	smClient     *smclient.SMClient
	sync.RWMutex //protect streamInfo/extentInfo when called in read/write
	streamInfo   *pb.StreamInfo
	extentInfo   map[uint64]*pb.ExtentInfo
	streamID     uint64
	writeCh      chan *Op
	stopper      *utils.Stopper
	iodepth      int
	done         int32
}

var (
	maxExtentSize = uint32(2 << 30)
	opPool        = sync.Pool{
		New: func() interface{} {
			return new(Op)
		},
	}
)

func NewStreamClient(sm *smclient.SMClient, streamID uint64, iodepth int) *AutumnStreamClient {
	utils.AssertTrue(xlog.Logger != nil)
	return &AutumnStreamClient{
		smClient: sm,
		streamID: streamID,
		writeCh:  make(chan *Op, iodepth),
		stopper:  utils.NewStopper(),
		iodepth:  iodepth,
		done:     0,
	}
}

type Op struct {
	blocks []*pb.Block
	wg     sync.WaitGroup

	//
	UserData interface{}

	//return value
	ExtentID uint64
	Offsets  []uint32
	Err      error
	done     int32
}

func (op *Op) IsComplete() bool {
	return atomic.LoadInt32(&op.done) == 1
}

func (op *Op) Reset(blocks []*pb.Block, userData interface{}) {
	op.blocks = blocks
	op.UserData = userData
	op.wg = sync.WaitGroup{}
	op.wg.Add(1)
	op.done = 0
}
func (op *Op) Free() {
	op.blocks = nil
	op.UserData = nil
	opPool.Put(op)
}
func (op *Op) Wait() {
	op.wg.Wait()
}

func (op *Op) Size() uint32 {
	ret := uint32(0)
	for i := range op.blocks {
		ret += op.blocks[i].BlockLength
	}
	return ret
}

func (sc *AutumnStreamClient) getPeers(extentID uint64) []string {
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

func (sc *AutumnStreamClient) getExtentConnFromIndex(extendIdIndex int) (*grpc.ClientConn, uint64, error) {
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
		return nil, id, errors.Errorf("not found node address %d", extentInfo.Replicates[0])
	}

	pool := conn.GetPools().Connect(nodeInfo.Address)
	//FIXME:
	//确保pool是healthy, 然后return
	return pool.Get(), id, nil

}

func (sc *AutumnStreamClient) getExtentConn(extentID uint64) *grpc.ClientConn {
	sc.RLock()
	defer sc.RUnlock()
	extentInfo := sc.extentInfo[extentID]
	nodeInfo := sc.smClient.LookupNode(extentInfo.Replicates[0])

	pool := conn.GetPools().Connect(nodeInfo.Address)
	return pool.Get()
}

func (sc *AutumnStreamClient) getLastExtentConn() (uint64, *grpc.ClientConn) {
	sc.RLock()
	defer sc.RUnlock()
	extentID := sc.streamInfo.ExtentIDs[len(sc.streamInfo.ExtentIDs)-1] //last extentd
	extentInfo := sc.extentInfo[extentID]
	nodeInfo := sc.smClient.LookupNode(extentInfo.Replicates[0])

	pool := conn.GetPools().Connect(nodeInfo.Address)
	return extentID, pool.Get()
}

func (sc *AutumnStreamClient) mustAllocNewExtent(oldExtentID uint64) {
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
func (sc *AutumnStreamClient) streamBlocks() {
	for {
		select {
		case <-sc.stopper.ShouldStop():
			for op := range sc.writeCh {
				op.Err = errors.Errorf("closed")
				atomic.StoreInt32(&op.done, 1)
				op.wg.Done()
			}
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
				goto retry
			} else {
				i := 0
				for _, op := range ops {
					opBlocks := len(op.blocks)
					opStart := i

					op.Err = err
					op.ExtentID = extentID
					op.Offsets = res.Offsets[opStart : opStart+opBlocks]
					atomic.StoreInt32(&op.done, 1)
					op.wg.Done()
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

func (sc *AutumnStreamClient) Connect() error {
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

func (sc *AutumnStreamClient) Close() {
	atomic.StoreInt32(&sc.done, 1)
	close(sc.writeCh)
	sc.stopper.Stop()
}

//TODO: add urgent flag
func (sc *AutumnStreamClient) Append(ctx context.Context, blocks []*pb.Block, userData interface{}) (*Op, error) {

	if atomic.LoadInt32(&sc.done) == 1 {
		return nil, errors.Errorf("closed")
	}

	if len(blocks) == 0 {
		return nil, errors.Errorf("blocks can not be nil")
	}
	for i := range blocks {
		if len(blocks[i].Data)%512 != 0 {
			return nil, errors.Errorf("not aligned")
		}
	}

	op := opPool.Get().(*Op)
	op.Reset(blocks, userData)

	sc.writeCh <- op
	return op, nil
}

//Read is random read inside a block
func (sc *AutumnStreamClient) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, error) {
	if atomic.LoadInt32(&sc.done) == 1 {
		return nil, errors.Errorf("closed")
	}
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

type AutumnSeqReader struct {
	sc                 *AutumnStreamClient
	currentExtentIndex int
	currentOffset      uint32
}

/* Read could return
1. (blocks, io.EOF)
2. (nil, io.EOF)
3. (blocks, nil)
4. (nil, Other Error)
*/
func (reader *AutumnSeqReader) Read(ctx context.Context) ([]*pb.Block, error) {
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
func (sc *AutumnStreamClient) NewSeqReader(opt ReaderOption) SeqReader {
	return &AutumnSeqReader{
		sc:                 sc,
		currentExtentIndex: 0,
		currentOffset:      512, //skip extent header
	}
}
