package streamclient

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*
At the start of a partition load, the partition server
sends a “check for commit length” to the primary EN of the last extent of these two streams.
This checks whether all the replicas are available and that they all have the same length.
If not, the extent is sealed and reads are only performed, during partition load,
against a replica sealed by the SM

相当于hdfs的lease recovery
*/

const (
	KB             = 1024
	MB             = 1024 * KB
	GB             = 1024 * MB
	ValueThreshold = 4 * KB

	MaxExtentSize     = 2 * GB
)

type StreamClient interface {
	Connect() error
	Close()
	AppendEntries(ctx context.Context, entries []*pb.EntryInfo) (uint64, uint32, error)
    Append(ctx context.Context, blocks []*pb.Block) (extentID uint64, offsets []uint32, end uint32, err error)
	NewLogEntryIter(opt ...ReadOption) LogEntryIter
	Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, uint32, error)
	Truncate(ctx context.Context, extentID uint64) (pb.StreamInfo, pb.StreamInfo, error)
	//FIXME: stat => ([]extentID , offset)
}

//random read block
type BlockReader interface {
	Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, uint32, error)
}

type LogEntryIter interface {
	HasNext() (bool, error)
	Next() *pb.EntryInfo
}

type AutumnBlockReader struct {
	em *smclient.ExtentManager
	sm *smclient.SMClient
}

func NewAutumnBlockReader(em *smclient.ExtentManager, sm *smclient.SMClient) *AutumnBlockReader {
	return &AutumnBlockReader{
		em: em,
		sm: sm,
	}
}

func (br *AutumnBlockReader) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, error) {
	exInfo := br.em.GetExtentInfo(extentID)
	if exInfo == nil {
		return nil,  errors.Errorf("no such extent")
	}
	conn := br.em.GetExtentConn(extentID, smclient.PrimaryPolicy{})
	if conn == nil {
		return nil, errors.Errorf("unable to get extent connection.")
	}
	c := pb.NewExtentServiceClient(conn)
	res, err := c.SmartReadBlocks(ctx, &pb.ReadBlocksRequest{
		ExtentID:    extentID,
		Offset:      offset,
		NumOfBlocks: numOfBlocks,
		Eversion: exInfo.Eversion ,
	})
	
	return res.Blocks, err
}



type readOption struct {
	ReadFromStart bool
	ExtentID      uint64
	Offset        uint32
	Replay        bool
}

type ReadOption func(*readOption)

func WithReplay() ReadOption {
	return func(opt *readOption) {
		opt.Replay = true
	}
}
func WithReadFromStart() ReadOption {
	return func(opt *readOption) {
		opt.ReadFromStart = true
	}
}

func WithReadFrom(extentID uint64, offset uint32) ReadOption {
	return func(opt *readOption) {
		opt.ReadFromStart = false
		opt.ExtentID = extentID
		opt.Offset = offset
	}
}

//for single stream
type AutumnStreamClient struct {
	StreamClient
	smClient     *smclient.SMClient
	sync.RWMutex //protect streamInfo/extentInfo when called in read/write
	streamInfo   *pb.StreamInfo
	//FIXME: move extentInfo output StreamClient
	//extentInfo  map[uint64]*pb.ExtentInfo
	em       *smclient.ExtentManager
	streamID uint64
}

func NewStreamClient(sm *smclient.SMClient, em *smclient.ExtentManager, streamID uint64) *AutumnStreamClient {
	utils.AssertTrue(xlog.Logger != nil)
	return &AutumnStreamClient{
		smClient: sm,
		em:       em,
		streamID: streamID,
	}
}

type AutumnEntryIter struct {
	sc                 *AutumnStreamClient
	opt                *readOption
	currentOffset      uint32
	currentExtentIndex int
	noMore             bool
	cache              []*pb.EntryInfo
	replay             uint32
}

func (iter *AutumnEntryIter) HasNext() (bool, error) {
	if len(iter.cache) == 0 {
		if iter.noMore {
			return false, nil
		}
		err := iter.receiveEntries()
		if err != nil {
			return false, err
		}
	}
	return len(iter.cache) > 0, nil
}

func (iter *AutumnEntryIter) Next() *pb.EntryInfo {
	if ok, err := iter.HasNext(); !ok || err != nil {
		return nil
	}
	ret := iter.cache[0]
	iter.cache = iter.cache[1:]
	return ret
}

func (iter *AutumnEntryIter) receiveEntries() error {
	loop := 0
retry:
	conn, extentID, err := iter.sc.getExtentConnFromIndex(iter.currentExtentIndex)
	if err != nil {
		//utils.AssertTrue(!iter.noMore)
		return err
	}

	xlog.Logger.Debugf("read extentID %d, offset : %d\n", extentID, iter.currentOffset)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	c := pb.NewExtentServiceClient(conn)
	res, err := c.ReadEntries(ctx, &pb.ReadEntriesRequest{
		ExtentID: extentID,
		Offset:   iter.currentOffset,
		Replay:   uint32(iter.replay),
	})
	cancel()
	//if err == context.DeadlineExceeded {
	if err != nil {
		if loop > 5 {
			return errors.New("finally timeout")
		}
		loop++
		time.Sleep(1 * time.Second)
		goto retry
	}

	//fmt.Printf("ans: %v, %v\n", err, err == context.DeadlineExceeded)
	//xlog.Logger.Debugf("res code is %v, len of entries %d\n", res.Code, len(res.Entries))
	if len(res.Entries) > 0 {
		iter.cache = nil
		iter.cache = append(iter.cache, res.Entries...)
	}

	switch res.Code {
	case pb.Code_OK:
		iter.currentOffset = res.End
		return nil
	case pb.Code_EndOfExtent:
		iter.currentOffset = 0
		iter.currentExtentIndex++
		//如果stream是BlobStream, 最后一个extent也返回EndOfExtent
		if iter.currentExtentIndex == len(iter.sc.streamInfo.ExtentIDs) {
			iter.noMore = true
		}
		return nil
	case pb.Code_EndOfStream:
		iter.noMore = true
		return nil
	case pb.Code_ERROR:
		return err
	default:
		return errors.Errorf("unexpected error")
	}

}

func (sc *AutumnStreamClient) NewLogEntryIter(opts ...ReadOption) LogEntryIter {
	readOpt := &readOption{}
	for _, opt := range opts {
		opt(readOpt)
	}
	leIter := &AutumnEntryIter{
		sc:  sc,
		opt: readOpt,
	}
	if readOpt.Replay {
		leIter.replay = 1
	}
	if readOpt.ReadFromStart {
		leIter.currentExtentIndex = 0
		leIter.currentOffset = 0
	} else {
		leIter.currentOffset = readOpt.Offset
		leIter.currentExtentIndex = sc.getExtentIndexFromID(readOpt.ExtentID)
		/*
			if leIter.currentExtentIndex < 0 {
				return nil, errors.Errorf("can not find extentID %d in stream %d", opt.ExtentID, sc.streamID)
			}
		*/
	}
	return leIter
}

func (sc *AutumnStreamClient) Truncate(ctx context.Context, extentID uint64) (pb.StreamInfo, pb.StreamInfo, error) {
	sc.Lock()
	defer sc.Unlock()
	var i int
	for i = range sc.streamInfo.ExtentIDs {
		if sc.streamInfo.ExtentIDs[i] == extentID {
			break
		}
	}
	if i == 0 {
		return pb.StreamInfo{}, pb.StreamInfo{}, errNoTrucate
	}
	/*
		sc.streamInfo.ExtentIDs = sc.streamInfo.ExtentIDs[i:]
		return sc.smClient.TruncateStream(ctx, sc.streamID, sc.streamInfo.ExtentIDs)
	*/
	//FIXME:
	return pb.StreamInfo{}, pb.StreamInfo{}, errNoTrucate
}

func (sc *AutumnStreamClient) getExtentIndexFromID(extentID uint64) int {
	sc.RLock()
	defer sc.RUnlock()
	for i := range sc.streamInfo.ExtentIDs {
		if extentID == sc.streamInfo.ExtentIDs[i] {
			return i
		}
	}
	return -1
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
	conn := sc.em.GetExtentConn(id, smclient.PrimaryPolicy{})
	if conn == nil {
		return nil, id, errors.Errorf("not found extentID %d", id)
	}
	return conn, id, nil
}

func (sc *AutumnStreamClient) getLastExtentConn() (uint64, *grpc.ClientConn, error) {
	sc.RLock()
	defer sc.RUnlock()
	if sc.streamInfo == nil || len(sc.streamInfo.ExtentIDs) == 0 {
		return 0, nil, errors.New("no streamInfo or streamInfo is not correct")
	}
	extentID := sc.streamInfo.ExtentIDs[len(sc.streamInfo.ExtentIDs)-1] //last extent
	conn := sc.em.GetExtentConn(extentID, smclient.PrimaryPolicy{})
	if conn == nil {
		return 0, nil, fmt.Errorf("can not get extent connection with id: %d", extentID)
	}
	return extentID, conn, nil
}

func (sc *AutumnStreamClient) MustAllocNewExtent(oldExtentID uint64, dataShard, parityShard uint32) error{
	var newExInfo *pb.ExtentInfo
	var err error
	for i := 0 ; i < 10 ; i ++{
		newExInfo, err = sc.smClient.StreamAllocExtent(context.Background(), sc.streamID, oldExtentID, dataShard, parityShard)
		if err == nil {
			break
		}
		xlog.Logger.Warn(err.Error())
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	sc.Lock()
	sc.streamInfo.ExtentIDs = append(sc.streamInfo.ExtentIDs, newExInfo.ExtentID)
	sc.Unlock()

	sc.em.SetExtentInfo(newExInfo.ExtentID, newExInfo)
	xlog.Logger.Debugf("created new extent %d on stream %d", newExInfo.ExtentID, sc.streamID)
	return nil
}

func (sc *AutumnStreamClient) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	s, _, err := sc.smClient.StreamInfo(ctx, []uint64{sc.streamID})
	cancel()
	if err != nil {
		return err
	}
	sc.streamInfo = s[sc.streamID]
	return nil
}

func (sc *AutumnStreamClient) Close() {

}

//AppendEntries blocks until success
//make all entries in the same extentID, and fill entires.
func (sc *AutumnStreamClient) AppendEntries(ctx context.Context, entries []*pb.EntryInfo) (uint64, uint32, error) {
	if len(entries) == 0 {
		return 0, 0, errors.Errorf("blocks can not be nil")
	}

	blocks := make([]*pb.Block,0, len(entries))

    for _, entry := range entries {
               data := utils.MustMarshal(entry.Log)
               blocks = append(blocks,  &pb.Block{
                       data,
               })
    }
	extentID, _, tail, err := sc.Append(ctx, blocks)
    return extentID, tail, err
}

func (sc *AutumnStreamClient) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, error) {
	conn := sc.em.GetExtentConn(extentID, smclient.PrimaryPolicy{})
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

func (sc *AutumnStreamClient) Append(ctx context.Context, blocks []*pb.Block) (uint64, []uint32, uint32,  error) {
	loop := 0
retry:
	extentID, conn, err := sc.getLastExtentConn()
	if err != nil {
		return 0, nil, 0, err
	}
	exInfo := sc.em.GetExtentInfo(extentID)
	if exInfo == nil {
		return extentID, nil, 0, errors.New("not such extent")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	c := pb.NewExtentServiceClient(conn)
	res, err := c.Append(ctx, &pb.AppendRequest{
		ExtentID: extentID,
		Blocks:   blocks,
		Eversion: exInfo.Eversion,
	})
	cancel()

	
	if status.Code(err) == codes.DeadlineExceeded{ //timeout
		if loop < 3 {
			loop ++
			goto retry
		}
		sc.MustAllocNewExtent(extentID, uint32(len(exInfo.Replicates)), uint32(len(exInfo.Parity)))
		loop = 0
		goto retry
	}
	if err != nil {//other errors
		return 0,nil,0, err
	}
	//检查offset结果, 如果已经超过2GB, 调用StreamAllocExtent
	
	utils.AssertTrue(res.End > 0)
	if res.End > MaxExtentSize {
		
		if err := sc.MustAllocNewExtent(extentID, uint32(len(exInfo.Replicates)), uint32(len(exInfo.Parity))); err != nil {
			return 0,nil,0, err
		}
	}
	return extentID, res.Offsets,res.End, nil

}