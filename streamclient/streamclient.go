package streamclient

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

/*
At the start of a partition load, the partition server
sends a “check for commit length” to the primary EN of the last extent of these two streams.
This checks whether all the replicas are available and that they all have the same length.
If not, the extent is sealed and reads are only performed, during partition load,
against a replica sealed by the SM
*/

const (
	KB             = 1024
	MB             = 1024 * KB
	GB             = 1024 * MB
	ValueThreshold = 4 * KB

	MaxMixedBlockSize = 4 * KB
	MaxExtentSize     = 2 * GB
	MaxEntriesInBlock = 100
)

type StreamClient interface {
	Connect() error
	Close()
	AppendEntries(ctx context.Context, entries []*pb.EntryInfo) (uint64, uint32, error)
    Append(ctx context.Context, blocks []*pb.Block) (extentID uint64, offsets []uint32, end uint32, err error)
	NewLogEntryIter(opt ReadOption) LogEntryIter
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
	em *AutumnExtentManager
	sm *smclient.SMClient
}

func NewAutumnBlockReader(em *AutumnExtentManager, sm *smclient.SMClient) *AutumnBlockReader {
	return &AutumnBlockReader{
		em: em,
		sm: sm,
	}
}

func (br *AutumnBlockReader) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, error) {
	conn := br.em.GetExtentConn(extentID)
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

type AutumnExtentManager struct {
	sync.RWMutex
	smClient   *smclient.SMClient
	extentInfo map[uint64]*pb.ExtentInfo
}

func NewAutomnExtentManager(sm *smclient.SMClient) *AutumnExtentManager {
	return &AutumnExtentManager{
		smClient:   sm,
		extentInfo: make(map[uint64]*pb.ExtentInfo),
	}
}

func (em *AutumnExtentManager) GetPeers(extentID uint64) []string {

	extentInfo := em.GetExtentInfo(extentID)

	var ret []string
	for _, id := range extentInfo.Replicates {
		n := em.smClient.LookupNode(id)
		utils.AssertTrue(n != nil)
		ret = append(ret, n.Address)
	}
	return ret
}

//always get alive node: FIXME:确保pool是healthy
func (em *AutumnExtentManager) GetExtentConn(extentID uint64) *grpc.ClientConn {
	extentInfo := em.GetExtentInfo(extentID)
	nodeInfo := em.smClient.LookupNode(extentInfo.Replicates[0])
	pool := conn.GetPools().Connect(nodeInfo.Address)
	return pool.Get()
}

func (em *AutumnExtentManager) GetExtentInfo(extentID uint64) *pb.ExtentInfo {
	em.RLock()
	info, ok := em.extentInfo[extentID]
	em.RUnlock()
	if !ok {
		//lazy receive data extentData, must success:FIXME: add new function to prefetch ExtentInfo
		var ei *pb.ExtentInfo
		for {
			m, err := em.smClient.ExtentInfo(context.Background(), []uint64{extentID})
			if err == nil {
				ei = proto.Clone(m[extentID]).(*pb.ExtentInfo)
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		em.Lock()
		em.extentInfo[extentID] = ei
		em.Unlock()
		info = ei

	}
	return info
}

func (em *AutumnExtentManager) SetExtentInfo(extentID uint64, info *pb.ExtentInfo) {
	em.Lock()
	defer em.Unlock()
	em.extentInfo[extentID] = info
}

type ReadOption struct {
	ReadFromStart bool
	ExtentID      uint64
	Offset        uint32
	Replay        bool
}

func (opt ReadOption) WithReplay() ReadOption {
	opt.Replay = true
	return opt
}
func (opt ReadOption) WithReadFromStart() ReadOption {
	opt.ReadFromStart = true
	return opt
}
func (opt ReadOption) WithReadFrom(extentID uint64, offset uint32) ReadOption {
	opt.ReadFromStart = false
	opt.ExtentID = extentID
	opt.Offset = offset
	return opt
}

//for single stream
type AutumnStreamClient struct {
	smClient     *smclient.SMClient
	sync.RWMutex //protect streamInfo/extentInfo when called in read/write
	streamInfo   *pb.StreamInfo
	//FIXME: move extentInfo output StreamClient
	//extentInfo  map[uint64]*pb.ExtentInfo
	em       *AutumnExtentManager
	streamID uint64
}

func NewStreamClient(sm *smclient.SMClient, em *AutumnExtentManager, streamID uint64) *AutumnStreamClient {
	utils.AssertTrue(xlog.Logger != nil)
	return &AutumnStreamClient{
		smClient: sm,
		em:       em,
		streamID: streamID,
	}
}

type AutumnLogEntryIter struct {
	sc                 *AutumnStreamClient
	opt                ReadOption
	currentOffset      uint32
	currentExtentIndex int
	noMore             bool
	cache              []*pb.EntryInfo
	replay             uint32
}

func (iter *AutumnLogEntryIter) HasNext() (bool, error) {
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

func (iter *AutumnLogEntryIter) Next() *pb.EntryInfo {
	if ok, err := iter.HasNext(); !ok || err != nil {
		return nil
	}
	ret := iter.cache[0]
	iter.cache = iter.cache[1:]
	return ret
}

func (iter *AutumnLogEntryIter) receiveEntries() error {
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

func (sc *AutumnStreamClient) NewLogEntryIter(opt ReadOption) LogEntryIter {
	leIter := &AutumnLogEntryIter{
		sc:  sc,
		opt: opt,
	}
	if opt.Replay {
		leIter.replay = 1
	}
	if opt.ReadFromStart {
		leIter.currentExtentIndex = 0
		leIter.currentOffset = 0
	} else {
		leIter.currentOffset = opt.Offset
		leIter.currentExtentIndex = sc.getExtentIndexFromID(opt.ExtentID)
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
	conn := sc.em.GetExtentConn(id)
	if conn == nil {
		return nil, id, errors.Errorf("not found extentID %d", id)
	}
	return conn, id, nil
}

func (sc *AutumnStreamClient) getLastExtentConn() (uint64, *grpc.ClientConn) {
	sc.RLock()
	defer sc.RUnlock()
	extentID := sc.streamInfo.ExtentIDs[len(sc.streamInfo.ExtentIDs)-1] //last extentd

	return extentID, sc.em.GetExtentConn(extentID)
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
	sc.Unlock()

	sc.em.SetExtentInfo(newExInfo.ExtentID, newExInfo)
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
	conn := sc.em.GetExtentConn(extentID)
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
	extentID, conn := sc.getLastExtentConn()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	c := pb.NewExtentServiceClient(conn)
	res, err := c.Append(ctx, &pb.AppendRequest{
		ExtentID: extentID,
		Blocks:   blocks,
		Peers:    sc.em.GetPeers(extentID), //sc.getPeers(extentID),
	})
	cancel()

	
	if status.Code(err) == codes.DeadlineExceeded{//timeout
		if loop < 3 {
			loop ++
			goto retry
		}
		sc.mustAllocNewExtent(extentID)
		loop = 0
		goto retry
	}
	if err != nil {//other errors
		return 0,nil,0, err
	}
	//检查offset结果, 如果已经超过2GB, 调用StreamAllocExtent
	
	utils.AssertTrue(res.End > 0)
	if res.End > MaxExtentSize {
		sc.mustAllocNewExtent(extentID)
	}
	return extentID, res.Offsets,res.End, nil

}