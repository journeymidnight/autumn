package streamclient

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
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

	MaxExtentSize     = 32 * MB
)

type StreamClient interface {
	Connect() error
	Close()
	AppendEntries(ctx context.Context, entries []*pb.EntryInfo) (uint64, uint32, error)
    Append(ctx context.Context, blocks []*pb.Block) (extentID uint64, offsets []uint32, end uint32, err error)
	NewLogEntryIter(opt ...ReadOption) LogEntryIter
	//Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, uint32, error)
	Truncate(ctx context.Context, extentID uint64, gabageKey string) (*pb.BlobStreams , error)
	//FIXME: stat => ([]extentID , offset)
	End() uint32
	CheckCommitLength() error
}

//random read block
type BlockReader interface {
	Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, uint32, error)
}

type LogEntryIter interface {
	HasNext() (bool, error)
	Next() *pb.EntryInfo
	//CheckCommitLength() error
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

func (br *AutumnBlockReader) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, uint32, error) {
retry:
	exInfo := br.em.GetExtentInfo(extentID)
	if exInfo == nil {
		return nil,  0, errors.Errorf("no such extent")
	}
	//BlockReader.Read should be a random read
	conn := br.em.GetExtentConn(extentID, smclient.AlivePolicy{})
	if conn == nil {
		return nil, 0, errors.Errorf("unable to get extent connection.")
	}
	c := pb.NewExtentServiceClient(conn)
	res, err := c.SmartReadBlocks(ctx, &pb.ReadBlocksRequest{
		ExtentID:    extentID,
		Offset:      offset,
		NumOfBlocks: numOfBlocks,
		Eversion: exInfo.Eversion ,
	})

	//network error
	if err != nil {
		return nil, 0, err
	}

	err = wire_errors.FromPBCode(res.Code, res.CodeDes)
	if err == wire_errors.VersionLow {
		br.em.WaitVersion(extentID, exInfo.Eversion+1)
		goto retry
	} else if err != nil {
		return nil, 0, err
	}

	return res.Blocks, res.End, nil
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

type StreamLock struct {
	revision int64
	ownerKey  string
}

func MutexToLock(mutex *concurrency.Mutex) StreamLock {
	return StreamLock{
		revision: mutex.Header().Revision,
		ownerKey: mutex.Key(),
	}
}

//for single stream
type AutumnStreamClient struct {
	StreamClient
	smClient     *smclient.SMClient
	streamInfo   *pb.StreamInfo

	em       *smclient.ExtentManager
	streamID uint64
	streamLock StreamLock
	end uint32
}

func NewStreamClient(sm *smclient.SMClient, em *smclient.ExtentManager, streamID uint64, streamLock StreamLock) *AutumnStreamClient {
	utils.AssertTrue(xlog.Logger != nil)
	return &AutumnStreamClient{
		smClient: sm,
		em:       em,
		streamID: streamID,
		streamLock: streamLock,
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
	conn               *grpc.ClientConn
}


func (sc *AutumnStreamClient) 	CheckCommitLength() error {
	//if last extent is not sealed, we must 'Check Commit length' for all replicates,
	//if any error happend, we seal and create a new extent
	if len(sc.streamInfo.ExtentIDs) == 0 {
		return nil
	}
	extentID := sc.streamInfo.ExtentIDs[len(sc.streamInfo.ExtentIDs)-1] //last extent
	exInfo := sc.em.Latest(extentID) 

	if exInfo.Avali > 0 {
		return nil
	}

	xlog.Logger.Infof("Check Commit Length at extent %d", extentID)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
		newExInfo, lastExtentEnd,  err := sc.smClient.StreamAllocExtent(ctx, sc.streamID, int64(sc.streamInfo.Sversion), extentID, 
			sc.streamLock.ownerKey, sc.streamLock.revision, 1, 0)
		
		xlog.Logger.Infof("Check Commit Length %v, new extent: %v", err, newExInfo)
		cancel()
		if err == nil {
			if newExInfo != nil {
				//update local streaminfo
				sc.streamInfo.ExtentIDs = append(sc.streamInfo.ExtentIDs, newExInfo.ExtentID)
				sc.em.WaitVersion(newExInfo.ExtentID, 1)
				sc.end = 0
			} else {
				sc.end = lastExtentEnd
			}
			break
		}
		fmt.Printf("err is %s\n\n", err.Error())
		xlog.Logger.Warnf(err.Error())
		time.Sleep(5 * time.Second)
		continue
		
	}
	return nil

} 
func (iter *AutumnEntryIter) HasNext() (bool, error) {
	if len(iter.cache) == 0 {
		if iter.noMore {
			return false, nil
		} 
		for iter.noMore == false && len(iter.cache) == 0 {
			err := iter.receiveEntries()
			if err != nil {
				return false, err
			}
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


	extentID, err := iter.sc.getExtentFromIndex(iter.currentExtentIndex)
	if err != nil {
		//utils.AssertTrue(!iter.noMore)
		return err
	}
retry:
	for loop := 0 ; iter.conn == nil ; loop ++ {
		iter.conn = iter.sc.em.GetExtentConn(extentID, smclient.AlivePolicy{})
		if iter.conn == nil {
			time.Sleep(3*time.Second)
			xlog.Logger.Warnf("retry to get connect to %d", extentID)
			if loop > 100 {
				return errors.New("retries too many time to get extent connection")
			}
		}
	}

	exInfo := iter.sc.em.GetExtentInfo(extentID)
	//xlog.Logger.Debugf("read extentID %d, offset : %d, eversion is %d\n", extentID, iter.currentOffset, exInfo.Eversion)

	//fmt.Printf("read extentID %d, offset : %d,  index : %d\n", extentID, iter.currentOffset, iter.currentExtentIndex)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	c := pb.NewExtentServiceClient(iter.conn)
	res, err := c.ReadEntries(ctx, &pb.ReadEntriesRequest{
		ExtentID: extentID,
		Offset:   iter.currentOffset,
		Replay:   uint32(iter.replay),
		Eversion: exInfo.Eversion,
	})
	cancel()


	if err != nil { //network error
		if loop > 5 {
			return errors.New("finally timeout")
		}
		loop++
		time.Sleep(3 * time.Second)
		iter.conn = nil
		goto retry
	}

	if res.Code == pb.Code_EVersionLow {
		iter.sc.em.WaitVersion(extentID, exInfo.Eversion+1)
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
		iter.conn = nil
		//如果stream是BlobStream, 最后一个extent也返回EndOfExtent
		if iter.currentExtentIndex == len(iter.sc.streamInfo.ExtentIDs) {
			iter.noMore = true
		}
		fmt.Printf("readentries currentIndex is %d in %v nomore?%v\n", iter.currentExtentIndex, iter.sc.streamInfo.ExtentIDs, iter.noMore)

		return nil
	default:
		return errors.Errorf(res.CodeDes)
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

//Truncate keeps extentID, if gabageKey is not nil, move all extents before extentID to gabageKey, else
//unref all extents.
func (sc *AutumnStreamClient) Truncate(ctx context.Context, extentID uint64, gabageKey string) (*pb.BlobStreams, error) {

	var i int
	for i = range sc.streamInfo.ExtentIDs {
		if sc.streamInfo.ExtentIDs[i] == extentID {
			break
		}
	}
	if i == 0 {
		return nil, errNoTruncate
	}

	return sc.smClient.TruncateStream(ctx, sc.streamID, extentID, 
		sc.streamLock.ownerKey, sc.streamLock.revision, int64(sc.streamInfo.Sversion), gabageKey)
}

func (sc *AutumnStreamClient) getExtentIndexFromID(extentID uint64) int {

	for i := range sc.streamInfo.ExtentIDs {
		if extentID == sc.streamInfo.ExtentIDs[i] {
			return i
		}
	}
	return -1
}

func (sc *AutumnStreamClient) getExtentFromIndex(extendIdIndex int) (uint64, error) {

	if extendIdIndex >= len(sc.streamInfo.ExtentIDs) {
		return 0, errors.Errorf("extentID too big %d", extendIdIndex)
	}

	id := sc.streamInfo.ExtentIDs[extendIdIndex]
	return id, nil
}

func (sc *AutumnStreamClient) getLastExtent() (uint64, error) {

	if sc.streamInfo == nil || len(sc.streamInfo.ExtentIDs) == 0 {
		return 0, errors.New("no streamInfo or streamInfo is not correct")
	}
	extentID := sc.streamInfo.ExtentIDs[len(sc.streamInfo.ExtentIDs)-1] //last extent
	return extentID, nil
}

func (sc *AutumnStreamClient) End() uint32 {
	return sc.end
}

//alloc new extent, and reset sc.end = 0
func (sc *AutumnStreamClient) MustAllocNewExtent(oldExtentID uint64) error{
	var newExInfo *pb.ExtentInfo
	var err error
	for {
		ctx , cancel := context.WithTimeout(context.Background(), 5 * time.Second)
		newExInfo, _, err = sc.smClient.StreamAllocExtent(ctx, sc.streamID, int64(sc.streamInfo.Sversion),
		oldExtentID , sc.streamLock.ownerKey, sc.streamLock.revision, 0, sc.end)
		cancel()
		if err == nil {
			break
		}
		xlog.Logger.Errorf(err.Error())
		if err == smclient.ErrTimeOut {
			time.Sleep(5 * time.Second)
			continue
		}
		return err
		
	}
	sc.streamInfo.ExtentIDs = append(sc.streamInfo.ExtentIDs, newExInfo.ExtentID)

	sc.em.WaitVersion(newExInfo.ExtentID, 1)
	sc.end = 0
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

	//wait to get the latest extentInfo for future write.
	exID, _ := sc.getLastExtent()
	exInfo := sc.em.Latest(exID)
	sc.em.WaitVersion(exID, exInfo.Eversion)

	fmt.Printf("stream info is %d : %v\n", sc.streamInfo.StreamID, sc.streamInfo.ExtentIDs)
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
	extentID, offsets , tail, err := sc.Append(ctx, blocks)
	if err != nil {
		return 0, 0, err
	}
	for i := range entries {
		entries[i].ExtentID = extentID
		entries[i].Offset = offsets[i]
	}
    return extentID, tail, err
}


func (sc *AutumnStreamClient) Append(ctx context.Context, blocks []*pb.Block) (uint64, []uint32, uint32,  error) {
	loop := 0
retry:

	extentID, err := sc.getLastExtent()
	if err != nil {
		xlog.Logger.Error(err)
		return 0, nil, 0, err
	}
	exInfo := sc.em.GetExtentInfo(extentID)
	if exInfo == nil {
		return extentID, nil, 0, errors.New("not such extent")
	}

	if exInfo.Avali > 0 {
		if err = sc.MustAllocNewExtent(extentID) ; err != nil {
			return 0, nil, 0, err
		}
		goto retry
	}

	conn := sc.em.GetExtentConn(extentID, smclient.PrimaryPolicy{})
	if conn == nil {
		if err = sc.MustAllocNewExtent(extentID) ; err != nil {
			return 0, nil, 0, err
		}
		goto retry

	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	c := pb.NewExtentServiceClient(conn)
	res, err := c.Append(ctx, &pb.AppendRequest{
		ExtentID: extentID,
		Blocks:   blocks,
		Eversion: exInfo.Eversion,
		Revision: sc.streamLock.revision,
	})
	cancel()

	
	if status.Code(err) == codes.DeadlineExceeded{ //timeout
		if loop < 3 {
			loop ++
			goto retry
		}
		err = sc.MustAllocNewExtent(extentID)
	}


	if err != nil {//may have other network errors
		return 0, nil, 0, err
	}

	if res.Code == pb.Code_EVersionLow {
		sc.em.WaitVersion(extentID, exInfo.Eversion+1)
		goto retry
	}
	
	//logic errors
	err = wire_errors.FromPBCode(res.Code, res.CodeDes)
	if err != nil {
		return 0, nil, 0, err
	}

	//检查offset结果, 如果已经超过2GB, 调用StreamAllocExtent
	utils.AssertTrue(res.End > 0)
	if res.End > MaxExtentSize {
		if err = sc.MustAllocNewExtent(extentID); err != nil {
			return 0, nil, 0, err
		}
	} else {
		sc.end = res.End
	}
	//update end
	return extentID, res.Offsets,res.End, nil
}