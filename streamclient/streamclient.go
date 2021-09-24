/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package streamclient

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB

	HardMaxExtentSize = 3 * GB //Hard limit of extent size
)

type StreamClient interface {
	Connect() error
	Close()
	AppendEntries(ctx context.Context, entries []*pb.EntryInfo, mustSync bool) (uint64, uint32, error)
	ReadLastBlock(ctx context.Context) (*pb.Block, error)
	Append(ctx context.Context, blocks []*pb.Block, mustSync bool) (extentID uint64, offsets []uint32, end uint32, err error)
	PunchHoles(ctx context.Context, extentIDs []uint64) error
	NewLogEntryIter(opt ...ReadOption) LogEntryIter
	//truncate extent BEFORE extentID
	Truncate(ctx context.Context, extentID uint64) error
	//CommitEnd() is offset of current extent
	CommitEnd() uint32
	//total number of extents
	StreamInfo() *pb.StreamInfo
}

//random read block
type BlockReader interface {
	Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32, hint byte) ([]*pb.Block, uint32, error)
}

type LogEntryIter interface {
	HasNext() (bool, error)
	Next() *pb.EntryInfo
}

type AutumnBlockReader struct {
	em *smclient.ExtentManager
	sm *smclient.SMClient
	blockCache  *ristretto.Cache
}

func NewAutumnBlockReader(em *smclient.ExtentManager, sm *smclient.SMClient) *AutumnBlockReader {
	blockCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		panic(err)
	}
	return &AutumnBlockReader{
		em: em,
		sm: sm,
		blockCache: blockCache,
	}
}

//hint
const (
	HintReadThrough = 1 << 0
	HintReadFromCache byte = 1 << 1
)


func blockCacheID(extentID uint64, offset uint32) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], extentID)
	binary.BigEndian.PutUint32(buf[8:12], offset)
	return buf
}

type blockInCache struct {
	block *pb.Block
	readEnd uint32
}

func (br *AutumnBlockReader) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32, hint byte) ([]*pb.Block, uint32, error) {
	//only cache metadata
	if (hint & HintReadFromCache) > 0 && numOfBlocks == 1{
		data , ok := br.blockCache.Get(blockCacheID(extentID, offset))
		if ok && data != nil {
			x := data.(blockInCache)
			return []*pb.Block{x.block}, x.readEnd, nil
		}
	}

retry:
	exInfo := br.em.GetExtentInfo(extentID)
	if exInfo == nil {
		return nil, 0, errors.Errorf("no such extent")
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
		Eversion:    exInfo.Eversion,
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

	if (hint & HintReadFromCache) > 0 && numOfBlocks == 1 {
		br.blockCache.Set(blockCacheID(extentID, offset), blockInCache{block: res.Blocks[0], readEnd: res.End}, 0)
	}
	return res.Blocks, res.End, nil
}

type readOption struct {
	ReadFromStart bool
	ExtentID      uint64
	Offset       uint32
	Replay       bool
	//max extents we can read
	MaxExtentRead   int
}

type ReadOption func(*readOption)

func WithReplay() ReadOption {
	return func(opt *readOption) {
		opt.Replay = true
	}
}

func WithReadFromStart(MaxExtentRead int) ReadOption {
	//WithReadFromStart is conflict with WithReadFrom
	return func(opt *readOption) {
		opt.ReadFromStart = true
		opt.MaxExtentRead = MaxExtentRead
	}
}

func WithReadFrom(extentID uint64, offset uint32, maxExtents int) ReadOption {
	//WithReadFrom is conflict with WithReadFromStart
	return func(opt *readOption) {
		opt.ReadFromStart = false
		opt.ExtentID = extentID
		opt.Offset = offset
		opt.MaxExtentRead = maxExtents
	}
}


type StreamLock struct {
	revision int64
	ownerKey string
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
	smClient   *smclient.SMClient
	streamInfo *pb.StreamInfo

	em         *smclient.ExtentManager
	streamID   uint64
	streamLock StreamLock
	end        uint32
	maxExtentSize uint32

	utils.SafeMutex//protect streamInfo from allocStream, Truncate, PunchHole
}

func NewStreamClient(sm *smclient.SMClient, em *smclient.ExtentManager, maxExtentSize uint32, streamID uint64, streamLock StreamLock) *AutumnStreamClient {
	utils.AssertTrue(xlog.Logger != nil)

	utils.AssertTruef(maxExtentSize < HardMaxExtentSize, "maxExtentSize %d is too large", maxExtentSize)
	return &AutumnStreamClient{
		smClient:   sm,
		em:         em,
		streamID:   streamID,
		streamLock: streamLock,
		maxExtentSize: maxExtentSize,
	}
}

type AutumnEntryIter struct {
	sc                 *AutumnStreamClient
	opt                *readOption
	currentOffset      uint32
	currentExtentIndex int
	noMore             bool
	cache              []*pb.EntryInfo
	replay             bool
	conn               *grpc.ClientConn
	n                  int//number of extents we have read
}


/*
At the start of a partition load, the partition server
sends a “check for commit length” to the primary EN of the last extent of these two streams.
This checks whether all the replicas are available and that they all have the same length.
If not, the extent is sealed and reads are only performed, during partition load,
against a replica sealed by the SM
*/
func (sc *AutumnStreamClient) checkCommitLength() {
	//if last extent is not sealed, we must 'Check Commit length' for all replicates,
	//if any error happend, we seal and create a new extent
	if len(sc.streamInfo.ExtentIDs) == 0 {
		panic("sc.streamInfo can not be zero")
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, lastEx, end, err := sc.smClient.CheckCommitLength(ctx, sc.streamID, sc.streamLock.ownerKey, sc.streamLock.revision)
		xlog.Logger.Infof("CheckCommitLength %v, new extent: %v", err, lastEx)
		cancel()
		if err == nil {
			sc.em.WaitVersion(lastEx.ExtentID, lastEx.Eversion)
			sc.end = end
			break
		}
		fmt.Printf("err is %s\n\n", err.Error())
		xlog.Logger.Warnf(err.Error())
		time.Sleep(5 * time.Second)
		continue
	}

}


func (sc *AutumnStreamClient) ReadLastBlock(ctx context.Context) (*pb.Block, error) {

	if len(sc.streamInfo.ExtentIDs) == 0 {
		return nil, errors.Errorf("no extent in stream")
	}

	findValidLastExtent := func(i int) (*pb.ExtentInfo, int) {
		for ;i >= 0; i-- {
			exID, err := sc.getExtentFromIndex(i)
			if err != nil {
				return nil, -1
			}
			//extent is sealed and SealedLength is 0, skip to preivous extent
			info := sc.em.GetExtentInfo(exID)
			if info.Avali > 0 && info.SealedLength == 0 {
				continue
			}
			return info, i
		}
		return nil, -1
	}


	getLastBlock := func(exInfo *pb.ExtentInfo) ([]*pb.Block, error) {
		retry:
			exInfo = sc.em.GetExtentInfo(exInfo.ExtentID)
			if exInfo == nil {
				return nil, errors.Errorf("no such extent")
			}
			conn := sc.em.GetExtentConn(exInfo.ExtentID, smclient.AlivePolicy{})
			if conn == nil {
				return nil, errors.Errorf("unable to get extent connection.")
			}
			c := pb.NewExtentServiceClient(conn)
			res, err := c.SmartReadBlocks(ctx, &pb.ReadBlocksRequest{
				ExtentID:    exInfo.ExtentID,
				Eversion:    exInfo.Eversion,
				OnlyLastBlock: true,
			})
			if err != nil {
				return nil, err
			}
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			if err == wire_errors.VersionLow {
				sc.em.WaitVersion(exInfo.ExtentID, exInfo.Eversion+1)
				goto retry
			} else if err != nil {
				return nil, err
			}
			return res.Blocks, nil
	}

	i := len(sc.streamInfo.ExtentIDs) - 1;
	for i >= 0 {
		exInfo, idx := findValidLastExtent(i)
		if idx == -1 {
			return nil, wire_errors.NotFound			
		}

		blocks, err := getLastBlock(exInfo)
		if err == wire_errors.NotFound {
			i = idx - 1
			continue
		}
		if err != nil {
			return nil, err
		}
		
		utils.AssertTrue(len(blocks) == 1)
		return blocks[0], nil
	}
	return nil, wire_errors.NotFound	
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
		return err
	}
retry:
	for i := 0; iter.conn == nil; i++ {
		iter.conn = iter.sc.em.GetExtentConn(extentID, smclient.AlivePolicy{})
		if iter.conn == nil {
			time.Sleep(3 * time.Second)
			xlog.Logger.Warnf("retry to get connect to %d", extentID)
			if i > 10 {
				return errors.New("retries too many time to get extent connection")
			}
		}
	}

	exInfo := iter.sc.em.GetExtentInfo(extentID)
	//xlog.Logger.Debugf("read extentID %d, offset : %d, eversion is %d\n", extentID, iter.currentOffset, exInfo.Eversion)

	//fmt.Printf("read extentID %d, offset : %d,  index : %d\n from %s ", extentID,
	//iter.currentOffset, iter.currentExtentIndex, iter.conn.Target())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	c := pb.NewExtentServiceClient(iter.conn)
	res, err := c.ReadEntries(ctx, &pb.ReadEntriesRequest{
		ExtentID: extentID,
		Offset:   iter.currentOffset,
		Replay:   iter.replay,
		Eversion: exInfo.Eversion,
	})
	cancel()

	if err != nil { //network error
		if loop > 5 {
			return errors.New("finally timeout")
		}
		fmt.Printf("receiveEntries get error %s\n", err.Error())
		loop++
		time.Sleep(3 * time.Second)
		iter.conn = nil
		goto retry
	}

	if res.Code == pb.Code_EVersionLow {
		iter.sc.em.WaitVersion(extentID, exInfo.Eversion+1)
		goto retry
	}

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
		iter.n ++
		if iter.currentExtentIndex == len(iter.sc.streamInfo.ExtentIDs) || iter.n >= iter.opt.MaxExtentRead {
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
		n :  0,
	}

	if readOpt.Replay {
		leIter.replay = true
	}
	
	if readOpt.ReadFromStart {
		leIter.currentExtentIndex = 0
		leIter.currentOffset = 0
	} else {
		leIter.currentOffset = readOpt.Offset
		leIter.currentExtentIndex = sc.getExtentIndexFromID(readOpt.ExtentID)
	}
	return leIter
}


func (sc *AutumnStreamClient) PunchHoles(ctx context.Context, extentIDs []uint64) error {
	sc.Lock()
	defer sc.Unlock()
	
	updatedStream, err := sc.smClient.PunchHoles(ctx, sc.streamID, extentIDs, sc.streamLock.ownerKey, sc.streamLock.revision)
	if err != nil {
		return err
	}
	sc.streamInfo = updatedStream
	return nil
}


func (sc *AutumnStreamClient) Truncate(ctx context.Context, extentID uint64) error {
	sc.Lock()
	defer sc.Unlock()

	var i int
	for i = range sc.streamInfo.ExtentIDs {
		if sc.streamInfo.ExtentIDs[i] == extentID {
			break
		}
	}
	if i == 0 {
		return errNoTruncate
	}

	updatedStream, err := sc.smClient.TruncateStream(ctx, sc.streamID, extentID,
		sc.streamLock.ownerKey, sc.streamLock.revision)

	if err != nil {
		return  err
	}

	sc.streamInfo = updatedStream
	return nil
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

func (sc *AutumnStreamClient) CommitEnd() uint32 {
	return sc.end
}

//alloc new extent, and reset sc.end = 0
func (sc *AutumnStreamClient) MustAllocNewExtent() error {

	sc.Lock()
	defer sc.Unlock()

	var newExInfo *pb.ExtentInfo
	var err error
	var updatedStream *pb.StreamInfo
	lastExID, err := sc.getLastExtent()
	if err != nil {
		return err
	}
	fmt.Printf("Seal stream %d on extent %d: sealedLength is %d\n", sc.streamID, lastExID, sc.end)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		updatedStream, newExInfo, err = sc.smClient.StreamAllocExtent(ctx, sc.streamID,
			sc.streamLock.ownerKey, sc.streamLock.revision, sc.end)
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
	sc.streamInfo = updatedStream
	sc.em.WaitVersion(newExInfo.ExtentID, 1)
	sc.end = 0
	fmt.Printf("created new extent %d on stream %d\n", newExInfo.ExtentID, sc.streamID)
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

	sc.checkCommitLength()

	extentID := sc.streamInfo.ExtentIDs[len(sc.streamInfo.ExtentIDs)-1] //last extent
	fmt.Printf("connected :stream %d, extent %d's end is %d\n", sc.streamID, extentID, sc.end)

	return nil
}

func (sc *AutumnStreamClient) Close() {

}

//AppendEntries blocks until success
//make all entries in the same extentID, and fill entires.
func (sc *AutumnStreamClient) AppendEntries(ctx context.Context, entries []*pb.EntryInfo, mustSync bool) (uint64, uint32, error) {
	if len(entries) == 0 {
		return 0, 0, errors.Errorf("blocks can not be nil")
	}

	blocks := make([]*pb.Block, 0, len(entries))

	for _, entry := range entries {
		data := utils.MustMarshal(entry.Log)
		blocks = append(blocks, &pb.Block{
			data,
		})
	}
	extentID, offsets, tail, err := sc.Append(ctx, blocks, mustSync)
	if err != nil {
		return 0, 0, err
	}
	for i := range entries {
		entries[i].ExtentID = extentID
		entries[i].Offset = offsets[i]
	}
	return extentID, tail, err
}

func (sc *AutumnStreamClient) Append(ctx context.Context, blocks []*pb.Block, mustSync bool) (uint64, []uint32, uint32, error) {

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
		if err = sc.MustAllocNewExtent(); err != nil {
			return 0, nil, 0, err
		}
		goto retry
	}

	conn := sc.em.GetExtentConn(extentID, smclient.PrimaryPolicy{})
	if conn == nil {
		if err = sc.MustAllocNewExtent(); err != nil {
			return 0, nil, 0, err
		}
		goto retry
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15 * time.Second)
	c := pb.NewExtentServiceClient(conn)
	res, err := c.Append(ctx, &pb.AppendRequest{
		ExtentID: extentID,
		Blocks:   blocks,
		Eversion: exInfo.Eversion,
		Revision: sc.streamLock.revision,
		MustSync: mustSync,
	})
	cancel()

	if status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Unavailable { //timeout
		fmt.Printf("append on extent %d; error is %v\n", extentID, err)
		if loop < 2 {
			loop ++
			time.Sleep(100 * time.Millisecond)
			goto retry
		}
		if err = sc.MustAllocNewExtent(); err != nil {
			return 0, nil, 0, err
		}
		loop = 0 //reset loop, new extent could retry
		goto retry
	}

	if err != nil { //may have other network errors:FIXME
		return 0, nil, 0, err
	}

	if res.Code == pb.Code_EVersionLow {
		sc.em.WaitVersion(extentID, exInfo.Eversion+1)
		goto retry
	}

	//logic errors
	err = wire_errors.FromPBCode(res.Code, res.CodeDes)
	if err != nil {
		fmt.Printf("append on extent %d; error is %v\n", extentID, err)
		return 0, nil, 0, err
	}

	sc.end = res.End
	//fmt.Printf("extent %d updated end is %d\n", extentID, sc.end)
	//检查offset结果, 如果已经超过MaxExtentSize, 调用StreamAllocExtent
	utils.AssertTrue(res.End > 0)
	
	if res.End > sc.maxExtentSize {
		if err = sc.MustAllocNewExtent(); err != nil {
			return 0, nil, 0, err
		}
	}
	//update end
	return extentID, res.Offsets, res.End, nil
}

func (sc *AutumnStreamClient) StreamInfo() *pb.StreamInfo {
	//copy sc.streamInfo
	return proto.Clone(sc.streamInfo).(*pb.StreamInfo)
}

//add String() function to pb.StringInfo

func FormatEntry(entry *pb.EntryInfo) string {
	var flag string
	switch entry.Log.Meta {
	case 2:
		flag = "blob"
	case 1:
		flag = "delete"
	case 0:
		flag = "norm"
	}
	return fmt.Sprintf("%s~%d on extent %d:(%d-%d) [%s]",
	y.ParseKey(entry.Log.Key), y.ParseTs(entry.Log.Key), entry.ExtentID, entry.Offset, entry.End, flag)
}
