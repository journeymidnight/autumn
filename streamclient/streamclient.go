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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/erasure_code"
	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB

	HardMaxExtentSize = 3 * GB //Hard limit of extent size
)

type block = []byte

type StreamClient interface {
	Connect() error
	Close()
	//AppendEntries(ctx context.Context, entries []block, mustSync bool) (uint64, uint32, error)
	ReadLastBlock(ctx context.Context) (block, error)
	Append(ctx context.Context, blocks []block, mustSync bool) (extentID uint64, offsets []uint32, end uint32, err error)
	PunchHoles(ctx context.Context, extentIDs []uint64) error
	NewLogEntryIter(opt ...ReadOption) LogEntryIter
	//truncate extent BEFORE extentID
	Truncate(ctx context.Context, extentID uint64) error
	//CommitEnd() is offset of current extent
	CommitEnd() uint32 //FIMME:remove this
	//total number of extents
	StreamInfo() *pb.StreamInfo
	Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32, hint byte) ([]block, uint32, error)
	SealedLength(extentID uint64) (uint64, error)
}

type LogEntryIter interface {
	HasNext() (bool, error)
	//(block, offset, end)
	Next() (data block, extentID uint64, offset uint32, end uint32)
}

//hint
const (
	HintReadThrough        = 1 << 0
	HintReadFromCache byte = 1 << 1
)

func blockCacheID(extentID uint64, offset uint32) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], extentID)
	binary.BigEndian.PutUint32(buf[8:12], offset)
	return buf
}

type blockInCache struct {
	block   block
	readEnd uint32
}

func (sc *AutumnStreamClient) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32, hint byte) ([]block, uint32, error) {
	//only cache metadata
	if (hint&HintReadFromCache) > 0 && numOfBlocks == 1 {
		data, ok := sc.blockCache.Get(blockCacheID(extentID, offset))
		if ok && data != nil {
			x := data.(blockInCache)
			return [][]byte{x.block}, x.readEnd, nil
		}
	}
	blocks, _, end, err := sc.smartRead(ctx, extentID, offset, numOfBlocks, false)

	if (hint&HintReadFromCache) > 0 && numOfBlocks == 1 {
		sc.blockCache.Set(blockCacheID(extentID, offset), blockInCache{block: blocks[0], readEnd: end}, 0)
	}
	return blocks, end, err
}

func (sc *AutumnStreamClient) SealedLength(extentID uint64) (uint64, error) {
	exInfo := sc.em.GetExtentInfo(extentID)
	if exInfo == nil || exInfo.Avali == 0 {
		return 0, errors.Errorf("extent %d not exist or not sealed", extentID)
	}
	return exInfo.SealedLength, nil
}

//helper function to read blocks' data
//err is [nil, err, wire_errors.EndOfExtent]
func (sc *AutumnStreamClient) readBlockData(ctx context.Context, conn *grpc.ClientConn, exInfo *pb.ExtentInfo, request *pb.ReadBlocksRequest) ([]block, []uint32, uint32, error) {

retry:
	c := pb.NewExtentServiceClient(conn)
	stream, err := c.ReadBlocks(ctx, request)
	//network error
	if err != nil {
		return nil, nil, 0, err
	}
	res, err := stream.Recv()
	//network error
	if err != nil {
		return nil, nil, 0, err
	}

	header := res.GetHeader()
	if header == nil {
		return nil, nil, 0, errors.Errorf("read response header is nil")
	}

	//logic error
	err = wire_errors.FromPBCode(header.Code, header.CodeDes)
	switch err {
	case nil, wire_errors.EndOfExtent:
		//normal case
	case wire_errors.VersionLow:
		sc.em.WaitVersion(exInfo.ExtentID, exInfo.Eversion+1)
		goto retry
	default:
		return nil, nil, 0, err
	}

	buf := new(bytes.Buffer)
	for {
		res, err := stream.Recv()
		if err != nil && err != io.EOF {
			return nil, nil, 0, err
		}
		payload := res.GetPayload()
		if payload == nil {
			break
		}
		buf.Write(payload)
	}

	//verfiy all blocks have been received
	sum := uint32(0)
	for i := range header.BlockSizes {
		sum += header.BlockSizes[i]
	}
	if sum != uint32(buf.Len()) {
		return nil, nil, 0, errors.Errorf("read response payload length is not equal to header")
	}
	//each block point to slice of buf
	blocks := make([][]byte, len(header.BlockSizes))
	n := 0
	for i := 0; i < len(blocks); i++ {
		blocks[i] = buf.Bytes()[n : n+int(header.BlockSizes[i])]
		n += int(header.BlockSizes[i])
	}
	return blocks, header.Offsets, header.End, err
}

//err could be [nil, error, wire_errors.EndOfExtent]
func (sc *AutumnStreamClient) smartRead(gctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32, onlyReadLast bool) ([][]byte, []uint32, uint32, error) {
	if onlyReadLast {
		utils.AssertTrue(numOfBlocks == 1)
	}

	span, ctx := opentracing.StartSpanFromContext(gctx, "SmartRead")
	defer span.Finish()

	exInfo := sc.em.GetExtentInfo(extentID)
	if exInfo == nil {
		return nil, nil, 0, errors.Errorf("extent %d not exist", extentID)
	}

	//replicate read
	if len(exInfo.Parity) == 0 {
		conn := sc.em.GetExtentConn(exInfo.ExtentID, smclient.AlivePolicy{})
		if conn == nil {
			return nil, nil, 0, errors.Errorf("unable to get extent connection.")
		}
		return sc.readBlockData(ctx, conn, exInfo, &pb.ReadBlocksRequest{
			ExtentID:      extentID,
			Offset:        offset,
			NumOfBlocks:   numOfBlocks,
			Eversion:      exInfo.Eversion,
			OnlyLastBlock: onlyReadLast,
		})
	}

	//EC read
	dataShards := len(exInfo.Replicates)
	parityShards := len(exInfo.Parity)
	n := dataShards + parityShards

	//if we read from n > dataShard, call cancel() to stop
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)

	dataBlocks := make([][]block, n)

	//channel
	type Result struct {
		Error   error
		End     uint32
		Len     int
		Offsets []uint32
	}

	stopper := utils.NewStopper()
	retChan := make(chan Result, n)
	errChan := make(chan Result, n)
	req := &pb.ReadBlocksRequest{
		ExtentID:      extentID,
		Offset:        offset,
		NumOfBlocks:   numOfBlocks,
		Eversion:      exInfo.Eversion,
		OnlyLastBlock: onlyReadLast,
	}

	submitReq := func(pool *conn.Pool, pos int) {
		if pool == nil {
			errChan <- Result{
				Error: errors.New("can not get conn"),
			}
			return
		}
		blocks, offsets, end, err := sc.readBlockData(pctx, pool.Get(), exInfo, req)
		if err != nil && err != wire_errors.EndOfExtent {
			errChan <- Result{
				Error: err,
			}
			return
		}
		//successful read
		dataBlocks[pos] = blocks
		retChan <- Result{
			Error:   err,
			End:     end,
			Len:     len(blocks),
			Offsets: offsets,
		}
	}

	//pools里面有可能有nil
	pools, err := sc.em.ConnPool(sc.em.GetPeers(req.ExtentID))
	if err != nil {
		return nil, nil, 0, err
	}

	//only read from avali data
	//read data shards
	for i := 0; i < dataShards; i++ {
		j := i
		if exInfo.Avali > 0 && (exInfo.Avali&(1<<j)) == 0 {
			continue
		}
		stopper.RunWorker(func() {
			submitReq(pools[j], j)
		})
	}

	//read parity data
	for i := 0; i < parityShards; i++ {
		j := i
		if exInfo.Avali > 0 && (1<<(j+dataShards))&exInfo.Avali == 0 {
			continue
		}
		stopper.RunWorker(func() {
			select {
			case <-stopper.ShouldStop():
				return
			case <-time.After(20 * time.Millisecond):
				submitReq(pools[dataShards+j], j+dataShards)
			}
		})

	}

	successRet := make([]Result, 0, dataShards)
	var success bool
waitResult:
	for {
		select {
		case <-pctx.Done(): //time out
			break waitResult
		case r := <-retChan:
			successRet = append(successRet, r)
			if len(successRet) == dataShards {
				success = true
				break waitResult
			}

		}
	}

	cancel()
	stopper.Stop()
	close(retChan)
	close(errChan)

	var lenOfBlocks int
	var end uint32
	var finalErr error
	var offsets []uint32
	if success {
		//collect successRet, END/ERROR should be the saved
		for i := 0; i < dataShards-1; i++ {
			//if err is EndExtent or EndStream, err must be the save
			if successRet[i].End != successRet[i+1].End {
				return nil, nil, 0, errors.Errorf("extent %d: wrong successRet, %+v != %+v ", exInfo.ExtentID, successRet[i], successRet[i+1])
			}
		}
		lenOfBlocks = successRet[0].Len
		end = successRet[0].End
		finalErr = successRet[0].Error //Could be EndOfExtent or nil
		offsets = successRet[0].Offsets
	} else {
		//collect errChan, if err is not wire_errors.NotFound
		errBuf := new(bytes.Buffer)
		for r := range errChan {
			if r.Error == wire_errors.NotFound {
				return nil, nil, 0, r.Error
			}
			errBuf.WriteString(r.Error.Error())
			errBuf.WriteString("\n")
		}
		return nil, nil, 0, errors.Errorf("%s", errBuf.String())
	}

	//join each block
	span.LogKV("DECODE_BLOCKS START", lenOfBlocks)
	retBlocks := make([][]byte, lenOfBlocks)
	for i := 0; i < lenOfBlocks; i++ {
		data := make([][]byte, n)
		for j := range dataBlocks {
			//in EC read, dataBlocks[j] could be nil, because we have got enough data to decode
			if dataBlocks[j] != nil {
				data[j] = dataBlocks[j][i]
			}
		}
		output, err := erasure_code.ReedSolomon{}.Decode(data, dataShards, parityShards)
		//EC decode error
		if err != nil {
			return nil, nil, 0, err
		}
		retBlocks[i] = output
	}
	return retBlocks, offsets, end, finalErr
}

type readOption struct {
	ReadFromStart bool
	ExtentID      uint64
	Offset        uint32
	//max extents we can read
	MaxExtentRead int
}

type ReadOption func(*readOption)

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
	//StreamClient
	smClient   *smclient.SMClient
	streamInfo *pb.StreamInfo

	em            *smclient.ExtentManager
	streamID      uint64
	streamLock    StreamLock
	end           uint32
	maxExtentSize uint32

	utils.SafeMutex //protect streamInfo from allocStream, Truncate, PunchHole
	blockCache      *ristretto.Cache
}

func NewStreamClient(sm *smclient.SMClient, em *smclient.ExtentManager, maxExtentSize uint32, streamID uint64, streamLock StreamLock) *AutumnStreamClient {
	utils.AssertTrue(xlog.Logger != nil)

	utils.AssertTruef(maxExtentSize < HardMaxExtentSize, "maxExtentSize %d is too large", maxExtentSize)
	blockCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	utils.AssertTruef(err == nil, "NewCache error: %v", err)
	return &AutumnStreamClient{
		smClient:      sm,
		em:            em,
		streamID:      streamID,
		streamLock:    streamLock,
		maxExtentSize: maxExtentSize,
		blockCache:    blockCache,
	}
}

type wrapBlockOffset struct {
	blockOffset uint32
	data        block
	extentID    uint64
	end         uint32
}

type AutumnEntryIter struct {
	sc                 *AutumnStreamClient
	opt                *readOption
	currentOffset      uint32
	currentExtentIndex int
	noMore             bool
	replay             bool
	n                  int //number of extents we have read

	cache []wrapBlockOffset
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

func (sc *AutumnStreamClient) ReadLastBlock(ctx context.Context) (block, error) {

	if len(sc.streamInfo.ExtentIDs) == 0 {
		return nil, errors.Errorf("no extent in stream")
	}

	findValidLastExtent := func(i int) (*pb.ExtentInfo, int) {
		for ; i >= 0; i-- {
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

	i := len(sc.streamInfo.ExtentIDs) - 1
	for i >= 0 {
		exInfo, idx := findValidLastExtent(i)
		if idx == -1 {
			return nil, wire_errors.NotFound
		}

		blocks, _, _, err := sc.smartRead(ctx, exInfo.ExtentID, 0, 1, true)
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
		for !iter.noMore && len(iter.cache) == 0 {
			err := iter.receiveEntries()
			if err != nil {
				return false, err
			}
		}
	}

	return len(iter.cache) > 0, nil
}

func (iter *AutumnEntryIter) Next() (block, uint64, uint32, uint32) {
	if ok, err := iter.HasNext(); !ok || err != nil {
		return nil, 0, 0, 0
	}
	ret := iter.cache[0]
	iter.cache = iter.cache[1:]
	return ret.data, ret.extentID, ret.blockOffset, ret.end
}

func (iter *AutumnEntryIter) receiveEntries() error {
	loop := 0

	extentID, err := iter.sc.getExtentFromIndex(iter.currentExtentIndex)
	if err != nil {
		return err
	}
retry:

	//exInfo := iter.sc.em.GetExtentInfo(extentID)
	//xlog.Logger.Debugf("read extentID %d, offset : %d, eversion is %d\n", extentID, iter.currentOffset, exInfo.Eversion)

	//fmt.Printf("read extentID %d, offset : %d,  index : %d\n from %s ", extentID,
	//iter.currentOffset, iter.currentExtentIndex, iter.conn.Target())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	blocks, offsets, end, err := iter.sc.smartRead(ctx, extentID, iter.currentOffset, 1000, false)
	cancel()

	if err != nil && err != wire_errors.EndOfExtent {
		if loop > 5 {
			return err
		}
		fmt.Printf("receiveEntries get error %s\n", err.Error())
		loop++
		time.Sleep(3 * time.Second)
		goto retry
	}

	for i := range blocks {
		var blockEnd uint32
		if i == len(blocks)-1 {
			blockEnd = end
		} else {
			blockEnd = offsets[i+1]
		}

		iter.cache = append(iter.cache, wrapBlockOffset{
			blockOffset: offsets[i],
			data:        blocks[i],
			extentID:    extentID,
			end:         blockEnd,
		})
	}

	switch err {
	case nil:
		iter.currentOffset = end
		return nil
	case wire_errors.EndOfExtent:
		iter.currentOffset = 0
		iter.currentExtentIndex++
		iter.n++
		if iter.currentExtentIndex == len(iter.sc.streamInfo.ExtentIDs) || iter.n >= iter.opt.MaxExtentRead {
			iter.noMore = true
		}
		fmt.Printf("readentries currentIndex is %d in %v nomore?%v\n", iter.currentExtentIndex, iter.sc.streamInfo.ExtentIDs, iter.noMore)

		return nil
	default:
		panic("never be here")
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
		n:   0,
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
		return err
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
		time.Sleep(100 * time.Millisecond)
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

//appendBlocks is a helper function for Append
func (sc *AutumnStreamClient) appendBlocks(ctx context.Context, exInfo *pb.ExtentInfo, blocks []block, mustSync bool) ([]uint32, uint32, error) {

	pools, err := sc.em.ConnPool(sc.em.GetPeers(exInfo.ExtentID))
	if err != nil {
		return nil, 0, errors.Errorf("extent %d's append can not get pool[%s]", exInfo.ExtentID, err.Error())
	}

	//choose offset
	offset := sc.end

	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	n := len(pools)

	dataBlocks := make([][]block, n)
	//erasure code
	if len(exInfo.Parity) > 0 {
		//EC, prepare data
		dataShard := len(exInfo.Replicates)
		parityShard := len(exInfo.Parity)

		for i := range blocks {
			striped, err := erasure_code.ReedSolomon{}.Encode(blocks[i], dataShard, parityShard)
			if err != nil {
				return nil, 0, err
			}
			for j := 0; j < len(striped); j++ {
				dataBlocks[j] = append(dataBlocks[j], striped[j])
			}

		}
		//replicate code
	} else {
		for i := 0; i < len(dataBlocks); i++ {
			dataBlocks[i] = blocks
		}
	}

	//FIXME: put stopper into sync.Pool
	stopper := utils.NewStopper()
	type Result struct {
		Error   error
		Offsets []uint32
		End     uint32
	}
	retChan := make(chan Result, n)

	//leaderless append mode, send Append to all peers
	for i := 0; i < n; i++ {
		j := i
		stopper.RunWorker(func() {
			conn := pools[j].Get()
			client := pb.NewExtentServiceClient(conn)

			stream, err := client.Append(pctx)
			if err != nil {
				retChan <- Result{err, nil, 0}
				return
			}

			//send header
			sizeOfBlocks := make([]uint32, len(dataBlocks[j]))
			for i := range sizeOfBlocks {
				sizeOfBlocks[i] = uint32(len(dataBlocks[j][i]))
			}
			if err = stream.Send(&pb.AppendRequest{
				Data: &pb.AppendRequest_Header{
					Header: &pb.AppendRequestHeader{
						ExtentID: exInfo.ExtentID,
						Commit:   offset,
						Revision: sc.streamLock.revision,
						MustSync: mustSync,
						Blocks:   sizeOfBlocks,
						Eversion: exInfo.Eversion,
					}}}); err != nil {
				retChan <- Result{err, nil, 0}
				return
			}

			//send data
			//grpc payload could be as large as 32MB
			for i := range dataBlocks[j] {
				if err = stream.Send(&pb.AppendRequest{
					Data: &pb.AppendRequest_Payload{
						Payload: dataBlocks[j][i],
					},
				}); err != nil {
					break
				}
			}
			if err != nil {
				retChan <- Result{err, nil, 0}
				return
			}

			res, err := stream.CloseAndRecv()

			if err == nil {
				if res.Code != pb.Code_OK {
					err = wire_errors.FromPBCode(res.Code, res.CodeDes)
				}
				retChan <- Result{Error: err, Offsets: res.Offsets, End: res.End}
			} else {
				retChan <- Result{Error: err}
			}
			xlog.Logger.Debugf("append remote done [%s], %v", pools[j].Addr, err)
		})
	}

	stopper.Wait()
	close(retChan)

	var preOffsets []uint32
	preEnd := int64(-1)
	for result := range retChan {
		if preOffsets == nil {
			preOffsets = result.Offsets
		}
		if preEnd == -1 {
			preEnd = int64(result.End)
		}
		if result.Error != nil {
			fmt.Printf("append on extent %d error: %v\n", exInfo.ExtentID, result.Error)
			return nil, 0, result.Error
		}
		if !utils.EqualUint32(result.Offsets, preOffsets) || preEnd != int64(result.End) {
			return nil, 0, errors.Errorf("block is not appended at the same offset [%v] vs [%v], end [%v] vs [%v]",
				result.Offsets, preOffsets, preEnd, result.End)
		}
	}

	return preOffsets, uint32(preEnd), nil
}

func (sc *AutumnStreamClient) Append(ctx context.Context, blocks []block, mustSync bool) (uint64, []uint32, uint32, error) {

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

	offsets, end, err := sc.appendBlocks(ctx, exInfo, blocks, mustSync)

	if err == wire_errors.LockedByOther {
		//conflict happend, tell upper layer to close this stream
		return extentID, nil, 0, err
	}

	if err == wire_errors.VersionLow {
		sc.em.WaitVersion(extentID, exInfo.Eversion+1)
		goto retry
	}

	if err != nil { //status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Unavailable
		//fmt.Printf("append on extent %d; error is %v\n", extentID, err)
		if loop < 2 {
			loop++
			time.Sleep(100 * time.Millisecond)
			goto retry
		}
		if err = sc.MustAllocNewExtent(); err != nil {
			return 0, nil, 0, err
		}
		loop = 0 //reset loop, new extent could retry
		goto retry
	}

	sc.end = end
	//检查offset结果, 如果已经超过MaxExtentSize, 调用StreamAllocExtent
	utils.AssertTrue(end > 0)

	if end > sc.maxExtentSize {
		if err = sc.MustAllocNewExtent(); err != nil {
			return 0, nil, 0, err
		}
	}
	//update end
	return extentID, offsets, end, nil
}

func (sc *AutumnStreamClient) StreamInfo() *pb.StreamInfo {
	//copy sc.streamInfo
	sc.RLock()
	defer sc.RUnlock()
	return proto.Clone(sc.streamInfo).(*pb.StreamInfo)
}

//add String() function to pb.StringInfo

// func FormatEntry(entry *pb.EntryInfo) string {
// 	var flag string
// 	switch entry.Log.Meta {
// 	case 2:
// 		flag = "blob"
// 	case 1:
// 		flag = "delete"
// 	case 0:
// 		flag = "norm"
// 	}
// 	return fmt.Sprintf("%s~%d on extent %d:(%d-%d) [%s]",
// 		y.ParseKey(entry.Log.Key), y.ParseTs(entry.Log.Key), entry.ExtentID, entry.Offset, entry.End, flag)
// }
