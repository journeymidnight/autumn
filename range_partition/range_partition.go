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
package range_partition

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dgryski/go-farm"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/range_partition/skiplist"
	"github.com/journeymidnight/autumn/range_partition/table"
	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

const (
	KB = 1024
	MB = KB * 1024
	GB = MB * 1024
)

var (
	errNoRoom        = errors.New("No room for write")
	errNotFound      = errors.New("not found")
	ErrBlockedWrites = errors.New("Writes are blocked, possibly due to DropAll or Close")
)

type OpenStreamFunc func(si pb.StreamInfo) streamclient.StreamClient

type RangePartition struct {
	logStream       streamclient.StreamClient
	rowStream       streamclient.StreamClient
	metaStream      streamclient.StreamClient

	blockReader streamclient.BlockReader

	writeCh         chan *request
	blockWrites     int32
	mt              *skiplist.Skiplist
	imm             []*skiplist.Skiplist
	utils.SafeMutex //protect mt,imm when swapping mt, imm

	flushStopper   *utils.Stopper
	flushChan      chan flushTask
	writeStopper   *utils.Stopper
	tableLock      utils.SafeMutex //protect tables
	tables         []*table.Table
	seqNumber      uint64

	PartID   uint64
	StartKey []byte
	EndKey   []byte

	closeOnce sync.Once    // For closing DB only once.
	vhead     valuePointer //vhead前的都在mt中

	opt *Option

	//functions
	openStream OpenStreamFunc //doGC, read log stream.


	//compaction pickup table policy
	pickupTablePolicy PickupTables

	hasOverlap uint32 //atomic

	majorCompactChan chan struct{}
	compactStopper *utils.Stopper

	gcRunChan 	     chan struct{}
	gcStopper  	*utils.Stopper
}

//TODO
//interface KV save some values

func OpenRangePartition(id uint64, metaStream streamclient.StreamClient, rowStream streamclient.StreamClient,
	logStream streamclient.StreamClient, blockReader streamclient.BlockReader,
	startKey []byte, endKey []byte,
	openStream OpenStreamFunc, opts ...OptionFunc,
) (*RangePartition, error) {

	utils.AssertTrue(len(opts) > 0)
	opt := &Option{}
	for _, optf := range opts {
		optf(opt)
	}

	fmt.Printf("opt is %+v\n", opt)
	rp := &RangePartition{
		rowStream:   rowStream,
		logStream:   logStream,
		metaStream:  metaStream,
		blockReader: blockReader,
		mt:          skiplist.NewSkiplist(int64(opt.MaxSkipList)),
		imm:         nil,
		blockWrites: 0,
		StartKey:    startKey,
		EndKey:      endKey,
		PartID:      id,
		openStream:  openStream,
		opt:         opt,
	}
	rp.startMemoryFlush()

	fmt.Printf("log end is %d\n", logStream.CommitEnd())
	fmt.Printf("row end is %d\n", rowStream.CommitEnd())
	fmt.Printf("meta end is %d\n", metaStream.CommitEnd())

	var tableLocs pspb.TableLocations


	block, err := metaStream.ReadLastBlock(context.Background())
	if err == wire_errors.NotFound {
		tableLocs.Locs = nil
	} else if err != nil {
		return nil, err
	} else {
		utils.MustUnMarshal(block.Data, &tableLocs)
	}

	fmt.Printf("table locs is %v\n", tableLocs.Locs)

	//replay log
	//open tables
	//tableLocs的顺序就是在logStream里面的顺序
	for _, tLoc := range tableLocs.Locs {
	retry:
		tbl, err := table.OpenTable(rp.blockReader, tLoc.ExtentID, tLoc.Offset)
		if err != nil {
			xlog.Logger.Error(err)
			time.Sleep(1 * time.Second)
			goto retry
		}
		rp.tables = append(rp.tables, tbl)

		key := y.ParseKey(tbl.Smallest())


		if !rp.IsUserKeyInRange(key) {
			rp.hasOverlap = 1
		}
		key = y.ParseKey(tbl.Biggest())
		if !rp.IsUserKeyInRange(key) {
			rp.hasOverlap = 1
		}
	}

	var lastTable *table.Table
	rp.seqNumber = 0
	for i := range rp.tables {
		fmt.Printf("table %d, table lastSeq %d, vp [%d, %d], len of table %d\n", 
				i, rp.tables[i].LastSeq, rp.tables[i].VpExtentID, rp.tables[i].VpOffset, rp.tables[i].EstimatedSize)
		if rp.tables[i].LastSeq > rp.seqNumber {
			rp.seqNumber = rp.tables[i].LastSeq
			lastTable = rp.tables[i]
		}
	}

	//ASSERT, FIXME
	rp.CheckTableOrder(rp.tables)

	//FIXME:poor performace: prefetch read will be better
	replayedLog := 0
	logSizeRead := uint32(0)

	replay := func(ei *pb.EntryInfo) (bool, error) {
		replayedLog++
		//fmt.Printf("from log %v\n", string(ei.Log.Key))
		//build ValueStruct from EntryInfo
		entriesReady := []*pb.EntryInfo{ei}

		userKey := y.ParseKey(ei.Log.Key)


		if !rp.IsUserKeyInRange(userKey) {
			return true, nil
		}

		head := valuePointer{extentID: ei.ExtentID, offset: ei.End}
		logSizeRead += ei.End - ei.Offset
		//print value len of ei
		if y.ParseTs(ei.Log.Key) > rp.seqNumber {
			rp.seqNumber = y.ParseTs(ei.Log.Key)
		}

		i := 0
		/*
		*
		* 有一种特殊情况, block里面有3个entries, 其中第3个entry
		* 放不到mt里面,会强制刷memtable, 而这个table的vp, 指向block的offset
		* 并且刷出来的table只有前2个entry, 总之结果就是有2个table有overlap的key的情况
		 */
		for err := rp.ensureRoomForWrite(entriesReady, head); err == errNoRoom; err = rp.ensureRoomForWrite(entriesReady, head) {
			i++
			if i%100 == 0 {
				xlog.Logger.Infof("Making room for writes")
			}
			// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			time.Sleep(10 * time.Millisecond)
		}
		/*
			if len(ei.Log.Key) == 0 {
				return true, nil
			}
		*/

		rp.writeToLSM([]*pb.EntryInfo{ei})

		rp.vhead = head
		return true, nil
	}

	start := time.Now()
	if lastTable == nil {
		//err := replayLog(rp.logStream, 0, 0, true, replay)
		err := replayLog(rp.logStream, replay, streamclient.WithReadFromStart(math.MaxUint32), streamclient.WithReplay())
		if err != nil {
			return nil, err
		}
	} else {
		//fmt.Printf("replay log from vp extent %d, offset [%d]\n", lastTable.VpExtentID, lastTable.VpOffset)
		/*
			rp.vhead = valuePointer{
				extentID: lastTable.VpExtentID,
				offset:   lastTable.VpOffset,
			}
		*/
		//err := replayLog(rp.logStream, lastTable.VpExtentID, lastTable.VpOffset, true, replay)
		err := replayLog(rp.logStream, replay, streamclient.WithReadFrom(lastTable.VpExtentID, lastTable.VpOffset, math.MaxUint32), streamclient.WithReplay())

		if err != nil {
			return nil, err
		}
	}

	//xlog.Logger.Infof("replayed log number: %d, time taken %v\n", replayedLog, time.Since(start))
	fmt.Printf("replayed log number: %d, mt size is %d, time taken %v, read size %v \n", replayedLog, rp.mt.MemSize(), time.Since(start), logSizeRead)

	//start real write
	rp.startWriteLoop()

	rp.pickupTablePolicy = DefaultPickupPolicy{
		compactRatio: 0.5,
		headRatio: 0.3,
		n :5,
		opt: rp.opt,
	}
	
	rp.startCompact()
	rp.startGC()
	//do compactions:FIXME, doCompactions放到另一个goroutine里面执行

	return rp, nil
}

type flushTask struct {
	mt        *skiplist.Skiplist
	vptr      valuePointer
	seqNum    uint64
	isCompact bool          //如果是compact任务, 不需要修改rp.mt
	resultCh  chan struct{} //也可以用wg, 但是防止未来还需要发数据
}

//split相关, 提供相关参数给上层
//ADD more policy here, to support different Split policy
func (rp *RangePartition) GetSplitPoint() []byte {
	utils.AssertTrue(rp.hasOverlap == 0)

	//find the biggest table
	rp.tableLock.Lock()
	defer rp.tableLock.Unlock()

	var biggestTable *table.Table
	biggestTable = rp.tables[0]

	for _, tbl := range rp.tables {
		if tbl.EstimatedSize > biggestTable.EstimatedSize {
			biggestTable = tbl
		}
	}
	return y.ParseKey(biggestTable.MidKey())
}

//split相关, 提供相关参数给上层
//ADD more policy here, to support different Split policy
func (rp *RangePartition) CanSplit() bool {
	rp.tableLock.Lock()
	defer rp.tableLock.Unlock()
	return atomic.LoadUint32(&rp.hasOverlap) == 0 && len(rp.tables) > 0
}

func (rp *RangePartition) LogRowStreamEnd() (uint32, uint32) {
	return rp.logStream.CommitEnd(), rp.rowStream.CommitEnd()
}

func (rp *RangePartition) startWriteLoop() {
	rp.writeStopper = utils.NewStopper()
	rp.writeCh = make(chan *request, rp.opt.WriteChCapacity)

	rp.writeStopper.RunWorker(rp.doWrites)
}


func (rp *RangePartition) IsUserKeyInRange(userKey []byte) bool {
	return bytes.Compare(rp.StartKey, userKey) <= 0 && (len(rp.EndKey) == 0 || bytes.Compare(userKey, rp.EndKey) < 0) 
}

func (rp *RangePartition) startMemoryFlush() {
	// Start memory fluhser.

	rp.flushStopper = utils.NewStopper()
	rp.flushChan = make(chan flushTask, 16)

	rp.flushStopper.RunWorker(rp.flushMemtable)
}

// handleFlushTask must be run serially.
func (rp *RangePartition) handleFlushTask(ft flushTask) error {
	// There can be a scenario, when empty memtable is flushed. For example, memtable is empty and
	// after writing request to value log, rotation count exceeds db.LogRotatesToFlush.
	if ft.mt.Empty() {
		return nil
	}

	iter := ft.mt.NewIterator()
	defer iter.Close()
	b := table.NewTableBuilder(rp.rowStream)
	defer b.Close()

	//var vp valuePointer
	var first []byte
	var last []byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if first == nil {
			first = iter.Key()
		}
		/*
			vs := iter.Value()
			if vs.Meta&bitValuePointer > 0 {
				vp.Decode(vs.Value)
			}
		*/
		//fmt.Printf("%s:%s\n", string(iter.Key()), iter.Value().Value)
		b.Add(iter.Key(), iter.Value())
		last = iter.Key()
	}

	b.FinishBlock()
	id, offset, err := b.FinishAll(ft.vptr.extentID, ft.vptr.offset, ft.seqNum)
	if err != nil {
		xlog.Logger.Errorf("ERROR while build table: %v", err)
		return err
	}

	tbl, err := table.OpenTable(rp.blockReader, id, offset)
	if err != nil {
		xlog.Logger.Errorf("ERROR while opening table: %v", err)
		return err
	}

	// We own a ref on tbl.

	rp.tableLock.Lock()
	rp.tables = append(rp.tables, tbl)
	rp.tableLock.Unlock()

	xlog.Logger.Debugf("flushed table %s to %s seq[%d], head %d\n", y.ParseKey(first), y.ParseKey(last), tbl.LastSeq, tbl.VpOffset)

	return nil
}

func (rp *RangePartition) getMemTables() ([]*skiplist.Skiplist, func()) {
	rp.RLock()
	defer rp.RUnlock()

	tables := make([]*skiplist.Skiplist, len(rp.imm)+1)

	// Get mutable memtable.
	tables[0] = rp.mt
	tables[0].IncrRef()
	last := len(rp.imm) - 1

	// reverse insert into tables
	for i := range rp.imm {
		tables[i+1] = rp.imm[last-i]
		tables[i+1].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

// flushMemtable must keep running until we send it an empty flushTask. If there
// are errors during handling the flush task, we'll retry indefinitely.
func (rp *RangePartition) flushMemtable() {
	for ft := range rp.flushChan {
		if ft.mt == nil {
			// We close db.flushChan now, instead of sending a nil ft.mt.
			continue
		}
		//save to rowStream
		for {
			err := rp.handleFlushTask(ft)
			if err == nil {
				if !ft.isCompact {
					// Update s.imm. Need a lock.
					rp.Lock()

					utils.AssertTrue(ft.mt == rp.imm[0])
					rp.imm = rp.imm[1:]
					ft.mt.DecrRef() // Return memory.
					rp.Unlock()
				} else { //如果是compact任务的build table, 不需要修改rp.imm
					ft.mt.DecrRef()
				}
				break
			}
			// Encountered error. Retry indefinitely.
			xlog.Logger.Errorf("Failure while flushing memtable to disk: %v. Retrying...", err)
			time.Sleep(time.Second)
		}

		//save table offset in PM
		var tableLocs []*pspb.Location
		rp.tableLock.RLock()
		for _, t := range rp.tables {
			tableLocs = append(tableLocs, &t.Loc)
		}
		rp.updateTableLocs(tableLocs)
		rp.tableLock.RUnlock()

		if ft.isCompact {
			ft.resultCh <- struct{}{}
		}

	}
}

func (rp *RangePartition) updateTableLocs(tableLocs []*pspb.Location) {
	if len(tableLocs) == 0 {
		return
	}

	var locations pspb.TableLocations
	locations.Locs = tableLocs
	data := utils.MustMarshal(&locations)
	//fmt.Printf("update table locations %v\n", tableLocs)
	backoff := 50 * time.Millisecond
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
		_, _, _, err := rp.metaStream.Append(ctx, []*pb.Block{{data}}, rp.opt.MustSync)
		cancel()
		if err != nil {
			xlog.Logger.Errorf("failed to set tableLocs for %d, retry...", rp.PartID)
			backoff = backoff * 2
			time.Sleep(backoff)
			continue
		}
		break
	}

}

//read metaStream, connect commitlog, rowDataStreamand blobDataStream
func (rp *RangePartition) Connect() error {
	var err error
	return err
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

type request struct {
	// Input values, 这个以后可能有多个Entry..., 比如一个request里面
	//A = "x", A:time = "20191224", A:md5 = "fbd80028d33edaa2d7bace539f534cb2"
	entries []*pb.EntryInfo

	isGCRequest bool
	// Output values and wait group stuff below
	wg  sync.WaitGroup
	Err error
	ref int32
}

//key + valueStruct
func estimatedSizeInSkl(e *pb.Entry) int {
	sz := len(e.Key)
	if y.ShouldWriteValueToLSM(e) {
		sz += len(e.Value) + 1 // Meta
	} else {
		sz += int(vptrSize) + 1 // vptrSize for valuePointer, 1 for meta
	}

	sz += utils.SizeVarint(e.ExpiresAt)

	sz += skiplist.MaxNodeSize + 8 //8 is nodeAlign
	return sz
}

func estimatedVS(key []byte, vs y.ValueStruct) int {
	sz := len(key)
	if vs.Meta&y.BitValuePointer == 0 && len(vs.Value) <= y.ValueThrottle {
		sz += len(vs.Value) + 1 // Meta
	} else {
		sz += int(vptrSize) + 1 // vptrSize for valuePointer, 1 for meta
	}
	sz += utils.SizeVarint(vs.ExpiresAt)

	sz += skiplist.MaxNodeSize + 8 //8 is nodeAlign
	return sz
}

//size in log
func (r *request) Size() int {
	size := 0
	for i := range r.entries {
		size += r.entries[i].Log.Size()
	}
	return size
}

func (req *request) reset() {
	req.entries = nil
	req.wg = sync.WaitGroup{}
	req.Err = nil
	req.ref = 0
	req.isGCRequest = false
}

func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	requestPool.Put(req)
}

func (req *request) Wait() error {
	req.wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}


// writeRequests is called serially by only one goroutine.
func (rp *RangePartition) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	if reqs[0].isGCRequest {
		utils.AssertTruef(len(reqs) == 1, "GC request should be the only request")
		gcRequest := reqs[0]
		for i := len(gcRequest.entries) - 1; i >= 0; i-- {
			userKey := y.ParseKey(gcRequest.entries[i].Log.Key)
			vs := rp.getValueStruct(userKey, 0)
			if vs.Version > y.ParseTs(gcRequest.entries[i].Log.Key) {
				//exclude this entry
				gcRequest.entries = append(gcRequest.entries[:i], gcRequest.entries[i+1:]...)
				continue
			}
		}
	}
	

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.wg.Done()
		}
	}

	xlog.Logger.Debugf("writeRequests called. Writing to log, len[%d]", len(reqs))

	entriesReady, head, err := rp.writeValueLog(reqs)
	if err != nil {
		done(err)
		return err
	}

	//lastReq := reqs[len(reqs)-1]
	//e := lastReq.entries[len(lastReq.entries)-1]
	//seqNum := y.ParseTs(e.Log.Key)

	xlog.Logger.Info("Writing to memtable")
	i := 0
	for err = rp.ensureRoomForWrite(entriesReady, rp.vhead); err == errNoRoom; err = rp.ensureRoomForWrite(entriesReady, rp.vhead) {
		i++
		if i%100 == 0 {
			xlog.Logger.Infof("Making room for writes")
		}
		// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
		// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
		// you will get a deadlock.
		time.Sleep(10 * time.Millisecond)
	}

	if err != nil {
		done(err)
		return errors.Wrap(err, "writeRequests")
	}

	//write to LSM
	for _, b := range reqs {
		if err := rp.writeToLSM(b.entries); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
	}

	rp.vhead = head
	done(nil)
	return nil
}

func getLowerByte(a uint32) byte {
	return byte(a & 0x000000FF)
}

func (rp *RangePartition) writeToLSM(entries []*pb.EntryInfo) error {
	for _, entry := range entries {
		if y.ShouldWriteValueToLSM(entry.Log) { // Will include deletion / tombstone case.
			rp.mt.Put(entry.Log.Key,
				y.ValueStruct{
					Value:     entry.Log.Value,
					Meta:      getLowerByte(entry.Log.Meta),
					ExpiresAt: entry.Log.ExpiresAt,
				})
		} else {
			vp := valuePointer{
				entry.ExtentID,
				entry.Offset,
				uint32(len(entry.Log.Value)),
			}
			rp.mt.Put(entry.Log.Key,
				y.ValueStruct{
					Value:     vp.Encode(),
					Meta:      getLowerByte(entry.Log.Meta) | y.BitValuePointer,
					ExpiresAt: entry.Log.ExpiresAt,
				})
		}
	}
	return nil
}

func (rp *RangePartition) isReqsTooBig(reqs []*request) bool {
	//grpc limit
	size := 0 //network size in grpc
	n := 0
	for _, req := range reqs {
		size += req.Size()
		if size > 30*MB {
			return true
		}

		n += len(req.entries)
		if n > 3*rp.opt.WriteChCapacity {
			return true
		}
	}

	return false
}

func (rp *RangePartition) doWrites() {
	//defer lc.Done()

	pendingCh := make(chan struct{}, 1)

	/*
		以下pendingCh <- struct{}{} // blocking
		都是在等待writeRequests结束
	*/

	writeRequests := func(reqs []*request) {
		if err := rp.writeRequests(reqs); err != nil {
			xlog.Logger.Errorf("writeRequests: %v", err)
		}
		<-pendingCh
	}

	// This variable tracks the number of pending writes.
	//reqLen := new(expvar.Int)
	//y.PendingWrites.Set(db.opt.Dir, reqLen)

	reqs := make([]*request, 0, 10)
	for {
		var r *request

		select {
		case r = <-rp.writeCh:
		case <-rp.writeStopper.ShouldStop():
			goto closedCase
		}

		for {
			if r.isGCRequest{
				pendingCh <- struct{}{} // blocking.
				if len(reqs) > 0 {
					writeRequests(reqs)
				}
				reqs = make([]*request, 1)
				reqs[0] = r
				goto writeCase
				
			} else {
				reqs = append(reqs, r)
			}

			//FIXME: if (reqs + r) too big, send reqs, and create new reqs including r
			//if req's entries are gc request, it means reqs is big enough, send it
			//the last element of reqs is gc request, writeRequests MUST check last element first
			if rp.isReqsTooBig(reqs) {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-rp.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-rp.writeStopper.ShouldStop():
				goto closedCase
			}

		}

	closedCase:
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		for {
			select {
			case r = <-rp.writeCh:
				reqs = append(reqs, r)
				//因为close时, 一定先停GC, 所以reqs不会有gc request, 我们把所有req
				//收集到, 然后提交
			default:
				pendingCh <- struct{}{} // Push to pending before doing a write.
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
	}
}

func (rp *RangePartition) ensureRoomForWrite(entries []*pb.EntryInfo, head valuePointer) error {

	rp.Lock()
	defer rp.Unlock()

	n := int64(0)
	for i := range entries {
		n += int64(estimatedSizeInSkl(entries[i].Log))
	}

	utils.AssertTrue(n <= rp.opt.MaxSkipList)

	if rp.mt.MemSize() + n < rp.opt.MaxSkipList {
		return nil
	}
	utils.AssertTrue(rp.mt != nil)
	xlog.Logger.Debugf("Flushing memtable, mt.size=%d", rp.mt.MemSize())

	seqNum := atomic.AddUint64(&rp.seqNumber, 1)
	//non-block, block if flushChan is full,
	select {
	case rp.flushChan <- flushTask{mt: rp.mt, vptr: head, seqNum: seqNum, isCompact: false}:
		// After every memtable flush, let's reset the counter.

		// Ensure value log is synced to disk so this memtable's contents wouldn't be lost.

		xlog.Logger.Debugf("Flushing memtable, mt.size=%d size of flushChan: %d\n",
			rp.mt.MemSize(), len(rp.flushChan))
		rp.imm = append(rp.imm, rp.mt)

		rp.mt = skiplist.NewSkiplist(rp.opt.MaxSkipList)
		// New memtable is empty. We certainly have room.

		return nil
	default:
		// We need to do this to unlock and allow the flusher to modify imm.
		return errNoRoom
	}
}

func (rp *RangePartition) newIterator() y.Iterator {
	//prefix不包括seqnum
	//FIXME: 是否实现prefetch?

	var iters []y.Iterator

	mts, decr := rp.getMemTables()
	defer decr()

	//memtable iters
	for i := 0; i < len(mts); i++ {
		iters = append(iters, mts[i].NewUniIterator(false))
	}

	rp.tableLock.RLock()
	for i := len(rp.tables) - 1; i >= 0; i-- {
		iters = append(iters, rp.tables[i].NewIterator(false))
	}
	rp.tableLock.RUnlock()
	return table.NewMergeIterator(iters, false)
}

func (rp *RangePartition) getTablesForKey(userKey []byte) []*table.Table {
	var out []*table.Table

	rp.tableLock.RLock()
	defer rp.tableLock.RUnlock()
	for _, t := range rp.tables {
		start := y.ParseKey(t.Smallest())
		end := y.ParseKey(t.Biggest())
		if bytes.Compare(userKey, start) >= 0 && bytes.Compare(userKey, end) <= 0 {
			out = append(out, t)
		}
	}
	//返回的tables按照SeqNum排序, SeqNum大的在前(新的table在前), 保证如果出现vesion相同
	//的key的情况下, 总是找到新的key(为什么会出现version相同的key? 从valuelog gc而来

	if len(out) > 1 {
		sort.Slice(out, func(i, j int) bool {
			return out[i].LastSeq > out[j].LastSeq
		})
	}

	return out
}

func (rp *RangePartition) Range(prefix []byte, start []byte, limit uint32) [][]byte {

	hasOverLap := atomic.LoadUint32(&rp.hasOverlap) == 1
	iter := rp.newIterator()
	defer iter.Close()
	var out [][]byte
	var skipKey []byte //note:包括seqnum
	startTs := y.KeyWithTs(start, atomic.LoadUint64(&rp.seqNumber))
	//readTs: 是否以后支持readTs:如果version比readTS大, 则忽略这个版本
	for iter.Seek(startTs); iter.Valid() && uint32(len(out)) < limit; iter.Next() {

		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}

		userKey := y.ParseKey(iter.Key())

		if hasOverLap {
			//if hasOverLap && userKey less than rp.startKey, then continue
			//这个情况一般不会出现,上次client会找到之前的range,不会到当前range
			if bytes.Compare(userKey, rp.StartKey) < 0 {
				continue
			}
			//if hasOverLap && userKey greater than rp.endKey, then break
			if len(rp.EndKey) > 0 && bytes.Compare(userKey, rp.EndKey) >= 0 {
				break
			}
		}

		if len(skipKey) > 0 {
			if y.SameKey(iter.Key(), skipKey) {
				continue
			} else {
				skipKey = skipKey[:0] //reset
			}
		}
		skipKey = y.SafeCopy(skipKey, iter.Key())

		vs := iter.Value()

		if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
			continue
		}
		//FIXME:slab allocation key
		out = append(out, y.Copy(userKey))
	}
	return out
}

func (rp *RangePartition) Get(userKey []byte) ([]byte, error) {

	vs := rp.getValueStruct(userKey, 0)

	if vs.Version == 0 {
		return nil, errNotFound
	} else if vs.Meta&y.BitDelete > 0 {
		return nil, errNotFound
	}

	if vs.Meta&y.BitValuePointer > 0 {

		var vp valuePointer
		vp.Decode(vs.Value)
		//fmt.Printf("%s's location is [%d, %d]\n", userKey, vp.extentID, vp.offset)
		blocks, _, err := rp.blockReader.Read(context.Background(), vp.extentID, vp.offset, 1, streamclient.HintReadThrough)
		if err != nil {
			return nil, err
		}

		entry := y.ExtractLogEntry(blocks[0])
		return entry.Value, nil
	}
	return vs.Value, nil

}

func (rp *RangePartition) getValueStruct(userKey []byte, version uint64) y.ValueStruct {

	mtables, decr := rp.getMemTables()
	defer decr()

	var internalKey []byte

	if version == 0 {
		//find the latest version
		internalKey = y.KeyWithTs(userKey, atomic.LoadUint64(&rp.seqNumber))
	} else {
		internalKey = y.KeyWithTs(userKey, version)
	}

	//search in rp.mt and rp.imm
	for i := 0; i < len(mtables); i++ {
		//samekey and the vs is the smallest bigger than req's version
		vs := mtables[i].Get(internalKey)

		//fmt.Printf("search result of internalKey %s:%d is %+v\n",userKey, y.ParseTs(internalKey),  vs)
		if vs.Meta == 0 && vs.Value == nil {
			//not found, userKey not match
			continue
		}

		// Found the required version of the key, return immediately.
		if version > 0  && vs.Version == version {
			return vs
		}

		if version == 0 && vs.Version > 0 {
			return vs
		}
	}

	tables := rp.getTablesForKey(userKey)
	hash := farm.Fingerprint64(userKey)

	var maxVs y.ValueStruct
	for _, th := range tables {
		if th.DoesNotHave(hash) {
			continue
		}
		iter := th.NewIterator(false)
		defer iter.Close()
		iter.Seek(internalKey)
		if !iter.Valid() {
			continue
		}

		if y.SameKey(internalKey, iter.Key()) {
			vsVersion := y.ParseTs(iter.Key()); 
			if version > 0 && vsVersion == version {
				maxVs = iter.ValueCopy()
				maxVs.Version = version
				break
			}
			if version == 0 && vsVersion > 0 {
				maxVs = iter.ValueCopy()
				maxVs.Version = vsVersion
				break
			}
		}
	}
	return maxVs
}

func (rp *RangePartition) Close() error {
	var err error
	rp.closeOnce.Do(func() {
		err = rp.close(true)
	})
	return err
}

func (rp *RangePartition) close(gracefull bool) error {
	xlog.Logger.Infof("Closing RangePartion %d", rp.PartID)
	atomic.StoreInt32(&rp.blockWrites, 1)

	//FIXME: stop GC first
	rp.gcStopper.Stop()
	rp.compactStopper.Stop()
	rp.writeStopper.Stop()
	close(rp.writeCh)

	//FIXME lost data in mt/imm, will have to replay log
	//doWrite在返回前,会调用最后一次writeRequest并且等待返回, 所以这里
	//mt和rp.vhead都是只读的
	//106是空的skiplist的只有头node的大小
	if gracefull && rp.mt.MemSize() > 106 {
		for {
			pushedFlushTask := func() bool {
				rp.Lock()
				defer rp.Unlock()
				utils.AssertTrue(rp.mt != nil)
				select {
				case rp.flushChan <- flushTask{mt: rp.mt, vptr: rp.vhead, seqNum: rp.seqNumber + 1}:
					//fmt.Printf("Gracefull stop: submitted to flushkask vp %+v\n", rp.vhead)
					rp.imm = append(rp.imm, rp.mt) // Flusher will attempt to remove this from s.imm.
					rp.mt = nil                    // Will segfault if we try writing!
					return true
				default:
					// If we fail to push, we need to unlock and wait for a short while.
					// The flushing operation needs to update s.imm. Otherwise, we have a deadlock.
					// TODO: Think about how to do this more cleanly, maybe without any locks.
				}
				return false
			}()
			if pushedFlushTask {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	close(rp.flushChan)
	//只有读到rp.flushChan close的时候,才退出, 可以保证
	//flushChan里面的任何都已经执行完了
	rp.flushStopper.Wait()

	return nil
}

func (rp *RangePartition) WriteAsync(key, value []byte, f func(error)) {

	newSeqNumber := atomic.AddUint64(&rp.seqNumber, 1)

	e := &pb.EntryInfo{
		Log: &pb.Entry{
			Key:   y.KeyWithTs(key, newSeqNumber),
			Value: value,
		},
	}

	req, err := rp.sendToWriteCh([]*pb.EntryInfo{e}, false)
	if err != nil {
		f(err)
		return
	}
	go func() {
		err := req.Wait()
		// Write is complete. Let's call the callback function now.
		f(err)
	}()

}

//submit a major compaction task 
func (rp *RangePartition) SubmitCompaction() error{
	select {
	case rp.majorCompactChan <- struct{}{}:
		return nil
	default:
		return errors.New("major compaction busy")
	}
}

func (rp *RangePartition) SubmitGC() error {
	select {
	case rp.gcRunChan <- struct{}{}:
		return nil
	default:
		return errors.New("gc busy")
	}
}

func (rp *RangePartition) Delete(key []byte) error {
	//search
	vs := rp.getValueStruct(key, 0)
	if vs.Version == 0 || vs.Meta&y.BitDelete > 0 {
		return errNotFound
	}

	newSeqNumber := atomic.AddUint64(&rp.seqNumber, 1)
	e := &pb.EntryInfo{
		Log: &pb.Entry{
			Key:  y.KeyWithTs(key, newSeqNumber),
			Meta: uint32(y.BitDelete),
		},
	}

	req, err := rp.sendToWriteCh([]*pb.EntryInfo{e}, false)
	if err != nil {
		return err
	}
	return req.Wait()
}



//检查是否所有tables全局有序, 否则panic
func (rp *RangePartition) CheckTableOrder(out []*table.Table) {

	tbls := make([]*table.Table, 0, len(out))
	copy(tbls, out)
	//sort tbls by lastSeq
	sort.Slice(tbls, func(i, j int) bool {
		return tbls[i].LastSeq > tbls[j].LastSeq
	})
	//
	index := make(map[string]uint64)

	if len(tbls) < 2 {
		return
	}
	
	for _, tbl := range tbls {
		it := tbl.NewIterator(false)
		for ; it.Valid(); it.Next() {
			userKey := y.ParseKey(it.Key())
			ts := y.ParseTs(it.Key())
			if prevTs, ok := index[string(userKey)]; ok {
				if prevTs < ts {
					panic(fmt.Sprintf("table order error, key is %s ,prevTs %d, ts %d on table %d", 
					string(userKey), prevTs, ts, tbl.LastSeq))
				}
			} else {
				index[string(userKey)] = ts
			}
		}
	}

}



//req.Wait will free the request
func (rp *RangePartition) Write(key, value []byte) error {
	newSeqNumber := atomic.AddUint64(&rp.seqNumber, 1)

	e := &pb.EntryInfo{
		Log: &pb.Entry{
			Key:   y.KeyWithTs(key, newSeqNumber),
			Value: value,
		},
	}
	req, err := rp.sendToWriteCh([]*pb.EntryInfo{e}, false)
	if err != nil {
		return err
	}
	return req.Wait()
}

//block API
func (rp *RangePartition) sendToWriteCh(entries []*pb.EntryInfo, isGC bool) (*request, error) {
	if atomic.LoadInt32(&rp.blockWrites) == 1 {
		return nil, ErrBlockedWrites
	}

	req := requestPool.Get().(*request)
	req.reset()

	req.entries = entries

	req.wg.Add(1)
	req.IncrRef()
	req.isGCRequest = isGC
	rp.writeCh <- req
	return req, nil
}

const vptrSize = unsafe.Sizeof(valuePointer{})

type valuePointer struct {
	extentID uint64
	offset   uint32
	len      uint32
}

// Encode encodes Pointer into byte buffer.
func (p valuePointer) Encode() []byte {
	b := make([]byte, vptrSize)
	// Copy over the content from p to b.
	*(*valuePointer)(unsafe.Pointer(&b[0])) = p
	return b
}

// Decode decodes the value pointer into the provided byte buffer.
func (p *valuePointer) Decode(b []byte) {
	// Copy over data from b into p. Using *p=unsafe.pointer(...) leads to
	// pointer alignment issues. See https://github.com/dgraph-io/badger/issues/1096
	// and comment https://github.com/dgraph-io/badger/pull/1097#pullrequestreview-307361714
	copy(((*[vptrSize]byte)(unsafe.Pointer(p))[:]), b[:vptrSize])
}