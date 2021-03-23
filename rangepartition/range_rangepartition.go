package rangepartition

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dgryski/go-farm"
	"github.com/journeymidnight/autumn/manager/pmclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/rangepartition/skiplist"
	"github.com/journeymidnight/autumn/rangepartition/table"
	"github.com/journeymidnight/autumn/rangepartition/y"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

const (
	KB                = 1024
	MB                = KB * 1024
	maxEntriesInBlock = 100

	//4 * KB for test
	maxSkipList     = 64 * MB
	writeChCapacity = 64
)

var (
	errNoRoom         = errors.New("No room for write")
	errNotFound       = errors.New("not found")
	ErrBlockedWrites  = errors.New("Writes are blocked, possibly due to DropAll or Close")
	maxEntriesInQueue = maxSkipList / (y.ValueThrottle + 20) / 2
)

type OpenStreamFunc func(si pb.StreamInfo) streamclient.StreamClient
type UpdateStreamFunc func([]pb.StreamInfo)

type RangePartition struct {
	discard *discardManager
	//metaStream *streamclient.StreamClient //设定metadata结构
	logStream streamclient.StreamClient
	//从ValueStruct中得到的地址, 用blockReader读出来, 因为它可能在[logStream, []blobStreams]里面
	//我们不需要读每个stream
	blockReader streamclient.BlockReader

	rowStream       streamclient.StreamClient
	logRotates      int32
	writeCh         chan *request
	blockWrites     int32
	mt              *skiplist.Skiplist
	imm             []*skiplist.Skiplist
	utils.SafeMutex //protect mt,imm when swapping mt, imm

	flushStopper   *utils.Stopper
	flushChan      chan flushTask
	writeStopper   *utils.Stopper
	compactStopper *utils.Stopper
	tableLock      utils.SafeMutex //protect tables
	tables         []*table.Table
	seqNumber      uint64

	PartID   uint64
	StartKey []byte
	EndKey   []byte
	pmClient pmclient.PMClient

	closeOnce    sync.Once    // For closing DB only once.
	vhead        valuePointer //vhead前的都在mt中
	openStream   OpenStreamFunc
	updateStream UpdateStreamFunc
}

//TODO
//interface KV save some values

func OpenRangePartition(id uint64, rowStream streamclient.StreamClient,
	logStream streamclient.StreamClient, blockReader streamclient.BlockReader,
	startKey []byte, endKey []byte, tableLocs []*pspb.Location, blobStreams []uint64,
	pmclient pmclient.PMClient,
	openStream OpenStreamFunc, updateStream UpdateStreamFunc,
) *RangePartition {
	rp := &RangePartition{
		rowStream:    rowStream,
		logStream:    logStream,
		blockReader:  blockReader,
		logRotates:   0,
		mt:           skiplist.NewSkiplist(maxSkipList),
		imm:          nil,
		blockWrites:  0,
		StartKey:     startKey,
		EndKey:       endKey,
		pmClient:     pmclient,
		PartID:       id,
		openStream:   openStream,
		updateStream: updateStream,
	}
	rp.startMemoryFlush()

	//replay log
	//open tables
	//tableLocs的顺序就是在logStream里面的顺序
	for _, tLoc := range tableLocs {
	retry:
		tbl, err := table.OpenTable(rp.rowStream, tLoc.ExtentID, tLoc.Offset)
		if err != nil {
			xlog.Logger.Error(err)
			time.Sleep(1 * time.Second)
			goto retry
		}
		rp.tables = append(rp.tables, tbl)
	}

	//search all tables, find the table who has the most latest seqNum
	seq := uint64(0)
	var lastTable *table.Table
	for _, table := range rp.tables {
		fmt.Printf("read table from [%s] to [%s]: seq[%d], vp[%d]\n", y.ParseKey(table.Smallest()), y.ParseKey(table.Biggest()), table.LastSeq, table.VpOffset)
		if table.LastSeq > seq {
			seq = table.LastSeq
			lastTable = table
		}
	}
	rp.seqNumber = seq

	//FIXME:poor performace: prefetch read will be better
	replayedLog := 0
	replay := func(ei *pb.EntryInfo) (bool, error) {
		replayedLog++
		//fmt.Printf("from log %v\n", ei)
		//build ValueStruct from EntryInfo
		entriesReady := []*pb.EntryInfo{ei}

		head := valuePointer{extentID: ei.ExtentID, offset: ei.Offset}

		if y.ParseTs(ei.Log.Key) > rp.seqNumber {
			rp.seqNumber++
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
		//fmt.Printf("replay %s, %d\n", ei.Log.Key, len(ei.Log.Value))
		rp.writeToLSM([]*pb.EntryInfo{ei})
		return true, nil
	}

	if lastTable == nil {
		replayLog(rp.logStream, 0, 0, true, replay)
	} else {
		fmt.Printf("replay log from vp offset [%d]\n", lastTable.VpOffset)
		/*
			rp.vhead = valuePointer{
				extentID: lastTable.VpExtentID,
				offset:   lastTable.VpOffset,
			}
		*/
		replayLog(rp.logStream, lastTable.VpExtentID, lastTable.VpOffset, true, replay)
	}
	fmt.Printf("replayed log number: %d\n", replayedLog)
	//start real write
	rp.startWriteLoop()
	return rp
}

type flushTask struct {
	mt        *skiplist.Skiplist
	vptr      valuePointer
	seqNum    uint64
	isCompact bool          //如果是compact任务, 不需要修改rp.mt
	resultCh  chan struct{} //也可以用wg, 但是防止未来还需要发数据
}

func (rp *RangePartition) startWriteLoop() {
	rp.writeStopper = utils.NewStopper()
	rp.writeCh = make(chan *request, writeChCapacity)

	rp.writeStopper.RunWorker(rp.doWrites)
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

	//todo
	tbl, err := table.OpenTable(rp.rowStream, id, offset)
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
		rp.tableLock.RUnlock()

		for {
			err := rp.pmClient.SetRowStreamTables(rp.PartID, tableLocs)
			if err != nil {
				xlog.Logger.Errorf("failed to set tableLocs for %d, retry...", rp.PartID)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break
		}
		if ft.isCompact {
			ft.resultCh <- struct{}{}
		}

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
	//A = "x", A:time = "y", A:md5 = "asdfasdf"
	entries []*pb.EntryInfo

	// Output values and wait group stuff below
	wg  sync.WaitGroup
	Err error
	ref int32
}

//key + valueStruct
func estimatedSizeInSkl(e *pb.Entry) int {
	sz := len(e.Key)
	if y.ShouldWriteValueToLSM(e) {
		sz += len(e.Value) + 2 // Meta, UserMeta
	} else {
		sz += int(vptrSize) + 2 // vptrSize for valuePointer, 2 for metas.
	}

	sz += utils.SizeVarint(e.ExpiresAt)

	sz += skiplist.MaxNodeSize + 8 //8 is nodeAlign
	return sz
}

func estimatedVS(key []byte, vs y.ValueStruct) int {
	sz := len(key)
	if vs.Meta&y.BitValuePointer == 0 && len(vs.Value) <= y.ValueThrottle {
		sz += len(vs.Value) + 2 // Meta, UserMeta
	} else {
		sz += int(vptrSize) + 2 // vptrSize for valuePointer, 2 for metas.
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

/*
func (rp *RangePartition) writeLog(reqs []*request) ([]*pb.EntryInfo, valuePointer, error) {
	if atomic.LoadInt32(&rp.blockWrites) == 1 {
		return nil, valuePointer{}, ErrBlockedWrites
	}
	utils.AssertTrue(len(reqs) != 0)

	return writeValueLog(rp.logStream, reqs)
}
*/

// writeRequests is called serially by only one goroutine.
func (rp *RangePartition) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
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
					UserMeta:  getLowerByte(entry.Log.UserMeta),
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
					UserMeta:  getLowerByte(entry.Log.UserMeta),
					ExpiresAt: entry.Log.ExpiresAt,
				})
		}
	}
	return nil
}

func isReqsTooBig(reqs []*request) bool {
	//grpc limit
	size := 0 //network size in grpc
	n := 0
	for _, req := range reqs {
		size += req.Size()
		if size > 30*MB {
			return true
		}

		n += len(req.entries)
		if n > 3*writeChCapacity {
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
			reqs = append(reqs, r)
			if isReqsTooBig(reqs) {
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

				if isReqsTooBig(reqs) {
					pendingCh <- struct{}{} // blocking.
					goto writeCase
				}

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

	forceFlush := atomic.LoadInt32(&rp.logRotates) >= 1
	n := int64(0)
	for i := range entries {
		n += int64(estimatedSizeInSkl(entries[i].Log))
	}

	utils.AssertTrue(n <= maxSkipList)

	if !forceFlush && rp.mt.MemSize()+n < maxSkipList {
		return nil
	}
	utils.AssertTrue(rp.mt != nil)
	xlog.Logger.Debugf("Flushing memtable, mt.size=%d", rp.mt.MemSize())

	seqNum := atomic.AddUint64(&rp.seqNumber, 1)
	//non-block, block if flushChan is full,
	select {
	case rp.flushChan <- flushTask{mt: rp.mt, vptr: head, seqNum: seqNum, isCompact: false}:
		// After every memtable flush, let's reset the counter.
		atomic.StoreInt32(&rp.logRotates, 0)

		// Ensure value log is synced to disk so this memtable's contents wouldn't be lost.

		xlog.Logger.Debugf("Flushing memtable, mt.size=%d size of flushChan: %d\n",
			rp.mt.MemSize(), len(rp.flushChan))
		rp.imm = append(rp.imm, rp.mt)

		rp.mt = skiplist.NewSkiplist(maxSkipList)
		// New memtable is empty. We certainly have room.

		return nil
	default:
		// We need to do this to unlock and allow the flusher to modify imm.
		return errNoRoom
	}
}

func (rp *RangePartition) getTablesForKey(userKey []byte) ([]*table.Table, func()) {
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
	//返回的tables安装.SeqNUm排序, SeqNum大的在前(新的table在前), 保证如果出现vesion相同
	//的key的情况下, 总是找到新的key(为什么会出现version相同的key? 从valuelog gc而来
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSeq > out[j].LastSeq
	})

	return out, func() {
		for _, t := range out {
			t.DecrRef()
		}
	}
}

func (rp *RangePartition) Get(userKey []byte, version uint64) ([]byte, error) {

	vs := rp.getValueStruct(userKey, version)

	if vs.Version == 0 {
		return nil, errNotFound
	} else if vs.Meta&y.BitDelete > 0 {
		return nil, errNotFound
	}

	if vs.Meta&y.BitValuePointer > 0 {

		var vp valuePointer
		vp.Decode(vs.Value)

		blocks, err := rp.blockReader.Read(context.Background(), vp.extentID, vp.offset, 1)
		if err != nil {
			return nil, err
		}

		entries := y.ExtractLogEntry(blocks[0])
		return entries[0].Value, nil
	}
	return vs.Value, nil

}

//internal APIs/block
func (rp *RangePartition) getValueStruct(userKey []byte, version uint64) y.ValueStruct {

	mtables, decr := rp.getMemTables()
	defer decr()

	var internalKey []byte
	if version == 0 {
		internalKey = y.KeyWithTs(userKey, atomic.LoadUint64(&rp.seqNumber))
	} else {
		internalKey = y.KeyWithTs(userKey, version)

	}
	//search in rp.mt and rp.imm
	for i := 0; i < len(mtables); i++ {
		//samekey and the vs is the smallest bigger than req's version
		vs := mtables[i].Get(internalKey)

		if vs.Meta == 0 && vs.Value == nil {
			//not found, userKey not match
			continue
		}
		return vs
	}

	var maxVs y.ValueStruct
	tables, decr := rp.getTablesForKey(userKey)
	defer decr()
	hash := farm.Fingerprint64(userKey)
	//search each tables to find element which has the max version
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
			if version := y.ParseTs(iter.Key()); version > maxVs.Version {
				maxVs = iter.ValueCopy()
				maxVs.Version = version
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
	xlog.Logger.Infof("Closing database")
	atomic.StoreInt32(&rp.blockWrites, 1)

	rp.writeStopper.Stop()
	close(rp.writeCh)

	//FIXME lost data in mt/imm, will have to replay log
	//doWrite在返回前,会调用最后一次writeRequest并且等待返回, 所以这里
	//mt和rp.vhead都是只读的
	if gracefull && !rp.mt.Empty() {
		for {
			pushedFlushTask := func() bool {
				rp.Lock()
				defer rp.Unlock()
				utils.AssertTrue(rp.mt != nil)
				select {
				case rp.flushChan <- flushTask{mt: rp.mt, vptr: rp.vhead, seqNum: rp.seqNumber + 1}:
					fmt.Printf("submitted to chan vp %d\n", rp.vhead)
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

	req, err := rp.sendToWriteCh([]*pb.EntryInfo{e})
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

	req, err := rp.sendToWriteCh([]*pb.EntryInfo{e})
	if err != nil {
		return err
	}
	return req.Wait()
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
	req, err := rp.sendToWriteCh([]*pb.EntryInfo{e})
	if err != nil {
		return err
	}
	return req.Wait()
}

//block API
func (rp *RangePartition) sendToWriteCh(entries []*pb.EntryInfo) (*request, error) {
	if atomic.LoadInt32(&rp.blockWrites) == 1 {
		return nil, ErrBlockedWrites
	}

	req := requestPool.Get().(*request)
	req.reset()

	req.entries = entries

	req.wg.Add(1)
	req.IncrRef()
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
