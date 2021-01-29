package rangepartition

import (
	"bytes"
	"context"
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

	maxSkipList = 2 * KB
)

var (
	errNoRoom        = errors.New("No room for write")
	errNotFound      = errors.New("not found")
	ErrBlockedWrites = errors.New("Writes are blocked, possibly due to DropAll or Close")
)

type RangePartition struct {
	//metaStream *streamclient.StreamClient //设定metadata结构
	blobStreams []streamclient.StreamClient
	logStream   streamclient.StreamClient
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

	flushStopper *utils.Stopper
	flushChan    chan flushTask
	writeStopper *utils.Stopper
	tableLock    utils.SafeMutex //protect tables
	tables       []*table.Table
	seqNumber    uint64

	PARTID   uint64
	startKey []byte
	endKey   []byte
	pmClient pmclient.PMClient
}

//TODO
//interface KV save some values

func OpenRangePartition(rowStream streamclient.StreamClient,
	logStream streamclient.StreamClient, blockReader streamclient.BlockReader,
	startKey []byte, endKey []byte, tableLocs []*pspb.TableLocation, blobStreams []streamclient.StreamClient,
	pmclient pmclient.PMClient) *RangePartition {
	rp := &RangePartition{
		rowStream:   rowStream,
		logStream:   logStream,
		blockReader: blockReader,
		logRotates:  0,
		mt:          skiplist.NewSkiplist(maxSkipList),
		imm:         nil,
		blockWrites: 0,
		startKey:    startKey,
		endKey:      endKey,
		pmClient:    pmclient,
		blobStreams: blobStreams,
	}
	rp.startMemoryFlush()

	rp.startWriteLoop()

	//replay log
	//open tables
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

	//search all tables, find the table who has the most lasted seqNum
	seq := uint64(0)
	var lastTable *table.Table
	for _, table := range rp.tables {
		if table.LastSeq > seq {
			seq = table.LastSeq
			lastTable = table
		}
	}

	replay := func(ei *pb.EntryInfo) bool {

	}

	if lastTable == nil {
		replayLog(rp.logStream, 0, 0, replay)
	} else {
		replayLog(rp.logStream, lastTable.VpExtentID, lastTable.VpOffset, replay)
	}

	//set Newest SeqNum

	//start real write
	return rp
}

type flushTask struct {
	mt     *skiplist.Skiplist
	vptr   valuePointer
	seqNum uint64
}

func (rp *RangePartition) startWriteLoop() {
	rp.writeStopper = utils.NewStopper()
	rp.writeCh = make(chan *request, 16)

	rp.writeStopper.RunWorker(rp.doWrites)

}
func (rp *RangePartition) startMemoryFlush() {
	// Start memory fluhser.

	rp.flushStopper = utils.NewStopper()
	rp.flushChan = make(chan flushTask, 1)

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
	//

	rp.tableLock.Lock()
	rp.tables = append(rp.tables, tbl)
	rp.tableLock.Unlock()

	xlog.Logger.Debugf("Flushed from %s to %s on vlog %+v\n", y.FormatKey(first), y.FormatKey(last), ft.vptr)

	//
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
				// Update s.imm. Need a lock.
				rp.Lock()

				utils.AssertTrue(ft.mt == rp.imm[0])
				rp.imm = rp.imm[1:]
				ft.mt.DecrRef() // Return memory.
				rp.Unlock()
				break
			}
			// Encountered error. Retry indefinitely.
			xlog.Logger.Errorf("Failure while flushing memtable to disk: %v. Retrying...", err)
			time.Sleep(time.Second)
		}

		//save table offset in PM
		var tableLocs []*pspb.TableLocation
		rp.tableLock.RLock()
		for _, t := range rp.tables {
			tableLocs = append(tableLocs, &t.Loc)
		}
		rp.tableLock.RUnlock()

		for {
			err := rp.pmClient.SetTables(rp.PARTID, tableLocs)
			if err != nil {
				xlog.Logger.Errorf("failed to set tableLocs for %d, retry...", rp.PARTID)
				time.Sleep(1 * time.Second)
				continue
			}
			break
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

func estimatedSizeInSkl(es []*pb.EntryInfo) int {
	size := 0
	for i := range es {
		size += _estimatedSizeInSkl(es[i].Log)
	}
	return size
}

//key + valueStruct
func _estimatedSizeInSkl(e *pb.Entry) int {
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

func (rp *RangePartition) writeLog(reqs []*request) (valuePointer, error) {
	if atomic.LoadInt32(&rp.blockWrites) == 1 {
		return valuePointer{}, ErrBlockedWrites
	}
	utils.AssertTrue(len(reqs) != 0)

	return writeValueLog(rp.logStream, reqs)
}

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

	xlog.Logger.Infof("writeRequests called. Writing to log, len[%d]", len(reqs))

	head, err := rp.writeLog(reqs)
	if err != nil {
		done(err)
		return err
	}

	//lastReq := reqs[len(reqs)-1]
	//e := lastReq.entries[len(lastReq.entries)-1]
	//seqNum := y.ParseTs(e.Log.Key)
	seqNum := atomic.AddUint64(&rp.seqNumber, 1)

	xlog.Logger.Info("Writing to memtable")
	i := 0
	for err = rp.ensureRoomForWrite(reqs, head, seqNum); err == errNoRoom; err = rp.ensureRoomForWrite(reqs, head, seqNum) {
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
		if err := rp.writeToLSM(b); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
	}

	done(nil)
	return nil
}

func getLowerByte(a uint32) byte {
	return byte(a & 0x000000FF)
}

func (rp *RangePartition) writeToLSM(req *request) error {
	for _, entry := range req.entries {
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
	var size int
	for {
		var r *request
		size = 0
		select {
		case r = <-rp.writeCh:
		case <-rp.writeStopper.ShouldStop():
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			size += r.Size()
			if size > 20*MB {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}
			if len(reqs) >= 10 {
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
		size = 0
		for {
			select {
			case r = <-rp.writeCh:
				reqs = append(reqs, r)
				size += r.Size()
				if size > 20*MB {
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

func (rp *RangePartition) ensureRoomForWrite(reqs []*request, head valuePointer, seqNum uint64) error {

	rp.Lock()
	defer rp.Unlock()

	forceFlush := atomic.LoadInt32(&rp.logRotates) >= 1
	n := int64(0)
	for i := range reqs {
		n += int64(estimatedSizeInSkl(reqs[i].entries))
	}
	if !forceFlush && rp.mt.MemSize()+n < maxSkipList {
		return nil
	}
	utils.AssertTrue(rp.mt != nil)
	xlog.Logger.Debugf("Flushing memtable, mt.size=%d", rp.mt.MemSize())

	//non-block, block if flushChan is full,
	select {
	case rp.flushChan <- flushTask{mt: rp.mt, vptr: head, seqNum: seqNum}:
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
	return out, func() {
		for _, t := range out {
			t.DecrRef()
		}
	}
}

func (rp *RangePartition) get(userKey []byte, version uint64) ([]byte, error) {

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
		/*
			utils.Check(err)
			var mix pspb.MixedLog
			utils.MustUnMarshal(blocks[0].UserData, &mix)
			eSize := mix.Offsets[0]
			var entry pb.Entry
			utils.MustUnMarshal(blocks[0].Data[:eSize], &entry)
			return entry.Value, nil

			//read value and decode
		*/
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
	xlog.Logger.Infof("Closing database")
	atomic.StoreInt32(&rp.blockWrites, 1)

	rp.writeStopper.Stop()
	close(rp.writeCh)

	//FIXME lost data in mt/imm

	close(rp.flushChan)
	rp.flushStopper.Wait()
	return nil
}

func (rp *RangePartition) writeAsync(key, value []byte, f func(error)) {

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

//req.Wait will free the request
func (rp *RangePartition) write(key, value []byte) error {
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
}

func (p valuePointer) Empty() bool {
	return p.extentID == 0 && p.offset == 0
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
