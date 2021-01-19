package rangepartition

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dgryski/go-farm"
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
	ValueThreshold    = 4 * KB
	maxEntriesInBlock = 100
	maxMixedBlockSize = 4 * KB

	//meta
	bitDelete       byte = 1 << 0 // Set if the key has been deleted.
	bitValuePointer byte = 1 << 1 // Set if the value is NOT stored directly next to key.

	maxSkipList = 2 * KB
)

var (
	errNoRoom   = errors.New("No room for write")
	errNotFound = errors.New("not found")
)

type RangePartition struct {
	//metaStream *streamclient.StreamClient //设定metadata结构
	rowStream  streamclient.StreamClient //设定checkpoint结构
	logStream  streamclient.StreamClient
	logRotates int32
	writeCh    chan *request
	mt         *skiplist.Skiplist
	imm        []*skiplist.Skiplist
	utils.SafeMutex
	//sync.RWMutex //protect mt,imm when swapping mt, imm
	flushStopper *utils.Stopper
	flushChan    chan flushTask
	writeStopper *utils.Stopper
	tableLock    sync.RWMutex //protect tables
	tables       []*table.Table
	seqNumber    uint64
}

func OpenRangePartition(rowStream streamclient.StreamClient, logStream streamclient.StreamClient) *RangePartition {
	rp := &RangePartition{
		rowStream:  rowStream,
		logStream:  logStream,
		logRotates: 0,
		mt:         skiplist.NewSkiplist(maxSkipList),
		imm:        nil,
	}
	rp.startMemoryFlush()
	rp.startWriteLoop()
	return rp
}

type flushTask struct {
	mt   *skiplist.Skiplist
	vptr valuePointer
}

func (rp *RangePartition) startWriteLoop() {
	rp.writeStopper = utils.NewStopper()
	rp.writeCh = make(chan *request, 16)

	rp.writeStopper.RunWorker(rp.doWrites)

}
func (rp *RangePartition) startMemoryFlush() {
	// Start memory fluhser.
	//if db.closers.memtable != nil {
	rp.flushChan = make(chan flushTask, 1)
	flushStopper := utils.NewStopper()
	flushStopper.RunWorker(rp.flushMemtable)
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
	id, offset, err := b.FinishAll(ft.vptr.extentID, ft.vptr.offset)
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
	//
	xlog.Logger.Debugf("Flushed from %s to %s on vlog %+v\n", first, last, ft.vptr)
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
	}
}

//read metaStream, connect commitlog, rowDataStreamand blobDataStream
func (rp *RangePartition) Connect() error {
	var err error
	return err
}

const vptrSize = unsafe.Sizeof(valuePointer{})

type valuePointer struct {
	extentID uint64
	offset   uint32
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

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

type request struct {
	// Input values
	pspb.Entry

	// Output values and wait group stuff below
	wg  sync.WaitGroup
	vp  valuePointer
	Err error
	ref int32
}

//key + valueStruct
func estimatedSizeInSkl(r *request) int {
	sz := len(r.Entry.Key)
	if shouldWriteValueToLSM(&r.Entry) {
		sz += len(r.Entry.Value) + 2 // Meta, UserMeta
	} else {
		sz += int(vptrSize) + 2 // vptrSize for ValuePointer, 2 for metas.
	}

	if r.ExpiresAt == 0 {
		sz += 1
	} else {
		sz += utils.SizeVarint(r.ExpiresAt)

	}
	sz += skiplist.MaxNodeSize + 8 //8 is nodeAlign
	return sz
}

//size in log
func (r *request) Size() int {
	return r.Entry.Size()
}

func (req *request) reset() {
	req.Entry.Reset()
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

type mixedBlock struct {
	offsets *pspb.MixedLog
	data    []byte
	tail    int
}

func NewMixedBlock() *mixedBlock {
	return &mixedBlock{
		offsets: new(pspb.MixedLog),
		data:    make([]byte, maxMixedBlockSize, maxMixedBlockSize),
		tail:    0,
	}
}

func (mb *mixedBlock) CanFill(entry *pspb.Entry) bool {
	if mb.tail+entry.Size() > maxMixedBlockSize || len(mb.offsets.Offsets) >= maxEntriesInBlock {
		return false
	}
	return true
}

func (mb *mixedBlock) Fill(entry *pspb.Entry) uint32 {
	mb.offsets.Offsets = append(mb.offsets.Offsets, uint32(mb.tail))
	entry.MarshalTo(mb.data[mb.tail:])
	offset := mb.tail
	mb.tail += entry.Size()
	return uint32(offset)
}

func (mb *mixedBlock) ToBlock() *pb.Block {
	mb.offsets.Offsets = append(mb.offsets.Offsets, uint32(mb.tail))
	userData, err := mb.offsets.Marshal()
	utils.Check(err)
	block := &pb.Block{
		BlockLength: uint32(maxMixedBlockSize),
		UserData:    userData,
		CheckSum:    utils.AdlerCheckSum(mb.data),
		Data:        mb.data,
	}
	return block
}

func (rp *RangePartition) writeLog(reqs []*request) (valuePointer, error) {

	utils.AssertTrue(len(reqs) != 0)
	//sort
	sort.Slice(reqs, func(i, j int) bool {
		return len(reqs[i].Value) < len(reqs[j].Value)
	})

	var blocks []*pb.Block
	var mblock *mixedBlock = nil
	i := 0

	//merge small reqs into on block
	for ; i < len(reqs); i++ {
		if !shouldWriteValueToLSM(&reqs[i].Entry) {
			break
		}
		if mblock == nil {
			mblock = NewMixedBlock()
		}
		if !mblock.CanFill(&reqs[i].Entry) {
			blocks = append(blocks, mblock.ToBlock())
			mblock = NewMixedBlock()
		}
		mblock.Fill(&reqs[i].Entry)
	}

	if mblock != nil {
		blocks = append(blocks, mblock.ToBlock())
	}

	j := i //j is start of Bigger Block
	//append bigger blocks, valuePointer points to a block
	for ; i < len(reqs); i++ {
		blockLength := utils.Ceil(uint32(reqs[i].Size()), 512)
		data := make([]byte, blockLength)
		reqs[i].Entry.MarshalTo(data)
		var mix pspb.MixedLog
		mix.Offsets = []uint32{uint32(reqs[i].Size())}
		blockUserData, err := mix.Marshal()
		utils.Check(err)

		blocks = append(blocks, &pb.Block{
			BlockLength: blockLength,
			UserData:    blockUserData,
			Data:        data,
			CheckSum:    utils.AdlerCheckSum(data),
		})
	}

	op, err := rp.logStream.Append(context.Background(), blocks, nil)
	defer op.Free()
	if err != nil {
		return valuePointer{}, err
	}
	op.Wait()
	if op.Err != nil {
		return valuePointer{}, op.Err
	}
	for ; j < len(reqs); j++ {
		reqs[j].vp = valuePointer{
			op.ExtentID,
			op.Offsets[j],
		}
	}
	utils.AssertTrue(len(op.Offsets) == len(blocks))
	utils.AssertTrue(len(op.Offsets) > 0)
	//endOfStream := op.Offsets[len(op.Offsets)-1] + blocks[len(blocks)-1].BlockLength
	//return head of this op on commitlog
	return valuePointer{
		op.ExtentID,
		op.Offsets[0],
	}, nil
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

	xlog.Logger.Info("Writing to memtable")
	i := 0
	for err = rp.ensureRoomForWrite(reqs, head); err == errNoRoom; err = rp.ensureRoomForWrite(reqs, head) {
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

func shouldWriteValueToLSM(e *pspb.Entry) bool {
	return len(e.Value) < ValueThreshold
}

func getLowerByte(a uint32) byte {
	return byte(a & 0x000000FF)
}

func (rp *RangePartition) writeToLSM(req *request) error {
	entry := &req.Entry
	if shouldWriteValueToLSM(&req.Entry) { // Will include deletion / tombstone case.
		rp.mt.Put(entry.Key,
			y.ValueStruct{
				Value:     entry.Value,
				Meta:      getLowerByte(entry.Meta),
				UserMeta:  getLowerByte(entry.UserMeta),
				ExpiresAt: entry.ExpiresAt,
			})
	} else {
		rp.mt.Put(entry.Key,
			y.ValueStruct{
				Value:     req.vp.Encode(),
				Meta:      getLowerByte(entry.Meta) | bitValuePointer,
				UserMeta:  getLowerByte(entry.UserMeta),
				ExpiresAt: entry.ExpiresAt,
			})
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

func (rp *RangePartition) ensureRoomForWrite(reqs []*request, head valuePointer) error {

	rp.Lock()
	defer rp.Unlock()

	forceFlush := atomic.LoadInt32(&rp.logRotates) >= 1
	n := int64(0)
	for i := range reqs {
		n += int64(estimatedSizeInSkl(reqs[i]))
	}
	if !forceFlush && rp.mt.MemSize()+n < maxSkipList {
		return nil
	}
	utils.AssertTrue(rp.mt != nil)
	xlog.Logger.Debugf("Flushing memtable, mt.size=%d", rp.mt.MemSize())

	//non-block, block if flushChan is full,
	select {
	case rp.flushChan <- flushTask{mt: rp.mt, vptr: head}:
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
	} else if vs.Meta&bitDelete > 0 {
		return nil, errNotFound
	}

	if vs.Meta&bitValuePointer > 0 {

		var vp valuePointer
		vp.Decode(vs.Value)

		blocks, err := rp.logStream.Read(context.Background(), vp.extentID, vp.offset, 1)
		utils.Check(err)
		var mix pspb.MixedLog
		utils.MustUnMarshal(blocks[0].UserData, &mix)
		eSize := mix.Offsets[0]
		var entry pspb.Entry
		utils.MustUnMarshal(blocks[0].Data[:eSize], &entry)
		return entry.Value, nil
		//read value and decode
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
	return nil
}

func (rp *RangePartition) writeAsync(key, value []byte, f func(error)) {
	req := rp.sendToWriteCh(key, value)
	go func() {
		err := req.Wait()
		// Write is complete. Let's call the callback function now.
		f(err)
	}()
}

func (rp *RangePartition) write(key, value []byte) error {
	req := rp.sendToWriteCh(key, value)
	return req.Wait()
}

//block API
func (rp *RangePartition) sendToWriteCh(key, value []byte) *request {
	req := requestPool.Get().(*request)
	req.reset()

	newSeqNumber := atomic.AddUint64(&rp.seqNumber, 1)

	req.Key = y.KeyWithTs(key, newSeqNumber)
	req.Value = value
	req.wg.Add(1)
	req.IncrRef()
	rp.writeCh <- req
	return req
}
