package range_partition

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/range_partition/skiplist"
	"github.com/journeymidnight/autumn/range_partition/table"
	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

type PickupTables interface {
	PickupTables(opt *Option, tbls []*table.Table) []*table.Table
}

type DefaultPickupPolicy struct {}


func (p DefaultPickupPolicy) PickupTables(opt *Option, tbls []*table.Table) []*table.Table {

}

func (rp *RangePartition) startCompact() {
	rp.compactStopper = utils.NewStopper()
	//only one compact goroutine
	rp.compactStopper.RunWorker(rp.compact)
}


func (rp *RangePartition) compact() {
	randTicker := utils.NewRandomTicker(10*time.Minute, 20*time.Minute)
	for {
		select {
		// Can add a done channel or other stuff.
		case <-randTicker.C:
			rp.tableLock.RLock()
			allTables := make([]*table.Table, 0, len(rp.tables))
			for _, t := range rp.tables {
				allTables = append(allTables, t)
				t.IncrRef()
			}
			rp.tableLock.RUnlock()
			compactTables := rp.pickTables(rp.opt, allTables)

			rp.doCompact(compactTables)
			for i := range allTables {
				allTables[i].DecrRef()
			}
		case <-rp.compactStopper.ShouldStop():
			randTicker.Stop()
			return
		}
	}
}

func (rp *RangePartition) deprecateTables(tbls []*table.Table) {

	rp.tableLock.Lock()
	defer rp.tableLock.Unlock()

	var i, j int
	var newTables []*table.Table
	for i < len(tbls) && j < len(rp.tables) {
		if tbls[i].LastSeq == rp.tables[j].LastSeq {
			i++
			j++ //同时存在
		} else if tbls[i].LastSeq < rp.tables[j].LastSeq {
			i++ //只在tbls存在
		} else {
			newTables = append(newTables, rp.tables[j])
			j++ //只在rp.tables存在
		}
	}

	for ; j < len(rp.tables); j++ {
		newTables = append(newTables, rp.tables[j])
	}

	var tableLocs []*pspb.Location
	for _, t := range newTables {
		tableLocs = append(tableLocs, &t.Loc)
	}
	rp.updateTableLocs(tableLocs)

	rp.tables = newTables
}

func (rp *RangePartition) doCompact(tbls []*table.Table, major bool) {
	if len(tbls) == 0 {
		return
	}
	//tbls的顺序是在stream里面的顺序

	var iters []y.Iterator
	var maxSeq uint64
	var head valuePointer
	for _, table := range tbls {
		if table.LastSeq > maxSeq {
			maxSeq = table.LastSeq
			head = valuePointer{extentID: table.VpExtentID, offset: table.VpOffset}
		}
		iters = append(iters, table.NewIterator(false))
	}

	it := table.NewMergeIterator(iters, false)
	defer it.Close()

	it.Rewind()

	//FIXME
	discardStats := make(map[uint32]int64)
	updateStats := func(vs y.ValueStruct) {
		if vs.Meta&y.BitValuePointer > 0 { //big Value
			var vp valuePointer
			vp.Decode(vs.Value)
			discardStats[uint32(vp.extentID)] += int64(vp.len)
		}
	}

	var numBuilds int
	resultCh := make(chan struct{})
	//ignore keep multiple versions and snapshot support
	capacity := int64(2 * rp.opt.MaxSkipList)
	for it.Valid() {
		var skipKey []byte
		timeStart := time.Now()
		var numKeys, numSkips uint64
		memStore := skiplist.NewSkiplist(capacity)
		for ; it.Valid(); it.Next() {

			userKey := y.ParseKey(it.Key())

			if !rp.IsUserKeyInRange(userKey) {
				continue
			}

			if len(skipKey) > 0 {
				if y.SameKey(it.Key(), skipKey) {
					updateStats(it.Value())
					numSkips++
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}

			vs := it.Value()

			skipKey = y.SafeCopy(skipKey, it.Key())

			if major && isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
				updateStats(it.Value())
				numSkips++
				continue
			}

			if memStore.MemSize()+int64(estimatedVS(it.Key(), it.Value())) > capacity {
				break
			}
			numKeys++
			memStore.Put(it.Key(), vs)
		}

		xlog.Logger.Debugf("LOG Compact %d tables Added %d keys. Skipped %d keys. Iteration took: %v, ", len(tbls),
			numKeys, numSkips, time.Since(timeStart))

		//这里的memstore没有用户会读, 所以不需要incref, 等待flushtask会decref, 自动释放内存
		//compact出来的table的seqNum一定
		rp.flushChan <- flushTask{mt: memStore, vptr: head, seqNum: maxSeq, isCompact: true, resultCh: resultCh}
		numBuilds++
	}

	//wait all numBuilds finished(saved in rowstream and saved in pm)
	for i := 0; i < numBuilds; i++ {
		<-resultCh
	}

	//FIXME
	eID := tbls[len(tbls)-1].Loc.ExtentID

	//last table's meta extentd
	_, err := rp.rowStream.Truncate(context.Background(), eID, "")
	if err != nil {
		xlog.Logger.Warnf("LOG Truncate extent %d error %v", eID, err)
	}

	//在这个时间, 虽然有可能memstore还没有完全刷下去, 但是rp.Close调用会等待flushTask全部执行完成.
	//另外, 在分裂时选择midKey后, 会有一个assert确保midKey在startKey和endKey之间
	if major {
		atomic.StoreUint32(&rp.hasOverlap, 0)
	}
}

func isDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&y.BitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}
