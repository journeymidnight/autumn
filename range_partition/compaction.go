package range_partition

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/range_partition/skiplist"
	"github.com/journeymidnight/autumn/range_partition/table"
	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

//given all tables in range partition, if eID is not 0, caller can call stream.Truncate(eID) to
//truncate the stream, in our system, every extentID is bigger than 0.

type PickupTables interface {
	PickupTables(tbls []*table.Table) (tables []*table.Table, eID uint64)
}

type DefaultPickupPolicy struct {
	compactRatio float64
	headRatio    float64
	n int //at most n tables to be merged
	MaxSkipList int64
}


func (p DefaultPickupPolicy) PickupTables(tbls []*table.Table) ([]*table.Table, uint64) {

	utils.AssertTruef(p.n > 1, "PickupPolicy: n must be greater than 1")

	//rule1:
	//if tables are on multiple extents, and the table size on the first extent is less then headRatio * all table size
	//pickup all tables on the first extent and the first table on the second extent
	//if we have merged the first table on the second extent, we can truncate the first extent safely
	totalSize := uint64(0)
	extents := make([]uint64, 0, len(tbls))
	accumulatedSize := make(map[uint64]uint64)
	chosenTbls := make([]*table.Table, 0, p.n)
	for _, t := range tbls {
		totalSize += t.EstimatedSize
		extents = append(extents, t.Loc.ExtentID)
		accumulatedSize[t.Loc.ExtentID] += t.EstimatedSize
	}

	if len(tbls) > 1 && accumulatedSize[extents[0]] < uint64(math.Round(p.headRatio*float64(totalSize))) {
		chosenExtentID := extents[0]
		var truncateID uint64
		//find all tables on chosenExtentID and table on the first extent
		var i int
		for i = 0; i < len(tbls) && i < p.n ; i++ {
			if extents[i] == chosenExtentID {
				chosenTbls = append(chosenTbls, tbls[i])
			} else if extents[i] != chosenExtentID {
				chosenTbls = append(chosenTbls, tbls[i])
				truncateID = tbls[i].Loc.ExtentID
				break
			}
		}
		/*
		If we just merged ALL the tables on the first extent, and did not reach the second extente
		in the next merge, we can still not reclaim the first extent.
		So if last table of chosenTbls is the last table on the first extent, we exclude this table.
		when next minor compaction happens, we will reach the second extent and truncate the first extent.
		*/
		//at least chosenTbles have two elements
		lastTableExtentID := chosenTbls[len(chosenTbls)-1].Loc.ExtentID
		if lastTableExtentID == chosenExtentID && i < len(tbls) && lastTableExtentID != tbls[i].Loc.ExtentID {
			chosenTbls = chosenTbls[:len(chosenTbls)-1]
			truncateID = 0
		}
		return chosenTbls, truncateID
	}

	//rule2:
	//chose any table whose size is less than compactRatio * MaxSkipListSize, at most n tables
	for i := 0; i < len(tbls) && i < p.n; i++ {
		if tbls[i].EstimatedSize < uint64(math.Round(p.compactRatio*float64(p.MaxSkipList))) {
			chosenTbls = append(chosenTbls, tbls[i])
		}
	}
	if len(chosenTbls) > 1 {
		return chosenTbls, 0
	}
	return nil, 0
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
			}
			rp.tableLock.RUnlock()

			compactTables, eID := rp.pickupTablePolicy.PickupTables(allTables)
			if compactTables == nil {
				continue
			}
			rp.doCompact(compactTables, false)
			rp.removeTables(compactTables)
			//truncate
			if eID != 0 {
				//last table's meta extentd
				_, err := rp.rowStream.Truncate(context.Background(), eID, "")
				if err != nil {
					xlog.Logger.Warnf("LOG Truncate extent %d error %v", eID, err)
				}
			}
		case <-rp.compactStopper.ShouldStop():
			randTicker.Stop()
			return
		}
	}
}

func (rp *RangePartition) removeTables(tbls []*table.Table) {

	rp.tableLock.Lock()
	defer rp.tableLock.Unlock()

	tblsIndex := make(map[string]bool)
	for _, t := range tbls {
		tblsIndex[fmt.Sprintf("%d-%d", t.Loc.ExtentID, t.Loc.Offset)] = true
	}

	for i := len(rp.tables) - 1; i >= 0; i-- {
		if _, ok := tblsIndex[fmt.Sprintf("%d-%d", rp.tables[i].Loc.ExtentID, rp.tables[i].Loc.Offset)]; ok {
			//exclude the table which is to be deprecate
			rp.tables = append(rp.tables[:i], rp.tables[i+1:]...)
		}
	}
	var tableLocs []*pspb.Location
	for _, t := range rp.tables {
		tableLocs = append(tableLocs, &t.Loc)
	}
	rp.updateTableLocs(tableLocs)
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
