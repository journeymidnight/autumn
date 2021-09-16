package range_partition

import (
	"context"
	"fmt"
	"math"
	"sort"
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
//RETURN: if tables is nil, do not compact
//
type PickupTables interface {
	PickupTables(tbls []*table.Table) (tables []*table.Table, eID uint64)
}

type DefaultPickupPolicy struct {
	compactRatio float64
	headRatio    float64
	n int //at most n tables to be merged
	opt *Option
}

//In size-tiered compaction, newer and smaller SSTables are successively merged into older and larger SSTables
func (p DefaultPickupPolicy) PickupTables(tbls []*table.Table) ([]*table.Table, uint64) {

	utils.AssertTruef(p.n > 1, "PickupPolicy: n must be greater than 1")

	if len(tbls) < 2 {
		return nil, 0
	}
	
	/*
	rule1:
	if tables are on multiple extents, and the table size on the first extent is less then headRatio * all table size
	pickup all tables on the first extent, and compact them and truncate.
	for invariance: all tables must are sorted, we can only compact neighbor tables
	*/

	totalSize := uint64(0)
	extents := make([]uint64, 0, len(tbls))
	accumulatedSize := make(map[uint64]uint64)
	compactTbls := make([]*table.Table, 0, p.n)
	for _, t := range tbls {
		totalSize += t.EstimatedSize
		extents = append(extents, t.FirstOccurrence())
		accumulatedSize[t.Loc.ExtentID] += t.EstimatedSize
	}

	if len(tbls) > 1 && accumulatedSize[extents[0]] < uint64(math.Round(p.headRatio*float64(totalSize))) {
		chosenTbls := make([]*table.Table, 0, p.n)
		chosenExtentID := extents[0]
		var truncateID uint64
		//find all tables on chosenExtentID
		for i := 0; i < len(tbls) && i < p.n ; i++ {
			if extents[i] == chosenExtentID {
				chosenTbls = append(chosenTbls, tbls[i])
			} else if extents[i] != chosenExtentID {
				truncateID = tbls[i].FirstOccurrence() //truncateID the second extentID
				break
			}
		}

		//sort both chosenTbls and tbls by LastSeq
		sort.Slice(tbls, func(i,j int)bool {
			return tbls[i].LastSeq < tbls[j].LastSeq
		})
		sort.Slice(chosenTbls, func(i, j int) bool {
			return chosenTbls[i].LastSeq < chosenTbls[j].LastSeq
		})

		i := sort.Search(len(tbls), func(i int) bool {
			return tbls[i].LastSeq == chosenTbls[0].LastSeq
		})
		j := 0 
		utils.AssertTruef(i >=0 , "search should always succeed")

		for i < len(tbls) && j < len(chosenTbls) && len(compactTbls) < p.n{
			if tbls[i].LastSeq < chosenTbls[j].LastSeq {
				compactTbls = append(compactTbls, tbls[i])//fix holes in chosenTbls
				i ++
			} else if tbls[i].LastSeq == chosenTbls[j].LastSeq {
				compactTbls = append(compactTbls, tbls[i])
				i ++
				j ++
			} else {//tbls[i].LastSeq > chosenTbls[j].LastSeq
				utils.AssertTruef(false, "chosenTbls is subset of tbls, should never happen")
			}
		}

		//do we have all tables on extent[0]?
		if j == len(chosenTbls) {
			return compactTbls, truncateID
		}
		return compactTbls, 0

	}

	/*
	rule2:
	choose a table whose size is less than compactRatio * totalSize, start from this table's previous table if any,
	to the next table whose size is less than compactRatio * totalSize
	merge newer and smaller SSTables into older and larger SSTables
	*/

	//sort tables by lastSeq.
	sort.Slice(tbls, func(i,j int)bool {
		return tbls[i].LastSeq < tbls[j].LastSeq
	})
	
	for i := 0; i < len(tbls) && i < p.n; i++ {
		if tbls[i].EstimatedSize < uint64(math.Round(p.compactRatio*float64(p.opt.MaxSkipList))) {
			if len(compactTbls) == 0 && i > 0 {
				compactTbls = append(compactTbls, tbls[i-1])
			} else {
				compactTbls = append(compactTbls, tbls[i])
			}
		} else if len(compactTbls) > 0 {
			compactTbls = append(compactTbls, tbls[i])
			break
		}
	}
	if len(compactTbls) > 1 {
		return compactTbls, 0
	}
	return nil, 0
}



func (rp *RangePartition) startCompact() {
	rp.compactStopper = utils.NewStopper()
	//only one compact goroutine
	rp.compactStopper.RunWorker(rp.compact)
}


func (rp *RangePartition) compact() {
	randTicker := utils.NewRandomTicker(10*time.Second, 20*time.Second)
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
			if len(compactTables) == 0 {
				continue
			}
			fmt.Printf("do compaction tasks for tables %+v\n", compactTables)
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
	//tbls的顺序是在stream里面的顺序, 改为按照seqNum排序
	//如果key完全一样, 在iter前面的优先级高
	sort.Slice(tbls, func(i, j int) bool {
		return tbls[i].LastSeq > tbls[j].LastSeq
	})
	
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
