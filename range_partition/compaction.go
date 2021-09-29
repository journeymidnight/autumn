package range_partition

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

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
	n            int //at most n tables to be merged
	opt          *Option
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
		for i := 0; i < len(tbls) && i < p.n; i++ {
			if extents[i] == chosenExtentID {
				chosenTbls = append(chosenTbls, tbls[i])
			} else if extents[i] != chosenExtentID {
				truncateID = tbls[i].FirstOccurrence() //truncateID the second extentID
				break
			}
		}

		//sort both chosenTbls and tbls by LastSeq
		sort.Slice(tbls, func(i, j int) bool {
			return tbls[i].LastSeq < tbls[j].LastSeq
		})
		sort.Slice(chosenTbls, func(i, j int) bool {
			return chosenTbls[i].LastSeq < chosenTbls[j].LastSeq
		})

		i := sort.Search(len(tbls), func(i int) bool {
			return tbls[i].LastSeq == chosenTbls[0].LastSeq
		})
		j := 0
		utils.AssertTruef(i >= 0, "search should always succeed")

		for i < len(tbls) && j < len(chosenTbls) && len(compactTbls) < p.n {
			if tbls[i].LastSeq < chosenTbls[j].LastSeq {
				compactTbls = append(compactTbls, tbls[i]) //fix holes in chosenTbls
				i++
			} else if tbls[i].LastSeq == chosenTbls[j].LastSeq {
				compactTbls = append(compactTbls, tbls[i])
				i++
				j++
			} else { //tbls[i].LastSeq > chosenTbls[j].LastSeq
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
	sort.Slice(tbls, func(i, j int) bool {
		return tbls[i].LastSeq < tbls[j].LastSeq
	})

	throttle := uint64(math.Round(p.compactRatio * float64(p.opt.MaxSkipList)))

	for i := 0; i < len(tbls); i++ {
		for i < len(tbls) && tbls[i].EstimatedSize < throttle && len(compactTbls) < p.n {
			//merge to older and larger SSTables
			if i > 0 && len(compactTbls) == 0 {
				compactTbls = append(compactTbls, tbls[i-1])
			}
			compactTbls = append(compactTbls, tbls[i])
			i++
		}
		if len(compactTbls) > 0 {
			//corner case : 1, 100, 100, 100
			if len(compactTbls) == 1 {
				compactTbls = append(compactTbls, tbls[i])
			}
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
	rp.majorCompactChan = make(chan struct{}, 1)
	rp.compactStopper.RunWorker(rp.compact)
}

func (rp *RangePartition) compact() {
	randTicker := utils.NewRandomTicker(10*time.Second, 20*time.Second)
	for {
		select {
		case <-rp.majorCompactChan:
			allTables := rp.getTables()
			if len(allTables) == 0 {
				continue
			}
			if len(allTables) < 2 && atomic.LoadUint32(&rp.hasOverlap) == 0 {
				continue
			}
			eID := allTables[len(allTables)-1].Loc.ExtentID
			fmt.Printf("do major compaction tasks for tables %+v\n", allTables)
			rp.doCompact(allTables, true)
			if eID != 0 {
				//last table's meta extentd
				err := rp.rowStream.Truncate(context.Background(), eID)
				if err != nil {
					xlog.Logger.Warnf("LOG Truncate extent %d error %v", eID, err)
				}
			}
			fmt.Printf("fininshed major compaction tasks for tables %+v\n", allTables)
		case <-randTicker.C:
			allTables := rp.getTables()
			compactTables, eID := rp.pickupTablePolicy.PickupTables(allTables)
			if len(compactTables) < 2 {
				continue
			}
			fmt.Printf("do minor compaction tasks for tables %+v\n", compactTables)
			rp.doCompact(compactTables, false)
			if eID != 0 {
				//last table's meta extentd
				err := rp.rowStream.Truncate(context.Background(), eID)
				if err != nil {
					xlog.Logger.Warnf("LOG Truncate extent %d error %v", eID, err)
				}
			}
			fmt.Printf("finished minor compaction tasks for tables %+v\n", compactTables)

		case <-rp.compactStopper.ShouldStop():
			randTicker.Stop()
			return
		}
	}
}

func (rp *RangePartition) doCompact(tbls []*table.Table, major bool) {

	if len(tbls) < 1 {
		return
	}

	//tbls的顺序是在stream里面的顺序, 改为按照seqNum排序
	//如果key完全一样, 在iter前面的优先级高
	sort.Slice(tbls, func(i, j int) bool {
		return tbls[i].LastSeq > tbls[j].LastSeq
	})

	discards := getDiscards(tbls)

	var iters []y.Iterator
	var maxSeq uint64
	var head valuePointer
	head = valuePointer{
		extentID: tbls[0].VpExtentID,
		offset:   tbls[0].VpOffset,
	}

	for _, table := range tbls {
		iters = append(iters, table.NewIterator(false))
	}

	updateStats := func(vs y.ValueStruct) {
		if (vs.Meta & y.BitValuePointer) > 0 { //big Value
			var vp valuePointer
			vp.Decode(vs.Value)
			discards[vp.extentID] += int64(vp.len)
		}
	}

	it := table.NewMergeIterator(iters, false)
	defer it.Close()

	it.Rewind()

	var numBuilds int
	resultCh := make(chan struct{})

	capacity := int64(2 * rp.opt.MaxSkipList)
	for it.Valid() {
		var skipKey []byte
		timeStart := time.Now()
		var numKeys, numSkips uint64
		memStore := skiplist.NewSkiplist(capacity)
		for ; it.Valid(); it.Next() {

			userKey := y.ParseKey(it.Key())

			//fmt.Printf("processing %s~%d\n", y.ParseKey(it.Key()), y.ParseTs(it.Key()))
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
				updateStats(it.Value()) //it is expired && bolb value, add discard
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

		if memStore.Empty() {
			return
		}

		task := flushTask{
			mt:        memStore,
			vptr:      head,
			seqNum:    maxSeq,
			isCompact: true,
			resultCh:  resultCh,
		}
		if !it.Valid() {
			//if this the last table, attach removedTables and discards

			validDiscard(discards, rp.logStream.StreamInfo().ExtentIDs)
			task.removedTable = tbls
			task.discards = discards
		}
		//fmt.Printf("send task %+v", task.discards)
		rp.flushChan <- task
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

func validDiscard(discards map[uint64]int64, extentIDs []uint64) map[uint64]int64 {
	extentIdx := make(map[uint64]bool)
	for _, extentID := range extentIDs {
		extentIdx[extentID] = true
	}
	for extentID := range discards {
		if _, ok := extentIdx[extentID]; !ok {
			delete(discards, extentID)
		}
	}
	return discards
}

func getDiscards(tbls []*table.Table) map[uint64]int64 {
	discards := make(map[uint64]int64)

	for _, tbl := range tbls {
		for k, v := range tbl.Discards {
			discards[k] += v
		}
	}
	return discards
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
