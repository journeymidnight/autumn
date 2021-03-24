package rangepartition

import (
	"context"
	"time"

	"github.com/journeymidnight/autumn/rangepartition/skiplist"
	"github.com/journeymidnight/autumn/rangepartition/table"
	"github.com/journeymidnight/autumn/rangepartition/y"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

func (rp *RangePartition) startCompact() {
	rp.compactStopper = utils.NewStopper()
	rp.compactStopper.RunWorker(rp.compact)
}

func (rp *RangePartition) compact() {
	randTicker := utils.NewRandomTicker(10*time.Minute, 20*time.Minute)
	for {
		select {
		// Can add a done channel or other stuff.
		case <-randTicker.C:
			//pick tables
			//doCompact
		case <-rp.compactStopper.ShouldStop():
			return
		}
	}
}

//tbls已经inc
func (rp *RangePartition) doCompact(tbls []*table.Table, major bool) {
	if len(tbls) == 0 {
		return
	}
	defer func() {
		for _, table := range tbls {
			table.DecrRef()
		}
	}()
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
	capacity := int64(2 * maxSkipList)
	for it.Valid() {
		var skipKey []byte
		timeStart := time.Now()
		var numKeys, numSkips uint64
		memStore := skiplist.NewSkiplist(capacity)
		for ; it.Valid(); it.Next() {
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

	eID := tbls[len(tbls)-1].Loc.ExtentID

	//last table's meta extentd
	_, _, err := rp.rowStream.Truncate(context.Background(), eID)
	if err == nil {
		_ = err
		//FIXME: send frontStream/endStream to sm
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
