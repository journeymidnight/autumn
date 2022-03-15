package range_partition

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/opentracing/opentracing-go"
)

//replay valuelog
//compact valuelog
func (rp *RangePartition) writeValueLog(reqs []*request) ([]*Entry, valuePointer, error) {

	var blocks []block
	var entries []*Entry

	for _, req := range reqs {
		for _, e := range req.entries {
			blocks = append(blocks, e.Encode())
			entries = append(entries, e)
		}
	}

	span := opentracing.GlobalTracer().StartSpan("writeValueLog")
	defer span.Finish()
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	extentID, offsets, tail, err := rp.logStream.Append(ctx, blocks, rp.opt.MustSync)
	if err != nil {
		return nil, valuePointer{}, err
	}

	//update entries
	for i := range entries {
		entries[i].ExtentID = extentID
		entries[i].Offset = offsets[i]
		if i == len(entries)-1 {
			entries[i].End = tail
		} else {
			entries[i].End = offsets[i+1]
		}
	}
	return entries, valuePointer{extentID: extentID, offset: tail}, nil

}

func replayLog(stream streamclient.StreamClient, replayFunc func(*Entry) (bool, error), opts ...streamclient.ReadOption) error {

	iter := stream.NewLogEntryIter(opts...)
	for {
		ok, err := iter.HasNext()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		data, extentID, offset, end := iter.Next()

		ei, err := DecodeEntry(data)
		ei.ExtentID = extentID
		ei.Offset = offset
		ei.End = end

		if err != nil {
			return err
		}
		next, err := replayFunc(ei)
		if err != nil {
			return err
		}
		if !next {
			break
		}
	}
	return nil
}

func discardEntry(ei *Entry, vs y.ValueStruct) bool {
	if vs.Version != y.ParseTs(ei.Key) {
		// Version not found. Discard.
		return true
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return true
	}
	return false
}

type GcTask struct {
	ForceGC bool
	ExIDs   []uint64 //if not forceGC, pickup will choose automatically
}

const (
	MAX_GC_ONCE = 3
)

func (rp *RangePartition) startGC() {
	rp.gcStopper = utils.NewStopper()
	rp.gcRunChan = make(chan GcTask, 1)
	//only one compact goroutine
	rp.gcStopper.RunWorker(func() {
		for {
			select {
			case <-rp.gcStopper.ShouldStop():
				return
			case task := <-rp.gcRunChan:
				//chose an extent to compact

				logStreamInfo := rp.logStream.StreamInfo()
				if len(logStreamInfo.ExtentIDs) < 2 {
					fmt.Printf("only one extent, no extent to delete\n")
					continue
				}
				holes := make([]uint64, 0, 3)
				if task.ForceGC {
					fmt.Printf("task to gc on %+v\n", task.ExIDs)
					//valide task.ExIDs
					idx := make(map[uint64]bool, len(logStreamInfo.ExtentIDs))
					for _, exID := range logStreamInfo.ExtentIDs {
						idx[exID] = true
					}
					for i := 0; i < len(task.ExIDs) && i < MAX_GC_ONCE; i++ {
						if _, ok := idx[task.ExIDs[i]]; ok {
							holes = append(holes, task.ExIDs[i])
						}
					}

				} else {
					tbls := rp.getTables()

					discards := getDiscards(tbls)
					//exlucde the last extent, last extent is usually a non-sealed extent
					validDiscard(discards, logStreamInfo.ExtentIDs[:len(logStreamInfo.ExtentIDs)-1])
					//sort discard
					canndidates := make([]uint64, 0, 3)

					for k := range discards {
						canndidates = append(canndidates, k)
					}
					sort.Slice(canndidates, func(i, j int) bool {
						return discards[canndidates[i]] > discards[canndidates[j]]
					})
					for i := 0; i < len(canndidates) && i < MAX_GC_ONCE; i++ {
						exID := canndidates[i]
						size, err := rp.logStream.SealedLength(exID)
						if err != nil {
							xlog.Logger.Errorf("get sealed length error: %v in runGC", err)
							continue
						}
						if size == 0 {
							continue
						}
						radio := float64(discards[exID]) / float64(size)
						if radio > 0.4 {
							holes = append(holes, exID)
						}
					}
				}

				if len(holes) == 0 {
					fmt.Printf("no extent to delete\n")
					continue
				}

				for i := range holes {
					rp.runGC(holes[i])
				}
				//TODO: retry if failed
				//delete extent for stream
				fmt.Printf("deleted extent [%v], for stream %d\n", holes, logStreamInfo.StreamID)
				//delete extent for block
				pctx, cancel := context.WithTimeout(rp.gcStopper.Ctx(), time.Second*5)
				if err := rp.logStream.PunchHoles(pctx, holes); err != nil {
					xlog.Logger.Errorf("punch holes error: %v in runGC", err)
				}
				cancel()
			}
		}
	})
}

func (rp *RangePartition) runGC(extentID uint64) {

	var count, moved int
	var freeSize uint64
	var moveSize uint64
	wb := make([]*Entry, 0, 100)

	fe := func(ei *Entry) (bool, error) {

		count++
		if count%100000 == 0 {
			xlog.Logger.Debugf("Processing entry %d", count)
		}

		freeSize += uint64(ei.Size())

		userKey := y.ParseKey(ei.Key)

		//fmt.Printf("processing %s\n", userKey)
		//if small file
		if (ei.Meta & uint32(BitValuePointer)) == 0 {
			//fmt.Printf("discard small entry, key: %s\n", streamclient.FormatEntry(ei))
			return true, nil
		}

		if !rp.IsUserKeyInRange(userKey) {
			return true, nil
		}

		//startKey <= userKey < endKey

		vs := rp.getValueStruct(userKey, 0) //get the latest version

		if discardEntry(ei, vs) {
			//fmt.Printf("discard blob entry, key: %s\n", streamclient.FormatEntry(ei))
			return true, nil
		}

		/*
			rp.Write(userKey, []byte("TEST"))
		*/

		utils.AssertTrue(len(vs.Value) > 0)
		var vp valuePointer
		vp.Decode(vs.Value)
		if vp.extentID == ei.ExtentID && vp.offset == ei.Offset {
			moved++
			//write ne to mt
			//keep seqNum

			ne := ei //use the same entry
			//fmt.Printf("MOVE %s\n", userKey)
			if len(wb) > 4 {
				//如果是GC request, 在写入log之前,还要再读一遍key, 如果有新Key已经写入, 则放弃
				req, err := rp.sendToWriteCh(wb, true)
				if err != nil {
					return false, err
				}
				req.Wait() //wait for write complete and reclaim memory
				wb = wb[:0]
			}
			wb = append(wb, ne)
			moveSize += uint64(ei.Size())
		}
		return true, nil
	}

	err := replayLog(rp.logStream, fe, streamclient.WithReadFrom(extentID, 0, 1))
	if err != nil {
		xlog.Logger.Errorf("replayLog error: %v", err)
		return
	}

	//flush wb
	if len(wb) > 0 {
		req, err := rp.sendToWriteCh(wb, true)
		if err != nil {
			xlog.Logger.Errorf("sendToWriteCh error: %v", err)
			return
		}
		req.Wait()
	}

	err = rp.logStream.PunchHoles(context.Background(), []uint64{extentID})
	if err != nil {
		xlog.Logger.Errorf("PunchHoles error: %v", err)
		return
	}

	fmt.Printf("GC: processed %d entries, %d entries moved, %v freed bytes, %v moved bytes\n", count, moved,
		utils.HumanReadableSize(freeSize), utils.HumanReadableSize(moveSize))

	xlog.Logger.Infof("GC: processed %d entries, %d entries moved, %d freed bytes, %d moved bytes", count, moved, freeSize, moveSize)

}
