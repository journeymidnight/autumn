package range_partition

import (
	"context"
	"fmt"
	"sort"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

//replay valuelog
//compact valuelog
func (rp *RangePartition) writeValueLog(reqs []*request) ([]*pb.EntryInfo, valuePointer, error) {

	var entries []*pb.EntryInfo

	for _, req := range reqs {
		entries = append(entries, req.entries...)
	}

	extentID, tail, err := rp.logStream.AppendEntries(context.Background(), entries, rp.opt.MustSync)
	if err != nil {
		return nil, valuePointer{}, err
	}
	return entries, valuePointer{extentID: extentID, offset: tail}, nil

}

func replayLog(stream streamclient.StreamClient, replayFunc func(*pb.EntryInfo) (bool, error), opts ...streamclient.ReadOption) error {

	iter := stream.NewLogEntryIter(opts...)
		
	for {
		ok, err := iter.HasNext()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		ei := iter.Next()
		next, err := replayFunc(ei)
		if err != nil {
			return err
		}
		if next == false {
			break
		}
	}
	return nil
}

//policy
//FIXME	
/*
func (rp *RangePartition) pickLog(discardRatio float64) *pb.StreamInfo {

}
*/

func discardEntry(ei *pb.EntryInfo, vs y.ValueStruct) bool {
	if vs.Version != y.ParseTs(ei.Log.Key) {
		// Version not found. Discard.
		return true
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return true
	}
	return false
}


func (rp *RangePartition) startGC() {
	rp.gcStopper = utils.NewStopper()
	rp.gcRunChan = make(chan struct{}, 1)
	//only one compact goroutine
	rp.gcStopper.RunWorker(func() {
		for {
			select {
				case <- rp.gcStopper.ShouldStop():
					return
				case <- rp.gcRunChan:
					//chose an extent to compact
				
					logStreamInfo := rp.logStream.StreamInfo()
					if len(logStreamInfo.ExtentIDs) < 2 {
						continue
					}
					tbls := rp.getTables()

					discards := getDiscards(tbls)
					validDiscard(discards, logStreamInfo.ExtentIDs[:len(logStreamInfo.ExtentIDs)-1])
					//sort discards by its value
					canndidates := make([]uint64, 0, len(discards))
					for k, _ := range discards {
						canndidates = append(canndidates, k)
					}
					sort.Slice(canndidates, func(i, j int) bool {
						return discards[canndidates[i]] > discards[canndidates[j]]
					})

					holes := make([]uint64, 0, 3)
					for i := 0 ; i < len(canndidates) && i < 3; i++ {
						exID := canndidates[i]
						size, err := rp.blockReader.SealedLength(exID)
						if err != nil {
							xlog.Logger.Errorf("get sealed length error: %v in runGC", err)
							continue
						}
						if size == 0 {
							continue
						}
						radio := float64(discards[exID]) / float64(size)
						if radio > 0.4 {
							rp.runGC(exID)
							holes = append(holes, exID)
						}
					}
					//TODO: retry if failed
					if err := rp.logStream.PunchHoles(context.Background(), holes); err != nil {
						xlog.Logger.Errorf("punch holes error: %v in runGC", err)
					}
			}
		}
	})
}



func (rp *RangePartition) runGC(extentID uint64) {

	var count, moved int
	var freeSize uint64
	var moveSize uint64
	wb := make([]*pb.EntryInfo, 0, 100)

	fe := func(ei *pb.EntryInfo) (bool, error) {

		count++
		if count%100000 == 0 {
			xlog.Logger.Debugf("Processing entry %d", count)
		}

		freeSize += ei.EstimatedSize

		userKey := y.ParseKey(ei.Log.Key)

		//fmt.Printf("processing %s\n", userKey)
		//if small file, ei.Log.Value must be nil
		//if big file, len(ei.Log.Value) > 0
		if (ei.Log.Meta & uint32(y.BitValuePointer)) == 0{
			//fmt.Printf("discard small entry, key: %s\n", streamclient.FormatEntry(ei))
			utils.AssertTrue(ei.Log.Value == nil)
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
			ne := &pb.EntryInfo{
				Log: &pb.Entry{
					Key:   ei.Log.Key,
					Value: ei.Log.Value,
				},
			}

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
			moveSize += ei.EstimatedSize
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
