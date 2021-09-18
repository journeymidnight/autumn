package range_partition

import (
	"context"
	"fmt"

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

	if (rp.logStream.NumOfExtents() > 2 && rp.logStream.CommitEnd() > rp.opt.TruncateSize) || rp.logStream.NumOfExtents() > 5 {
		blobStream, err := rp.logStream.Truncate(context.Background(), extentID, rp.blobKey)
		if err == nil{
			rp.discard.MergeBlobStream(blobStream)
			fmt.Printf("truncated log on extent %d for space %s, new stream is %d\n", extentID, rp.blobKey, blobStream.StreamID)
		} else {
			xlog.Logger.Errorf("Truncate error %v", err)
		}
	}

	//FIXME
	//if rp.logStream.Size() > 4GB then rp.logStream.Truncate()
	return entries, valuePointer{extentID: extentID, offset: tail}, nil
}

func replayLog(stream streamclient.StreamClient, startExtentID uint64, startOffset uint32, replay bool, replayFunc func(*pb.EntryInfo) (bool, error)) error {
	var opts []streamclient.ReadOption

	if replay {
		opts = append(opts, streamclient.WithReplay())
	}
	if startOffset == 0 && startExtentID == 0 {
		opts = append(opts, streamclient.WithReadFromStart())
	} else {
		opts = append(opts, streamclient.WithReadFrom(startExtentID, startOffset))
	}



	iter := stream.NewLogEntryIter(opts...)
	
	

	fmt.Println("CheckCommitLength Done")
	
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
func (rp *RangePartition) pickLog(discardRatio float64) *pb.StreamInfo {
	stream, holeSize := rp.discard.MaxDiscard()
	fmt.Printf("picked stream %+v, holeSize is %+v", stream, holeSize)
	return stream
}


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
					rp.runGC(0.5)
			}
		}
	})
}



func (rp *RangePartition) runGC(discardRatio float64) {
	streamInfo := rp.pickLog(discardRatio)
	if streamInfo == nil {
		return
	}

	var candidate streamclient.StreamClient

	candidate = rp.openStream(*streamInfo)

	candidate.Connect()
	var count, moved int
	var freed uint64
	var size uint64
	wb := make([]*pb.EntryInfo, 0, 100)

	fe := func(ei *pb.EntryInfo) (bool, error) {
		count++
		if count%100000 == 0 {
			xlog.Logger.Debugf("Processing entry %d", count)
		}

		freed += ei.EstimatedSize

		//if small file, ei.Log.Value must be nil
		//if big file, len(ei.Log.Value) > 0
		if ei.Log.Value == nil {
			return true, nil
		}

		userKey := y.ParseKey(ei.Log.Key)

		if !rp.IsUserKeyInRange(userKey) {
			return true, nil
		}

		//startKey <= userKey < endKey

		vs := rp.getValueStruct(userKey, 0) //get the lasted version

		if discardEntry(ei, vs) {
			return true, nil
		}

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

			//?batch?
			if len(wb) > 4 || ei.EstimatedSize+size > 16*MB {
				req, err := rp.sendToWriteCh(wb, true)
				if err != nil {
					return false, err
				}
				go func() {
					//Wait() will release req
					req.Wait()
				}()
			}
			wb = append(wb, ne)
			size += ei.EstimatedSize

		}
		return true, nil
	}

	replayLog(candidate, 0, 0, false, fe)

}
