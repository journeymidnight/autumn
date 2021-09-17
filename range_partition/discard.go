package range_partition

import (
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
)


type STREAMno = uint64
type EXTENTno = uint64

type discardManager struct {
	streams         map[STREAMno]*pb.StreamInfo
	reverseIndex    map[EXTENTno]uint64 //extentID=>stream1
	discardOfStream map[STREAMno]int64
	utils.SafeMutex
}

func NewDiscardManager(si map[uint64]*pb.StreamInfo) *discardManager {
	dsm := &discardManager{
		reverseIndex:    make(map[uint64]uint64),
		discardOfStream: make(map[uint64]int64),
	}

	dsm.streams = si
	for streamID, extentIDs := range si {
		for _, extentID := range extentIDs.ExtentIDs {
			dsm.reverseIndex[extentID] = streamID
		}
	}
	return dsm
}

//input : map[extentID]=>len(free data)
func (dsm *discardManager) UpdateDiscardStats(stats map[uint64]int64) {
	dsm.RLock()
	defer dsm.RUnlock()
	for extentID, discard := range stats {
		if streamID, ok := dsm.reverseIndex[extentID]; ok {
			dsm.discardOfStream[streamID] += discard
		}
	}
}

func (dsm *discardManager) MaxDiscard() (*pb.StreamInfo, int64) {
	dsm.RLock()
	defer dsm.RUnlock()
	maxDiscard := int64(0)
	maxStreamID := uint64(0)
	if len(dsm.discardOfStream) == 0 {
		return nil, maxDiscard
	}
	for streamID, discard := range dsm.discardOfStream {
		if maxDiscard < discard {
			maxStreamID = streamID
			maxDiscard = discard
		}

	}
	ret := dsm.streams[maxStreamID]
	return ret, maxDiscard
}

//update internal state from ne
func (dsm *discardManager) MergeBlobStream(newBlobStream *pb.StreamInfo) {
	if newBlobStream == nil {
		return
	}
	dsm.Lock()
	defer dsm.Unlock()


	dsm.streams[newBlobStream.StreamID] = newBlobStream

	for _, extentID := range newBlobStream.ExtentIDs {
		dsm.reverseIndex[extentID] = newBlobStream.StreamID
	}
}
