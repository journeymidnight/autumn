package rangepartition

import (
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
)

type discardManager struct {
	streams         map[uint64]pb.StreamInfo
	reverseIndex    map[uint64][]uint64 //extentID=>[stream1, stream2]
	discardOfStream map[uint64]int64
	utils.SafeMutex
}

func NewDiscardManager(si map[uint64]pb.StreamInfo) *discardManager {
	dsm := &discardManager{
		reverseIndex:    make(map[uint64][]uint64),
		discardOfStream: make(map[uint64]int64),
	}

	dsm.streams = si
	for streamID, extentIDs := range si {
		for _, extentID := range extentIDs.ExtentIDs {
			dsm.reverseIndex[extentID] = append(dsm.reverseIndex[extentID], streamID)
		}
	}
	return dsm
}

//input : map[exteintID]=>len(free data)
func (dsm *discardManager) UpdateDiscardStats(stats map[uint64]int64) {
	dsm.RLock()
	defer dsm.RUnlock()
	for extentID, discard := range stats {
		for _, streamID := range dsm.reverseIndex[extentID] {
			dsm.discardOfStream[streamID] += discard
		}
	}

}

func (dsm *discardManager) MaxDiscard() *pb.StreamInfo {
	dsm.RLock()
	defer dsm.RUnlock()
	maxDiscard := int64(0)
	maxStreamID := uint64(0)
	if len(dsm.discardOfStream) == 0 {
		return nil
	}
	for streamID, discard := range dsm.discardOfStream {
		if maxDiscard < discard {
			maxStreamID = streamID
			maxDiscard = discard
		}

	}
	ret := dsm.streams[maxStreamID]
	return &ret
}

func (dsm *discardManager) AddBlobStream(si pb.StreamInfo) {
	dsm.Lock()
	defer dsm.Unlock()

	dsm.streams[si.StreamID] = si

	for _, extentID := range si.ExtentIDs {
		dsm.reverseIndex[extentID] = append(dsm.reverseIndex[extentID], extentID)
	}
}
