/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package range_partition

import (
	"fmt"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
)


type STREAMno = uint64
type EXTENTno = uint64

type discardManager struct {
	streams         map[STREAMno]*pb.StreamInfo
	reverseIndex    map[EXTENTno]uint64 //extentID=>stream1
	discardOfStream map[STREAMno]int64
	dirty bool
	stopper *utils.Stopper //goroutine to update discard stats
	utils.SafeMutex
}

func NewDiscardManager(si map[uint64]*pb.StreamInfo) *discardManager {
	dm := &discardManager{
		reverseIndex:    make(map[uint64]uint64),
		discardOfStream: make(map[uint64]int64),
		dirty: false,
	}

	dm.streams = si
	for streamID, extentIDs := range si {
		for _, extentID := range extentIDs.ExtentIDs {
			dm.reverseIndex[extentID] = streamID
		}
	}
	fmt.Printf("NewDiscardManager streams is %d\n", len(dm.streams))

	//update discard stats from time to time
	go func(){
		for {

			dm.UpdateDiscardStats(dm.discardOfStream)
		}
	}()
	return dm
}

func (dsm *discardManager) Close() {

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

	if len(dsm.streams) == 0 {
		return nil, 0
	}
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

	if len(dsm.streams) == 0 {
		return
	}
	dsm.streams[newBlobStream.StreamID] = newBlobStream

	for _, extentID := range newBlobStream.ExtentIDs {
		dsm.reverseIndex[extentID] = newBlobStream.StreamID
	}
}
