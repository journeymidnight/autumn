package main

import (
	"bytes"
	"sort"

	"github.com/journeymidnight/autumn/manager/pmclient"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
)

type SortedRegions struct {
	regions []*pspb.RegionInfo
	ref     int32
}

type AutumnLib struct {
	pm *pmclient.AutumnPMClient
	utils.SafeMutex
}

func (lib *AutumnLib) update() {
	regions := lib.pm.GetRegions()
	//sort by startKey
	sort.Slice(regions, func(i, j int) bool {
		if regions[i].Rg == nil || regions[j].Rg == nil {
			return true
		}
		return bytes.Compare(regions[i].Rg.StartKey, regions[j].Rg.EndKey) < 0
	})

	lib.sortedRegions = regions
}

func (lib *AutumnLib) write(key, value []byte) error {
}

func (lib *AutumnLib) read() {

}
