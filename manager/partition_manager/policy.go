package partition_manager

import (
	"errors"

	"github.com/journeymidnight/autumn/proto/pspb"
)


type SimplePolicy struct{}

//FIXME: should clone
func (SimplePolicy) AllocPart(nodes map[uint64]*pspb.PSDetail) (PSID uint64 ,err error) {
	for i := range nodes {
		return nodes[i].PSID, nil
	}
	return 0, errors.New("no nodes to allocate")
}
