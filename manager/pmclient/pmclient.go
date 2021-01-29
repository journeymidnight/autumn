package pmclient

import "github.com/journeymidnight/autumn/proto/pspb"

type PMClient interface {
	GetMeta(uint64) *pspb.PartitionMeta
	SetTables(uint64, []*pspb.TableLocation) error
}
