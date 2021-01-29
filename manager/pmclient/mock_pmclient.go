package pmclient

import (
	"github.com/journeymidnight/autumn/proto/pspb"
)


type MockPMClient struct {
}

func (c *MockPMClient) SetTables(uint64, []*pspb.TableLocation) error {
	return nil
}

func (c *MockPMClient) GetMeta(id uint64) *pspb.PartitionMeta {
	return &pspb.PartitionMeta{
		Range: &pspb.Range{
			[]byte(""),
			[]byte(""),
		},
	}
}
