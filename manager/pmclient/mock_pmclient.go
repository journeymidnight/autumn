package pmclient

import (
	"github.com/journeymidnight/autumn/proto/pspb"
)

type MockPMClient struct {
	Tables []*pspb.TableLocation
}

func (c *MockPMClient) SetTables(id uint64, tables []*pspb.TableLocation) error {
	c.Tables = tables
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
