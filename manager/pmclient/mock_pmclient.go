package pmclient

import (
	"github.com/journeymidnight/autumn/proto/pspb"
)

type MockPMClient struct {
	Tables []*pspb.Location
}

func (c *MockPMClient) SetRowStreamTables(id uint64, tables []*pspb.Location) error {
	c.Tables = tables
	return nil
}
