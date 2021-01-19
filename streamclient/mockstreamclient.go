package streamclient

import (
	"context"
	"os"

	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
)

type MockStreamClient struct {
	ex       *extent.Extent
	fileName string
}

func NewMockStreamClient(fileName string, id uint64) StreamClient {
	ex, err := extent.CreateExtent(fileName, id)
	utils.Check(err)
	return &MockStreamClient{
		ex:       ex,
		fileName: fileName,
	}
}

//single thread
func (client *MockStreamClient) Append(ctx context.Context, blocks []*pb.Block, userData interface{}) (*Op, error) {

	cmp := client.ex.CommitLength()
	client.ex.Lock()
	defer client.ex.Unlock()

	offsets, err := client.ex.AppendBlocks(blocks, nil)
	utils.Check(err)

	for i := range blocks {
		cmp += blocks[i].BlockLength + 512
	}
	utils.AssertTrue(cmp == client.ex.CommitLength())

	op := opPool.Get().(*Op)
	op.Reset(blocks, userData)

	op.wg.Done()
	op.Wait()

	op.Offsets = offsets
	return op, nil
}

func (client *MockStreamClient) Close() {
	client.ex.Close()
	os.Remove(client.fileName)
}

func (client *MockStreamClient) Connect() error {
	return nil
}

func (client *MockStreamClient) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, error) {
	return client.ex.ReadBlocks(offset, numOfBlocks, (32 << 20))
}
