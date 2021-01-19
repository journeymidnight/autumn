package streamclient

import (
	"context"
	"testing"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/stretchr/testify/assert"
)

func newTestBlock(size uint32) *pb.Block {
	data := make([]byte, size)
	utils.SetRandStringBytes(data)
	return &pb.Block{
		CheckSum:    utils.AdlerCheckSum(data),
		BlockLength: size,
		Data:        data,
	}
}

func TestAppendReadFile(t *testing.T) {
	b := newTestBlock(512)
	client := NewMockStreamClient("test.tmp", 100)
	defer client.Close()
	op, err := client.Append(context.Background(), []*pb.Block{b}, nil)
	assert.Nil(t, err)
	defer op.Free()
	op.Wait()

	bs, err := client.Read(context.Background(), 100, op.Offsets[0], 1)
	assert.Nil(t, err)
	assert.Equal(t, b.Data, bs[0].Data)
}
