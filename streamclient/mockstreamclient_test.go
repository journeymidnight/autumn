package streamclient

import (
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBlock(size uint32) *pb.Block {
	data := make([]byte, size)
	utils.SetRandStringBytes(data)
	rand.Seed(time.Now().UnixNano())
	lazy := rand.Int63n(2)
	return &pb.Block{
		CheckSum:    utils.AdlerCheckSum(data),
		BlockLength: size,
		Data:        data,
		Lazy:        uint32(lazy),
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

func TestSeqReadAll(t *testing.T) {
	blocks := make([]*pb.Block, 40)
	for i := range blocks {
		blocks[i] = newTestBlock(512)
	}
	client := NewMockStreamClient("test.tmp", 100)
	defer client.Close()
	op, err := client.Append(context.Background(), blocks, nil)
	defer op.Free()
	op.Wait()
	require.NoError(t, err)

	//reader from start

	reader := client.NewSeqReader(ReaderOption{}.WithReadFromStart().WithReadAll(true))
	var ret []*pb.Block
	i := 0
	for {
		ret, err = reader.Read(context.Background())
		if err != nil && err != io.EOF {
			require.NoError(t, err)
		}
		if len(ret) == 0 {
			break
		}

		require.Equal(t, blocks[i:i+len(ret)], ret)
		i += len(ret)
	}

	//read from some point
	reader = client.NewSeqReader(ReaderOption{}.WithReadFrom(100, op.Offsets[20]).WithReadAll(true))
	i = 20
	for {
		ret, err := reader.Read(context.Background())
		if err != nil && err != io.EOF {
			require.NoError(t, err)
		}
		l := len(ret)

		require.Equal(t, blocks[i:i+l], ret)
		i += len(ret)
		if err == io.EOF {
			break
		}
	}
}

func TestSeqLazyRead(t *testing.T) {
	blocks := make([]*pb.Block, 40)
	for i := range blocks {
		blocks[i] = newTestBlock(512)
	}
	client := NewMockStreamClient("test.tmp", 100)
	defer client.Close()
	op, err := client.Append(context.Background(), blocks, nil)
	defer op.Free()
	op.Wait()
	require.NoError(t, err)

	//clean data for future compare [lazy read]
	for _, b := range blocks {
		if b.Lazy > 0 {
			b.Data = nil
		}
	}

	//reader from start
	reader := client.NewSeqReader(ReaderOption{}.WithReadFromStart().WithReadAll(false))
	var ret []*pb.Block
	i := 0
	for {
		ret, err = reader.Read(context.Background())
		if err != nil && err != io.EOF {
			require.NoError(t, err)
		}
		if len(ret) == 0 {
			break
		}

		require.Equal(t, blocks[i:i+len(ret)], ret)
		i += len(ret)
	}
}
