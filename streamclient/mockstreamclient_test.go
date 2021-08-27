package streamclient

import (
	"context"
	"fmt"
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
	return &pb.Block{
		Data:        data,
	}
}

func TestAppendReadBlocks(t *testing.T) {
	b := newTestBlock(512)
	br := NewMockBlockReader()
	client := NewMockStreamClient("log",br)
	defer client.Close()
	exID, offsets,_, err := client.Append(context.Background(), []*pb.Block{b}, true)
	assert.Nil(t, err)

	bs,_, err := br.Read(context.Background(), exID, offsets[0], 1)
	assert.Nil(t, err)
	assert.Equal(t, b.Data, bs[0].Data)
}

func TestAppendReadEntries(t *testing.T) {
	cases := []*pb.EntryInfo{
		{
			Log: &pb.Entry{
				Key:   []byte("a"),
				Value: []byte("xx"),
			},
		},
		{
			Log: &pb.Entry{
				Key:   []byte("b"),
				Value: []byte("xx"),
			},
		},
	}
	br := NewMockBlockReader()
	client := NewMockStreamClient("log", br)
	defer client.Close()
	eID, tail, err := client.AppendEntries(context.Background(), cases, true)

	require.NoError(t, err)
	

	//GC read
	iter := client.NewLogEntryIter(WithReadFromStart())

	//小value在GC时,一个block只返回自己的大小, 上面的entry全部可以GC
	n := 0
	for {
		ok, err := iter.HasNext()
		require.NoError(t, err)
		if !ok {
			break
		}
		ei := iter.Next()
		require.Equal(t, cases[n].Log.Key, ei.Log.Key)
		require.Equal(t, []byte(nil), ei.Log.Value)
		n ++
	}
	require.Equal(t,2, n)

	//iter = client.NewLogEntryIter(ReadOption{}.WithReadFromStart().WithReplay())
	iter = client.NewLogEntryIter(WithReadFromStart(), WithReplay())

	expectedKeys := [][]byte{
		[]byte("a"),
		[]byte("b"),
	}

	var ans [][]byte
	for {
		ok, err := iter.HasNext()
		require.NoError(t, err)
		if !ok {
			break
		}
		ei := iter.Next()
		ans = append(ans, ei.Log.Key)
	}
	require.Equal(t, expectedKeys, ans)

	ans = ans[:0]
	_, _, err = client.AppendEntries(context.Background(), cases, true)
	require.NoError(t, err)

	iter = client.NewLogEntryIter(WithReadFrom(eID, tail), WithReplay())
	for {
		ok, err := iter.HasNext()
		require.NoError(t, err)
		if !ok {
			break
		}
		ei := iter.Next()
		ans = append(ans, ei.Log.Key)
	}
	require.Equal(t, expectedKeys, ans)
}

func TestAppendReadBigBlocks(t *testing.T) {
	cases := []*pb.EntryInfo{
		{
			Log: &pb.Entry{
				Key:   []byte("a"),
				Value: []byte("xx"),
			},
		},
		{
			Log: &pb.Entry{
				Key:   []byte("b"),
				Value: []byte(fmt.Sprintf("%01048576d", 10)),
			},
		},
	}
	client := NewMockStreamClient("log", NewMockBlockReader())
	defer client.Close()
	_, _, err := client.AppendEntries(context.Background(), cases, true)

	require.NoError(t, err)

	iter := client.NewLogEntryIter(WithReadFromStart(), WithReplay())
	var ans []int //value大小
	for i :=0 ; i < len(cases) ; i ++ {
		ok, err := iter.HasNext()
		require.NoError(t, err)
		if !ok {
			break
		}
		ei := iter.Next()
		//key都存在
		require.Equal(t, cases[i].Log.Key, ei.Log.Key)

		ans = append(ans, len(ei.Log.Value))
	}

	//小key的value长度== 2
	//big value return nil value
	require.Equal(t, []int{2,0}, ans) 
}

/*
func TestSplitExtent(t *testing.T) {
	cases := []*pb.EntryInfo{
		{
			Log: &pb.Entry{
				Key:   []byte("a"),
				Value: []byte("xx"),
			},
		},
		{
			Log: &pb.Entry{
				Key:   []byte("b"),
				Value: []byte(fmt.Sprintf("%01048576d", 10)), //1MB
			},
		},
		{
			Log: &pb.Entry{
				Key:   []byte("c"),
				Value: []byte("xx"),
			},
		},
	}

	br := NewMockBlockReader()
	client := NewMockStreamClient("log", br).(*MockStreamClient)
	defer client.Close()

	_, _, err := client.AppendEntries(context.Background(), cases)
	_, _, err = client.AppendEntries(context.Background(), cases)
	require.NoError(t, err)

	l := len(client.stream)

	p := client.stream[1]
	frontStream, _, err := client.Truncate(context.Background(), p)
	require.NoError(t, err)

	truncStream := OpenMockStreamClient(frontStream, br)
	defer truncStream.Close()

	//fmt.Printf("len[%d] split to %d vs %d\n", l, len(newStream.ExtentIDs), len(client.exs))
	require.Equal(t, l, len(frontStream.ExtentIDs)+len(client.stream))

	iter := client.NewLogEntryIter(WithReadFromStart(), WithReplay())
	for {
		ok, err := iter.HasNext()
		require.NoError(t, err)
		if !ok {
			break
		}
		iter.Next()
		//ei := iter.Next()
		//fmt.Printf("%s\n", ei.Log.Key)
	}

}
*/