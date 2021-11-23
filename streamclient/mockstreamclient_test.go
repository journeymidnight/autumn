package streamclient

import (
	"context"
	"fmt"
	"math"
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
		Data: data,
	}
}

func TestAppendReadBlocks(t *testing.T) {
	b := newTestBlock(512)
	client := NewMockStreamClient("log")
	defer client.Close()
	exID, offsets, _, err := client.Append(context.Background(), []*pb.Block{b}, true)
	assert.Nil(t, err)

	bs, _, err := client.Read(context.Background(), exID, offsets[0], 1, HintReadThrough)
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
	client := NewMockStreamClient("log")
	defer client.Close()
	eID, tail, err := client.AppendEntries(context.Background(), cases, true)

	require.NoError(t, err)

	//GC read
	iter := client.NewLogEntryIter(WithReadFromStart(1))

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
		require.Equal(t, 2, len(ei.Log.Value))
		n++
	}
	require.Equal(t, 2, n)

	//iter = client.NewLogEntryIter(ReadOption{}.WithReadFromStart().WithReplay())
	iter = client.NewLogEntryIter(WithReadFromStart(1))

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

	iter = client.NewLogEntryIter(WithReadFrom(eID, tail, 1))
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
	client := NewMockStreamClient("log")
	defer client.Close()
	_, _, err := client.AppendEntries(context.Background(), cases, true)

	require.NoError(t, err)

	iter := client.NewLogEntryIter(WithReadFromStart(1))
	var ans []int //value大小
	for i := 0; i < len(cases); i++ {
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

	require.Equal(t, []int{int(len(cases[0].Log.Value)), int(len(cases[1].Log.Value))}, []int{ans[0], ans[1]})
}

func TestTruncate(t *testing.T) {
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

	client := NewMockStreamClient("log").(*MockStreamClient)
	defer client.Close()

	_, _, err := client.AppendEntries(context.Background(), cases, false)
	require.NoError(t, err)

	_, _, err = client.AppendEntries(context.Background(), cases, false)
	require.NoError(t, err)

	p := client.stream[1]
	err = client.Truncate(context.Background(), p)
	require.NoError(t, err)

	iter := client.NewLogEntryIter(WithReadFromStart(math.MaxUint32))
	result := make([]string, 0)
	for {
		ok, err := iter.HasNext()
		require.NoError(t, err)
		if !ok {
			break
		}
		ei := iter.Next()
		result = append(result, string(ei.Log.Key))
	}
	require.Equal(t, []string{"a", "b", "c"}, result)

}

func TestPunchHoles(t *testing.T) {
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

	client := NewMockStreamClient("log").(*MockStreamClient)
	defer client.Close()

	_, _, err := client.AppendEntries(context.Background(), cases, false)
	require.NoError(t, err)

	_, _, err = client.AppendEntries(context.Background(), cases, false)
	require.NoError(t, err)

	err = client.PunchHoles(context.Background(), []uint64{client.stream[0], client.stream[1]})
	require.NoError(t, err)

	iter := client.NewLogEntryIter(WithReadFromStart(math.MaxUint32))
	result := make([]string, 0)
	for {
		ok, err := iter.HasNext()
		require.NoError(t, err)
		if !ok {
			break
		}
		ei := iter.Next()
		result = append(result, string(ei.Log.Key))
	}
	require.Equal(t, []string{}, result)

}
