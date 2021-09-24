package range_partition

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/stretchr/testify/require"
)

//WARNING: mockstreamclient.testThreshold MUST BE 1M to run this test.
func TestRunGCMiddle(t *testing.T) {
	br := streamclient.NewMockBlockReader()
	logStream := streamclient.NewMockStreamClient("log", br)
	rowStream := streamclient.NewMockStreamClient("sst", br)
	metaStream := streamclient.NewMockStreamClient("meta", br)

	defer logStream.Close()
	defer rowStream.Close()
	defer metaStream.Close()


	rp, _ := OpenRangePartition(3, metaStream, rowStream, logStream, br,
		[]byte(""), []byte(""), func(si pb.StreamInfo) streamclient.StreamClient {
			return streamclient.OpenMockStreamClient(si, br)
		}, TestOption())

	for i := 0 ;i < 2 ; i ++ {
		require.Nil(t, rp.Write([]byte(fmt.Sprintf("a%d", i)), []byte("xx")))
		require.Nil(t, rp.Write([]byte(fmt.Sprintf("b%d", i)), []byte(fmt.Sprintf("%01048576d", 10)))) //1MB
		require.Nil(t, rp.Write([]byte(fmt.Sprintf("c%d", i)), []byte("xx")))
	}
	
	//a0,b0 on the first
	//c0,a1,b1 on the second
	//c1 on the third
	require.Equal(t, 3, len(logStream.StreamInfo().ExtentIDs))

	//a0, b0
	//c1
	//b1 (c0, a1被删除, 仍然在table中存在)
	secondEx := logStream.StreamInfo().ExtentIDs[1]
	fmt.Printf("run GC on second Extent %d\n", secondEx)
	rp.runGC(secondEx)

	v , err := rp.Get([]byte("c0"), 0)
	require.Nil(t, err)
	require.Equal(t, []byte("xx"), v)

	expectedValue := []string{"a0","b0", "c1", "b1"}
	var results []string
	replayLog(logStream, func(ei *pb.EntryInfo) (bool, error) {
		fmt.Printf("%s\n", streamclient.FormatEntry(ei))
		results = append(results, string(y.ParseKey(ei.Log.Key)))
		return true, nil
	}, streamclient.WithReadFromStart(math.MaxUint32), streamclient.WithReplay())
	
	require.Equal(t, expectedValue, results)
	
}
//WARNING: mockstreamclient.testThreshold MUST BE 1M to run this test.
func TestRunGCMove(t *testing.T) {

	br := streamclient.NewMockBlockReader()
	logStream := streamclient.NewMockStreamClient("log", br)
	rowStream := streamclient.NewMockStreamClient("sst", br)
	metaStream := streamclient.NewMockStreamClient("meta", br)

	defer logStream.Close()
	defer rowStream.Close()
	defer metaStream.Close()


	rp, _ := OpenRangePartition(3, metaStream, rowStream, logStream, br,
		[]byte(""), []byte(""), func(si pb.StreamInfo) streamclient.StreamClient {
			return streamclient.OpenMockStreamClient(si, br)
		}, TestOption())
	
	
	for i := 0 ;i < 2 ; i ++ {
		require.Nil(t, rp.Write([]byte(fmt.Sprintf("a%d", i)), []byte("xx")))
		require.Nil(t, rp.Write([]byte(fmt.Sprintf("b%d", i)), []byte(fmt.Sprintf("%01048576d", 10)))) //1MB
		require.Nil(t, rp.Write([]byte(fmt.Sprintf("c%d", i)), []byte("xx")))
	}
	
	//a0,b0 on the first
	//c0,a1,b1 on the second
	//c1 on the third
	require.Equal(t, 3, len(logStream.StreamInfo().ExtentIDs))


	rp.Delete([]byte("a0"))
	
	firstEx := logStream.StreamInfo().ExtentIDs[0]
	fmt.Printf("run GC on second Extent %d\n", firstEx)
	rp.runGC(firstEx)

	/*
	c0 on first, flag [0] 
    a1 on first, flag [0] 
    b1 on first, flag [2] 
    c1 on second, flag [0] 
    a0 on second, flag [1] a0 is delete tag
    b0 on second, flag [2] 
	*/
	expectedValue := []string{"c0", "a1", "b1", "c1", "a0", "b0"}
	var result []string
	replayLog(logStream, func(ei *pb.EntryInfo) (bool, error) {
		result = append(result, string(y.ParseKey(ei.Log.Key)))
		return true, nil
	}, streamclient.WithReadFromStart(math.MaxUint32), streamclient.WithReplay())
	require.Equal(t, expectedValue, result)
}
func TestLogReplay(t *testing.T) {

	val1 := []byte("sampleval012345678901234567890123")
	val2 := []byte(fmt.Sprintf("%01048576d", 10)) // Return 1MB value which is > math.MaxUint16.

	cases := []*pb.EntryInfo{
		{
			Log: &pb.Entry{
				Key:   []byte("a"),
				Value: val1,
			},
		},
		{
			Log: &pb.Entry{
				Key:   []byte("a1"),
				Value: val1,
			},
		},
		{
			Log: &pb.Entry{
				Key:   []byte("b"),
				Value: val2,
			},
		},
	}

	logStream := streamclient.NewMockStreamClient("log", streamclient.NewMockBlockReader())
	defer logStream.Close()

	extentID, offset, err := logStream.AppendEntries(context.Background(), cases, true)
	require.NoError(t, err)
	expecteEI := []*pb.EntryInfo{
		{
			Log: &pb.Entry{
				Key:   []byte("a"),
				Value: val1,
			},
			ExtentID:      100,
			Offset:        512,
			EstimatedSize: 38},
		{
			Log: &pb.Entry{
				Key:   []byte("a1"), //key is one byte bigger than previous
				Value: val1,
			},
			ExtentID:      100,
			Offset:        512,
			EstimatedSize: 39},
		{
			Log: &pb.Entry{
				Key:   []byte("b"),
				Value: nil,
				Meta:  2,
			},
			ExtentID:      100,
			Offset:        4096 + 512 /*mix block size*/ + 512, /*extent header*/
			EstimatedSize: 1049600,
		},
	}
	i := 0
	replayLog(logStream, func(ei *pb.EntryInfo) (bool, error) {
		fmt.Printf("%s\n", ei.Log.Key)
		require.Equal(t, expecteEI[i], ei)
		i++
		return true, nil
	}, streamclient.WithReadFrom(extentID, offset, math.MaxUint32))

}
