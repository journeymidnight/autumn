package range_partition

import (
	"context"
	"fmt"
	"testing"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/stretchr/testify/require"
)

/*
func init() {
	xlog.InitLog([]string{"rp.log"}, zapcore.DebugLevel)
}
*/

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

	extentID, offset, err := logStream.AppendEntries(context.Background(), cases)
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
	replayLog(logStream, extentID, offset, false, func(ei *pb.EntryInfo) (bool, error) {
		fmt.Printf("%s\n", ei.Log.Key)
		require.Equal(t, expecteEI[i], ei)
		i++
		return true, nil
	})

}
