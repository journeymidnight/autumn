package rangepartition

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/rangepartition/skiplist"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/xlog"

	"go.uber.org/zap/zapcore"

	"github.com/journeymidnight/autumn/rangepartition/y"
	"github.com/stretchr/testify/require"
)

func init() {
	xlog.InitLog([]string{"rp.log"}, zapcore.DebugLevel)
}

func TestEstimateSize(t *testing.T) {
	bigValue := []byte(fmt.Sprintf("%01048576d", 10)) //1MB
	smallValue := []byte(fmt.Sprintf("%01048d", 10))  //1KB

	requests := []*request{
		&request{
			entries: []*pb.EntryInfo{{Log: &pb.Entry{Key: y.KeyWithTs([]byte("hello"), 0), Value: []byte("test")}}},
		},
		&request{
			entries: []*pb.EntryInfo{
				{
					Log: &pb.Entry{
						Key:   y.KeyWithTs([]byte("hello1"), 0),
						Value: bigValue,
					},
				},
			},
		},
		&request{
			entries: []*pb.EntryInfo{
				{
					Log: &pb.Entry{
						Key:   y.KeyWithTs([]byte("hello2"), 0),
						Value: smallValue,
					},
				},
			},
		},
		&request{
			entries: []*pb.EntryInfo{
				{
					Log: &pb.Entry{
						Key:       y.KeyWithTs([]byte("hello3"), 0),
						Value:     []byte("testasdfasdfasdfasdfasdfafafasdfasdfa"),
						ExpiresAt: 1243434343434,
					},
				},
			},
		},
	}

	x := skiplist.NewSkiplist(10 * MB)
	pre := x.MemSize()
	for i := range requests {
		l := int64(estimatedSizeInSkl(requests[i].entries))
		_writeToLSM(x, requests[i])
		fmt.Printf("%d <= %d\n", x.MemSize()-pre, l)
		require.True(t, x.MemSize()-pre <= l)
		pre = x.MemSize()
	}

}

//helper function for TestEstimateSize.

func _writeToLSM(skl *skiplist.Skiplist, req *request) int64 {
	for _, entry := range req.entries {
		if y.ShouldWriteValueToLSM(entry.Log) { // Will include deletion / tombstone case.
			skl.Put(entry.Log.Key,
				y.ValueStruct{
					Value:     entry.Log.Value,
					Meta:      getLowerByte(entry.Log.Meta),
					UserMeta:  getLowerByte(entry.Log.UserMeta),
					ExpiresAt: entry.Log.ExpiresAt,
				})
		} else {
			vp := valuePointer{
				entry.ExtentID,
				entry.Offset,
			}
			skl.Put(entry.Log.Key,
				y.ValueStruct{
					Value:     vp.Encode(),
					Meta:      getLowerByte(entry.Log.Meta) | y.BitValuePointer,
					UserMeta:  getLowerByte(entry.Log.UserMeta),
					ExpiresAt: entry.Log.ExpiresAt,
				})
		}
	}

	return skl.MemSize()
}

// Opens a badger db and runs a a test on it.
func runRPTest(t *testing.T, test func(t *testing.T, rp *RangePartition)) {

	logStream := streamclient.NewMockStreamClient(fmt.Sprintf("%d.vlog", rand.Uint32()), 10)
	rowStream := streamclient.NewMockStreamClient(fmt.Sprintf("%d.sst", rand.Uint32()), 12)

	defer logStream.Close()
	defer rowStream.Close()
	rp := OpenRangePartition(rowStream, logStream, logStream.(streamclient.BlockReader))
	defer func() {
		require.NoError(t, rp.Close())
	}()
	test(t, rp)
}

func TestWriteRead(t *testing.T) {
	runRPTest(t, func(t *testing.T, rp *RangePartition) {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			rp.writeAsync([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)), func(e error) {
				wg.Done()
			})
			//rp.write([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
		}
		wg.Wait()

		for i := 0; i < 100; i++ {
			v, err := rp.get([]byte(fmt.Sprintf("key%d", i)), 300)
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("val%d", i)), v)
		}

	})
}

func TestUpdateRead(t *testing.T) {
	runRPTest(t, func(t *testing.T, rp *RangePartition) {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			rp.writeAsync([]byte("key"), []byte(fmt.Sprintf("val%d", i)), func(e error) {
				wg.Done()
			})
		}
		wg.Wait()

		value, err := rp.get([]byte("key"), 0)
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("val%d", 99)), value)

	})
}

func TestGetBig(t *testing.T) {
	runRPTest(t, func(t *testing.T, rp *RangePartition) {
		//txnSet(t, db, []byte("key1"), []byte("val1"), 0x08)
		bigValue := []byte(fmt.Sprintf("%01048576d", 10))
		err := rp.write([]byte("key1"), bigValue)
		require.NoError(t, err)

		v, err := rp.get([]byte("key1"), 0)

		require.NoError(t, err)
		require.Equal(t, len(bigValue), len(v))

	})

}

/*
0. interface for KV
1. replay/compact
2. big iterator
3. test
*/
