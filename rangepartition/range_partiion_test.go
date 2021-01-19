package rangepartition

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/journeymidnight/autumn/proto/pspb"
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

	cases := []request{
		{
			Entry: pspb.Entry{
				Key:   y.KeyWithTs([]byte("hello"), 0),
				Value: []byte("test"),
			},
		},
		{
			Entry: pspb.Entry{
				Key:   y.KeyWithTs([]byte("hello1"), 0),
				Value: bigValue,
			},
		},
		{
			Entry: pspb.Entry{
				Key:   y.KeyWithTs([]byte("hello2"), 0),
				Value: smallValue,
			},
		},
		{
			Entry: pspb.Entry{
				Key:       y.KeyWithTs([]byte("hello3"), 0),
				Value:     []byte("testasdfasdfasdfasdfasdfafafasdfasdfa"),
				ExpiresAt: 1243434343434,
			},
		},
	}

	x := skiplist.NewSkiplist(10 * MB)
	pre := x.MemSize()
	for i := range cases {
		l := int64(estimatedSizeInSkl(&cases[i]))
		writeToLSM(x, &cases[i])
		fmt.Printf("%d <= %d\n", x.MemSize()-pre, l)
		require.True(t, x.MemSize()-pre <= l)
		pre = x.MemSize()
	}

}

func writeToLSM(skl *skiplist.Skiplist, req *request) int64 {
	entry := &req.Entry
	if shouldWriteValueToLSM(&req.Entry) { // Will include deletion / tombstone case.
		skl.Put(entry.Key,
			y.ValueStruct{
				Value:     entry.Value,
				Meta:      getLowerByte(entry.Meta),
				UserMeta:  getLowerByte(entry.UserMeta),
				ExpiresAt: entry.ExpiresAt,
			})
	} else {
		skl.Put(entry.Key,
			y.ValueStruct{
				Value:     req.vp.Encode(),
				Meta:      getLowerByte(entry.Meta) | bitValuePointer,
				UserMeta:  getLowerByte(entry.UserMeta),
				ExpiresAt: entry.ExpiresAt,
			})
	}
	return skl.MemSize()
}

// Opens a badger db and runs a a test on it.
func runRPTest(t *testing.T, test func(t *testing.T, rp *RangePartition)) {

	logStream := streamclient.NewMockStreamClient(fmt.Sprintf("%d.vlog", rand.Uint32()), 10)
	rowStream := streamclient.NewMockStreamClient(fmt.Sprintf("%d.sst", rand.Uint32()), 12)

	defer logStream.Close()
	defer rowStream.Close()
	rp := OpenRangePartition(rowStream, logStream)
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
