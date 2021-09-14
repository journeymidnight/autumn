package range_partition

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/range_partition/table"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/stretchr/testify/require"
)

func TestCompaction(t *testing.T) {

	br := streamclient.NewMockBlockReader()
	logStream := streamclient.NewMockStreamClient("log", br)
	rowStream := streamclient.NewMockStreamClient("sst", br)

	defer logStream.Close()
	defer rowStream.Close()

	var server streamclient.MockEtcd
	rp, err := OpenRangePartition(3, rowStream, logStream, br,
		[]byte(""), []byte(""), nil, nil, server.SetRowStreamTables, func(si pb.StreamInfo) streamclient.StreamClient {
			return streamclient.OpenMockStreamClient(si, br)
		}, TestOption())

	require.Nil(t, err)
	defer rp.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5000; i++ {
		wg.Add(1)
		k := fmt.Sprintf("%04d", i)
		v := make([]byte, 1000)
		rp.WriteAsync([]byte(k), []byte(v), func(e error) {
			wg.Done()
		})
	}
	wg.Wait()

	var tbls []*table.Table

	time.Sleep(time.Second)

	rp.tableLock.RLock()
	fmt.Printf("before compaction %d\n", len(rp.tables))
	for _, t := range rp.tables {
		tbls = append(tbls, t)
	}
	rp.tableLock.RUnlock()

	rp.doCompact(tbls, true)
	rp.removeTables(tbls)
	fmt.Printf("after compaction %d\n", len(rp.tables))

}
