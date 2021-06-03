package rangepartition

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/manager/pmclient"
	"github.com/journeymidnight/autumn/rangepartition/table"
	"github.com/journeymidnight/autumn/streamclient"
)

func TestCompaction(t *testing.T) {
	logStream := streamclient.NewMockStreamClient("log")
	rowStream := streamclient.NewMockStreamClient("sst")
	pmclient := new(pmclient.MockPMClient)

	defer logStream.Close()
	defer rowStream.Close()

	rp := OpenRangePartition(3, rowStream, logStream, logStream.(streamclient.BlockReader),
		[]byte(""), []byte(""), nil, nil, pmclient, streamclient.OpenMockStreamClient)
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

	fmt.Printf("before compaction %d\n", len(rp.tables))

	rp.tableLock.RLock()
	for _, t := range rp.tables {
		tbls = append(tbls, t)
	}
	rp.tableLock.RUnlock()

	rp.doCompact(tbls, true)
	rp.deprecateTables(tbls)
	for _, t := range tbls {
		t.DecrRef()
	}
	fmt.Printf("after compaction %d\n", len(rp.tables))

}
