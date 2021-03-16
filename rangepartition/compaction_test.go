package rangepartition

import (
	"fmt"
	"sync"
	"testing"

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
		[]byte(""), []byte(""), nil, nil, pmclient, streamclient.OpenMockStreamClient, streamclient.UpdateStreamMock)
	defer rp.Close()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		k := fmt.Sprintf("%04d", i)
		v := fmt.Sprintf("%d", i)
		rp.WriteAsync([]byte(k), []byte(v), func(e error) {
			wg.Done()
		})
	}
	wg.Wait()

	var tbls []*table.Table

	rp.tableLock.RLock()
	for _, t := range rp.tables {
		t.IncrRef()
		tbls = append(tbls, t)
	}
	rp.tableLock.RUnlock()

	rp.doCompact(tbls, true)
	fmt.Printf("%d\n", len(rp.tables))
}
