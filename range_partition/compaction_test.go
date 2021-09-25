package range_partition

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/range_partition/table"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/stretchr/testify/require"
)

func TestCompaction(t *testing.T) {

	br := streamclient.NewMockBlockReader()
	logStream := streamclient.NewMockStreamClient("log", br)
	rowStream := streamclient.NewMockStreamClient("sst", br)
	metaStream := streamclient.NewMockStreamClient("meta", br)

	defer logStream.Close()
	defer rowStream.Close()
	defer metaStream.Close()

	rp, err := OpenRangePartition(3, metaStream, rowStream, logStream, br,
		[]byte(""), []byte(""), func(si pb.StreamInfo) streamclient.StreamClient {
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
	beforeNums := len(rp.tables)
	for _, t := range rp.tables {
		tbls = append(tbls, t)
	}
	rp.tableLock.RUnlock()

	rp.doCompact(tbls, true)
	fmt.Printf("after compaction %d\n", len(rp.tables))
	afterNums := len(rp.tables)

	require.Less(t, afterNums,beforeNums)

	//no key was lost
	keys := rp.Range([]byte(""), []byte(""), 5000)
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("%04d", i)
		require.Equal(t, k, string(keys[i]))
	}

	//location was written to meta
	block , err := metaStream.ReadLastBlock(context.Background())
	require.Nil(t, err)
	var tableLocs pspb.TableLocations
	utils.MustUnMarshal(block.Data, &tableLocs)
	require.Equal(t, afterNums, len(tableLocs.Locs))

	rp.Close()
}


func TestDicardBigData(t *testing.T) {
	br := streamclient.NewMockBlockReader()
	logStream := streamclient.NewMockStreamClient("log", br)
	rowStream := streamclient.NewMockStreamClient("sst", br)
	metaStream := streamclient.NewMockStreamClient("meta", br)

	defer logStream.Close()
	defer rowStream.Close()
	defer metaStream.Close()

	rp, err := OpenRangePartition(1, metaStream, rowStream, logStream, br,
		[]byte(""), []byte(""), func(si pb.StreamInfo) streamclient.StreamClient {
			return streamclient.OpenMockStreamClient(si, br)
		}, TestOption())
	
	require.Nil(t, err)

	data1 := []byte(fmt.Sprintf("data1%01048576d", 10)) //1MB
	data2 := []byte(fmt.Sprintf("data2%01048576d", 10)) //1MB
	require.Nil(t, rp.Write([]byte("a"),data1)) 
	require.Nil(t, rp.Write([]byte("b"),data2)) 

	rp.Delete([]byte("a"))
	rp.Delete([]byte("b"))


	
	var wg sync.WaitGroup
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		k := fmt.Sprintf("%04d", i)
		v := make([]byte, 1000)
		rp.WriteAsync([]byte(k), []byte(v), func(e error) {
			wg.Done()
		})
	}
	wg.Wait()
	


	rp.Close() //FORCE rp flush table



	//open again
	rp, err = OpenRangePartition(1, metaStream, rowStream, logStream, br,
		[]byte(""), []byte(""), func(si pb.StreamInfo) streamclient.StreamClient {
			return streamclient.OpenMockStreamClient(si, br)
		}, TestOption())
	
	require.Nil(t, err)


	var tbls []*table.Table
	rp.tableLock.RLock()
	for _, t := range rp.tables {
		tbls = append(tbls, t)
	}
	rp.tableLock.RUnlock()


	rp.doCompact(tbls, true)
	tbls = tbls[:0]


	rp.Write([]byte("c"),data1)
	rp.Delete([]byte("c"))

	require.Equal(t, 1, len(rp.tables))
	require.Equal(t, 2, len(rp.tables[0].Discards))
	/*
	for _, t := range rp.tables {
		fmt.Printf("table %v ,size %d discards %v\n", t.Loc, t.EstimatedSize,  t.Discards)
	}
	*/

	rp.Close()



	//open rp again again
	rp, err = OpenRangePartition(1, metaStream, rowStream, logStream, br,
		[]byte(""), []byte(""), func(si pb.StreamInfo) streamclient.StreamClient {
			return streamclient.OpenMockStreamClient(si, br)
		}, TestOption())
	
	require.Nil(t, err)
	defer rp.Close()



	rp.tableLock.RLock()
	for _, t := range rp.tables {
		tbls = append(tbls, t)
	}
	rp.tableLock.RUnlock()


	rp.doCompact(tbls, true)
	tbls = tbls[:0]


	require.Equal(t, 3, len(rp.tables[len(rp.tables) - 1].Discards))
	
	/*
	for _, t := range rp.tables {
		fmt.Printf("table %v ,size %d discards %v\n", t.Loc, t.EstimatedSize,  t.Discards)
	}
	*/




}