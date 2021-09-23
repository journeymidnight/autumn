package extent

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/journeymidnight/autumn/extent/record"
	"github.com/journeymidnight/autumn/extent/wal"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"go.uber.org/zap/zapcore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	_ = fmt.Println
)



func init() {
	xlog.InitLog([]string{"extent_test.log"}, zapcore.DebugLevel)
}

func generateBlock(size uint32) *pb.Block {
	data := make([]byte, size)
	utils.SetRandStringBytes(data)
	return &pb.Block{
		Data: data,
	}
}

func TestReadEntries(t *testing.T) {
	entry := new(pb.Entry)
	entry.Key = []byte("key")
	entry.Value = []byte("value")
	data, err := entry.Marshal()
	require.Nil(t, err)
	extent, err := CreateExtent("localtest.ext", 100)
	defer os.Remove("localtest.ext")
	extent.Lock()
	extent.AppendBlocks([]*pb.Block{{Data: data}}, true)
	extent.Unlock()
	entries, end, err := extent.ReadEntries(0, 10<<20, true)
	for i := range entries {
		require.Equal(t, uint64(100), entries[i].ExtentID)
		require.Equal(t, []byte("key"), entries[i].Log.Key)
		require.Equal(t, []byte("value"), entries[i].Log.Value)
	}
	fmt.Println(end)
}

func TestAppendReadFile(t *testing.T) {
	cases := []*pb.Block{
		generateBlock(4096),
		generateBlock(4096),
		generateBlock(8192),
		generateBlock(4096),
	}

	extent, err := CreateExtent("localtest.ext", 100)
	defer os.Remove("localtest.ext")
	//extent.ResetWriter()
	assert.Nil(t, err)
	extent.Lock()
	ret, _, err := extent.AppendBlocks(cases, true)
	extent.Unlock()
	assert.Nil(t, err)

	//single thread read
	retBlocks, _, _, err := extent.ReadBlocks(ret[0], 4, (20 << 20))

	assert.Nil(t, err)
	assert.Equal(t, cases, retBlocks)

	//multithread read, push offset index into chan

	type tuple struct {
		caseIndex uint32
		offset    uint32
	}
	ch := make(chan tuple)

	complets := int32(0)
	//create 2 threads
	for i := 0; i < 2; i++ {
		go func() {
			for ele := range ch {
				blocks, _, _, err := extent.ReadBlocks(ele.offset, 1, (20 << 20))
				require.Nil(t, err)

				require.Equal(t, cases[ele.caseIndex], blocks[0])
				atomic.AddInt32(&complets, 1)
				if atomic.LoadInt32(&complets) == int32(len(cases)) {
					close(ch)
				}
			}
		}()
	}

	for i, offset := range ret {
		ch <- tuple{
			caseIndex: uint32(i),
			offset:    offset,
		}
	}

}

func TestReplayExtent(t *testing.T) {

	extentName := "localtest.ext"
	cases := []*pb.Block{
		generateBlock(4096),
		generateBlock(4096),
		generateBlock(8192),
		generateBlock(4096),
	}

	extent, err := CreateExtent(extentName, 100)
	//extent.ResetWriter()
	defer os.Remove(extentName)
	assert.Nil(t, err)
	extent.Lock()
	_, _, err = extent.AppendBlocks(cases, true)
	extent.Unlock()
	assert.Nil(t, err)
	extent.Close()

	ex, err := OpenExtent(extentName)
	assert.Nil(t, err)
	assert.False(t, ex.IsSeal())
	//ex.ResetWriter()

	//write new cases
	ex.Lock()
	_, _, err = ex.AppendBlocks(cases, true)
	ex.Unlock()

	assert.Nil(t, err)
	commit := ex.CommitLength()
	ex.Lock()
	err = ex.Seal(ex.commitLength)
	ex.Unlock()
	assert.Nil(t, err)
	defer os.Remove("localtest.idx")
	ex.Close()

	//open sealed extent
	ex, err = OpenExtent(extentName)
	assert.Nil(t, err)
	assert.True(t, ex.IsSeal())
	assert.Equal(t, commit, ex.CommitLength())

	//read test
	blocks, _, _, err := ex.ReadBlocks(0, 1, (20 << 20)) //read object1

	assert.Nil(t, err)
	assert.Equal(t, cases[0], blocks[0])

}

func TestWalExtent(t *testing.T) {
	extent, err := CreateExtent("localtest.ext", 100)
	//extent.ResetWriter()
	defer os.Remove("localtest.ext")
	if err != nil {
		panic(err.Error())
	}
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	defer os.RemoveAll(p)

	walLog, err := wal.OpenWal(p, func() {
		extent.Sync()
	})
	require.Nil(t, err)

	cases := []*pb.Block{
		generateBlock(10),
		generateBlock(20),
		generateBlock(4 << 10),
		generateBlock(40 << 20),
		generateBlock(10),
	}

	extent.Lock()


	i := 0
	end := uint32(0)
	for _, block := range cases {
		start := extent.CommitLength()
		var wg sync.WaitGroup
		wg.Add(2) //2 tasks
		errC := make(chan error)
		go func() { //wal
			defer wg.Done()
			err := walLog.Write(100, start, 0, []*pb.Block{block})
			errC <- err
		}()

		go func() { //extent
			defer wg.Done()

			if i == len(cases)-1 { //skip last write
				errC <- nil
				return
			}

			_, end, err = extent.AppendBlocks([]*pb.Block{block}, false)
			errC <- err
		}()

		go func() {
			wg.Wait()
			close(errC)
		}()

		for err := range errC {
			require.Nil(t, err)
		}
		i++
	}
	walLog.Close()
	extent.Unlock()
	extent.Close()

	extent, err = OpenExtent("localtest.ext")
	require.Nil(t, err)
	walLog, err = wal.OpenWal(p, func() {})
	require.Nil(t, err)

	err = walLog.Replay(func(_ uint64, start uint32, rev int64, blocks []*pb.Block) {
		if err := extent.RecoveryData(start, rev, blocks); err != nil {
			t.Fatal(err.Error())
		}
	})

	require.Nil(t, err)

	//FIXME
	blocks, offsets, end, err := extent.ReadBlocks(0, uint32(len(cases)), 60<<20)
	//require.Nil(t, err)
	for i := range blocks {
		fmt.Printf("offset %d, len:%d\n", offsets[i], len(blocks[i].Data))
		require.Equal(t, cases[i].Data, blocks[i].Data)
	}
	fmt.Printf("End:%d\n", end)
}


func TestValidBlock(t *testing.T) {
	cases := []*pb.Block{
		generateBlock(4096),
		generateBlock(4096),
	}

	extent, err := CreateExtent("localtest.ext", 100)
	assert.Nil(t, err)


	start, err := extent.ValidAllBlocks(0)
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), start)


	defer os.Remove("localtest.ext")
	extent.Lock()
	extent.AppendBlocks(cases, true)
	extent.Unlock()

	//
	extent.file.Truncate(5000)
	extent.commitLength = 5000
	
	start, err = extent.ValidAllBlocks(0)
	
	fmt.Printf("start is %d, err is %v\n", start, err)
	require.Equal(t, record.ComputeEnd(0, 4096), start)
}




func TestReadLastBlock(t *testing.T) {
	
	cases := []*pb.Block{
		generateBlock(65<<10),
		generateBlock(1234),
		generateBlock(4096),
		generateBlock(1),
	}

	extent, err := CreateExtent("localtest.ext", 100)
	assert.Nil(t, err)
	defer os.Remove("localtest.ext")

	extent.Lock()
	extent.AppendBlocks(cases, true)
	extent.Unlock()


	block, start, tail, err := extent.ReadLastBlock()
	require.Nil(t, err)
	require.Equal(t, cases[len(cases) - 1], block[0])
	require.Equal(t, extent.commitLength, tail)

	//assert ComputeEnd
	_, offsets, _, err := extent.ReadBlocks(0, 100, 10<<20)
	offset := uint32(0)
	for i := 0; i < len(cases); i++ {
		require.Equal(t, offset, offsets[i])
		offset = record.ComputeEnd(offset, uint32(len(cases[i].Data)))
	}
	require.Equal(t, offsets[len(offsets) -1 ], start[0])
}
func TestWriteECFriendlyBlock(t *testing.T) {
	extent, err := CreateExtent("localtest.ext", 100)
	require.Nil(t, err)
	//extent.ResetWriter()
	defer os.Remove("localtest.ext")
	b1 := generateBlock(512 << 10)
	extent.Lock()
	extent.AppendBlocks([]*pb.Block{b1}, true)

	extent.AppendBlocks([]*pb.Block{b1}, true)
	extent.Unlock()

	blocks, _, _, err := extent.ReadBlocks(0, 2, 5<<20)
	require.Nil(t, err)
	require.Equal(t, b1, blocks[0])
	require.Equal(t, b1, blocks[1])

}


func BenchmarkExtentWithoutSync(b *testing.B) {
	extent, err := CreateExtent("localtest.ext", 100)
	//extent.ResetWriter()
	defer os.Remove("localtest.ext")
	if err != nil {
		panic(err.Error())
	}
	n := uint32(4096)
	block := generateBlock(n)
	extent.Lock()
	defer extent.Unlock()
	for i := 0; i < b.N; i++ {
		_, _, err = extent.AppendBlocks([]*pb.Block{
			block,
		}, false)

		if err != nil {
			panic(err.Error())
		}
	}
}

func BenchmarkExtent(b *testing.B) {
	extent, err := CreateExtent("localtest.ext", 100)
	//extent.ResetWriter()
	defer os.Remove("localtest.ext")
	if err != nil {
		panic(err.Error())
	}
	n := uint32(4096)
	block := generateBlock(n)
	extent.Lock()
	defer extent.Unlock()
	for i := 0; i < b.N; i++ {
		_, _, err = extent.AppendBlocks([]*pb.Block{
			block,
		}, true)

		if err != nil {
			panic(err.Error())
		}
	}

}
