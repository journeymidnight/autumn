package extent

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	_ = fmt.Println
)

//struct memory is for test
type memory struct {
	vec  []byte
	pos  int
	end  int
	size int
}

func newMemory(size int) *memory {
	return &memory{
		vec:  make([]byte, size),
		pos:  0,
		end:  0,
		size: size,
	}
}

func (f *memory) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		f.pos += int(offset)
	default:
		return 0, errors.New("bytes.Reader.Seek: only support SeekCurrent")
	}
	return int64(f.pos), nil
}

func (f *memory) resetPos() {
	f.pos = 0
}

func (f *memory) Read(buf []byte) (n int, err error) {
	if f.pos >= f.end {
		return 0, io.EOF
	}
	n = copy(buf, f.vec[f.pos:])
	f.pos += n
	return n, nil
}

func (f *memory) Write(p []byte) (n int, err error) {
	if f.pos >= f.size {
		return -1, io.ErrShortBuffer
	}
	d := utils.Min(len(p), f.size-f.end)
	n = copy(f.vec[f.pos:], p[:d])
	f.pos += n
	f.end = utils.Max(f.end, f.pos)
	return n, nil
}

func TestReadWriteBlockUserData(t *testing.T) {
	data := make([]byte, 1024)
	block := pb.Block{
		CheckSum:    utils.AdlerCheckSum(data),
		BlockLength: 1024,
		Data:        data,
		UserData:    []byte("hello"),
	}

	f := newMemory(3000)
	err := writeBlock(f, &block)
	assert.Nil(t, err)

	f.resetPos()
	block1, err := readBlock(f)

	assert.Equal(t, block, block1)
}

func TestReadWriteBlock(t *testing.T) {
	data := make([]byte, 1024)
	block := pb.Block{
		CheckSum:    utils.AdlerCheckSum(data),
		BlockLength: 1024,
		Data:        data,
	}

	f := newMemory(3000)
	err := writeBlock(f, &block)
	assert.Nil(t, err)

	f.resetPos()
	block1, err := readBlock(f)

	assert.Equal(t, block, block1)
}

func generateBlock(name string, size uint32) *pb.Block {
	data := make([]byte, size)
	utils.SetRandStringBytes(data)
	return &pb.Block{
		CheckSum:    utils.AdlerCheckSum(data),
		BlockLength: size,
		Data:        data,
	}
}

func TestAppendReadFile(t *testing.T) {
	cases := []*pb.Block{
		generateBlock("object1", 4096),
		generateBlock("object2", 4096),
		generateBlock("object3", 8192),
		generateBlock("object4", 4096),
	}

	extent, err := CreateExtent("localtest.ext", 100)
	defer os.Remove("localtest.ext")
	assert.Nil(t, err)
	extent.Lock()
	ret, err := extent.AppendBlocks(cases, nil)
	extent.Unlock()
	assert.Nil(t, err)

	//single thread read
	retBlocks, err := extent.ReadBlocks(ret[0], 4, (20 << 20))

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
				blocks, err := extent.ReadBlocks(ele.offset, 1, (20 << 20))
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
		generateBlock("object1", 4096),
		generateBlock("object2", 4096),
		generateBlock("object3", 8192),
		generateBlock("object4", 4096),
	}

	extent, err := CreateExtent(extentName, 100)
	defer os.Remove(extentName)
	assert.Nil(t, err)
	extent.Lock()
	_, err = extent.AppendBlocks(cases, nil)
	extent.Unlock()
	assert.Nil(t, err)

	extent.Close()

	//open append extent, replay all the data
	ex, err := OpenExtent(extentName)
	assert.Nil(t, err)
	assert.False(t, ex.IsSeal())
	assert.Equal(t, uint32(512*5+4096*3+8192), ex.CommitLength())

	//write new cases
	ex.Lock()
	_, err = ex.AppendBlocks(cases, nil)
	ex.Unlock()
	assert.Nil(t, err)
	commit := ex.CommitLength()

	err = ex.Seal(ex.commitLength)
	assert.Nil(t, err)
	defer os.Remove("localtest.idx")
	ex.Close()

	//open sealed extent
	ex, err = OpenExtent(extentName)
	assert.Nil(t, err)
	assert.True(t, ex.IsSeal())
	assert.Equal(t, commit, ex.CommitLength())

	//read test
	blocks, err := ex.ReadBlocks(512, 1, (20 << 20)) //read object1

	assert.Nil(t, err)
	assert.Equal(t, cases[0], blocks[0])

}

func TestExtentHeader(t *testing.T) {
	header := newExtentHeader(3)
	assert.Equal(t, extentMagicNumber, string(header.magicNumber))

	f := newMemory(512)
	err := header.Marshal(f)
	assert.Nil(t, err)

	f.resetPos()

	newHeader := newExtentHeader(0)
	newHeader.Unmarshal(f)

	assert.Equal(t, header, newHeader)

}

func BenchmarkExtent(b *testing.B) {
	extent, err := CreateExtent("localtest.ext", 100)
	defer os.Remove("localtest.ext")
	if err != nil {
		panic(err.Error())
	}
	n := uint32(4096)
	block := generateBlock("test", n)
	commit := uint32(512)
	extent.Lock()
	for i := 0; i < b.N; i++ {
		_, err = extent.AppendBlocks([]*pb.Block{
			block,
		}, &commit)

		if err != nil {
			panic(err.Error())
		}
		commit += 512 + n
	}
}
