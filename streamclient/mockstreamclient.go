package streamclient

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/pkg/errors"
)

var (
	testThreshold = 1 * MB
	errNoTrucate  = errors.New("not have to truncate")
)

type MockStreamClient struct {
	exs    []*extent.Extent
	names  []string
	ID     uint64
	suffix string
}

/*

type saved struct {
	exs   []*extent.Extent
	names []string
}

var  map[uint64]saved
*/

var (
	fileNamePrefix = "mockextent"
)

func fileName(id uint64, suffix string) string {
	return fmt.Sprintf("%s_%d.%s", fileNamePrefix, id, suffix)
}

func NewMockStreamClient(suffix string) StreamClient {
	sID := uint64(rand.Uint32())
	name := fileName(sID, suffix)

	ex, err := extent.CreateExtent(name, sID)
	utils.Check(err)
	return &MockStreamClient{
		exs:    []*extent.Extent{ex},
		ID:     sID,
		suffix: suffix,
	}
}

//only open log file
func OpenMockStreamClient(si pb.StreamInfo) StreamClient {
	sID := si.StreamID
	var exs []*extent.Extent
	for _, eID := range si.ExtentIDs {
		name := fileName(eID, "log")
		ex, err := extent.OpenExtent(name)
		utils.Check(err)
		exs = append(exs, ex)

	}
	return &MockStreamClient{
		exs:    exs,
		ID:     sID,
		suffix: "log",
	}
}

func UpdateStreamMock([]pb.StreamInfo) {

}

func (client *MockStreamClient) Truncate(ctx context.Context, extentID uint64) (pb.StreamInfo, pb.StreamInfo, error) {

	var i int
	for i = range client.exs {
		if client.exs[i].ID == extentID {
			break
		}
	}
	if i == 0 {
		return pb.StreamInfo{}, pb.StreamInfo{}, errNoTrucate
	}

	f := func(id uint64, start int, end int) pb.StreamInfo {
		var array []uint64
		for i := start; i < end; i++ {
			array = append(array, client.exs[i].ID)
		}
		return pb.StreamInfo{
			StreamID:  id,
			ExtentIDs: array,
		}
	}

	//update smclient...
	blobStreamID := rand.Uint64()
	blobStreamInfo := f(blobStreamID, 0, i)

	myStreamInfo := f(client.ID, i, len(client.exs))
	client.exs = client.exs[i:] //升级自己
	//生成新的blobstream,
	return blobStreamInfo, myStreamInfo, nil
}

//block API, entries has been batched
func (client *MockStreamClient) AppendEntries(ctx context.Context, entries []*pb.EntryInfo) (uint64, uint32, error) {
	//exID := len(client.exs) - 1
	//ex := client.exs[exID]
	//ex.Lock()
	//commitLength := ex.CommitLength()
	blocks, j, k := entriesToBlocks(entries)
	//defer ex.Unlock()
	exID, offsets, err := client.Append(ctx, blocks)
	//offsets, err := ex.AppendBlocks(blocks, nil)
	if err != nil {
		return 0, 0, err
	}
	for i := 0; i < len(entries)-j; i++ {
		entries[j+i].ExtentID = uint64(exID)
		entries[j+i].Offset = offsets[k+i]
	}

	tail := offsets[len(offsets)-1] + blocks[len(blocks)-1].BlockLength + 512
	//change entries
	return uint64(exID), tail, nil
}

//block API
func (client *MockStreamClient) Append(ctx context.Context, blocks []*pb.Block) (uint64, []uint32, error) {
	exIndex := len(client.exs) - 1
	exID := client.exs[exIndex].ID
	ex := client.exs[exIndex]
	commitLength := ex.CommitLength()
	ex.Lock()
	offsets, err := ex.AppendBlocks(blocks, &commitLength)
	ex.Unlock()
	if ex.CommitLength() > uint32(testThreshold) {
		//seal
		ex.Seal(ex.CommitLength())
		//create new
		eID := uint64(rand.Uint32())
		name := fileName(eID, client.suffix)
		newEx, err := extent.CreateExtent(name, eID)
		utils.Check(err)
		client.exs = append(client.exs, newEx)
	}
	return uint64(exID), offsets, err
}

func (client *MockStreamClient) Close() {
	for _, ex := range client.exs {
		name := fileName(ex.ID, client.suffix)
		ex.Close()
		os.Remove(name)
	}
}

func (client *MockStreamClient) Connect() error {
	return nil
}

func (client *MockStreamClient) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, error) {

	var ex *extent.Extent
	for i := range client.exs {
		if client.exs[i].ID == extentID {
			ex = client.exs[i]
			break
		}
	}
	if ex == nil {
		return nil, errors.New("extentID not good")
	}

	blocks, err := ex.ReadBlocks(offset, numOfBlocks, (32 << 20))
	if err == extent.EndOfExtent || err == extent.EndOfStream {
		return blocks, io.EOF
	}
	if err != nil {
		return nil, err
	}
	return blocks, err
}

func (client *MockStreamClient) NewLogEntryIter(opt ReadOption) LogEntryIter {

	x := &MockLockEntryIter{
		sc:  client,
		opt: opt,
	}
	if opt.ReadFromStart {
		x.currentOffset = 512 //skip extent header
		x.currentIndex = 0
	} else {
		x.currentOffset = opt.Offset

		for i := range client.exs {
			if client.exs[i].ID == opt.ExtentID {
				x.currentIndex = i
			}
		}

	}
	if opt.Replay {
		x.replay = true
	}

	return x
}

type MockLockEntryIter struct {
	sc            *MockStreamClient
	currentOffset uint32
	currentIndex  int
	opt           ReadOption
	noMore        bool
	cache         []*pb.EntryInfo
	replay        bool
}

func (iter *MockLockEntryIter) HasNext() (bool, error) {
	if len(iter.cache) == 0 {
		if iter.noMore {
			return false, nil
		}
		err := iter.receiveEntries()
		if err != nil {
			return false, err
		}
	}
	return len(iter.cache) > 0, nil
}

func (iter *MockLockEntryIter) receiveEntries() error {
	ex := iter.sc.exs[iter.currentIndex]

	res, tail, err := ex.ReadEntries(iter.currentOffset, 16*KB, iter.replay)

	if len(res) > 0 {
		iter.cache = nil
		iter.cache = append(iter.cache, res...)
	}
	switch err {
	case nil:
		iter.currentOffset = tail
		return nil
	case extent.EndOfExtent:
		iter.currentOffset = 512 //skip extent header
		iter.currentIndex++
		if iter.currentIndex == len(iter.sc.exs) {
			iter.noMore = true
		}
		return nil
	case extent.EndOfStream:
		iter.noMore = true
		return nil
	default:
		return errors.Errorf("unexpected error %s", err.Error())
	}
}

func (iter *MockLockEntryIter) Next() *pb.EntryInfo {
	if ok, err := iter.HasNext(); !ok || err != nil {
		return nil
	}
	ret := iter.cache[0]
	iter.cache = iter.cache[1:]
	return ret
}
