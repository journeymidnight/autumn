package streamclient

import (
	"context"
	"io"
	"os"

	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/rangepartition/y"
	"github.com/journeymidnight/autumn/utils"
)

type MockStreamClient struct {
	ex       *extent.Extent
	fileName string
}

func NewMockStreamClient(fileName string, id uint64) StreamClient {
	ex, err := extent.CreateExtent(fileName, id)
	utils.Check(err)
	return &MockStreamClient{
		ex:       ex,
		fileName: fileName,
	}
}

func (client *MockStreamClient) Truncate(ctx context.Context, extentID uint64) error {
	return nil
}

//block API, entries has been batched
func (client *MockStreamClient) AppendEntries(ctx context.Context, entries []*pb.EntryInfo) (uint64, uint32, error) {
	client.ex.Lock()
	commitLength := client.ex.CommitLength()
	blocks, j, k := entriesToBlocks(entries, commitLength)
	defer client.ex.Unlock()
	offsets, err := client.ex.AppendBlocks(blocks, nil)
	if err != nil {
		return 0, 0, err
	}
	for i := 0; i < len(entries)-j; i++ {
		entries[j+i].ExtentID = 100
		entries[j+i].Offset = offsets[k+i]
	}
	//change entries
	return 100, client.ex.CommitLength(), nil
}

//block API
func (client *MockStreamClient) Append(ctx context.Context, blocks []*pb.Block) (uint64, []uint32, error) {
	commitLength := client.ex.CommitLength()
	client.ex.Lock()
	offsets, err := client.ex.AppendBlocks(blocks, &commitLength)
	client.ex.Unlock()
	return 100, offsets, err
}

func (client *MockStreamClient) Close() {
	client.ex.Close()
	os.Remove(client.fileName)
}

func (client *MockStreamClient) Connect() error {
	return nil
}

func (client *MockStreamClient) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, error) {
	blocks, err := client.ex.ReadBlocks(offset, numOfBlocks, (32 << 20))
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
	} else {
		x.currentOffset = opt.Offset
	}
	return x
}

type MockLockEntryIter struct {
	sc            *MockStreamClient
	currentOffset uint32
	opt           ReadOption
	noMore        bool
	cache         []*pb.EntryInfo
}

func (iter *MockLockEntryIter) HasNext() (bool, error) {
	if len(iter.cache) == 0 {
		if iter.noMore {
			return false, nil
		}
		err := iter.receiveBlocks()
		if err != nil {
			return false, err
		}
	}
	return len(iter.cache) > 0, nil
}

func (iter *MockLockEntryIter) receiveBlocks() error {
	blocks, err := iter.sc.ex.ReadBlocks(iter.currentOffset, 8, (32 << 20))
	if err != nil && err != extent.EndOfExtent && err != extent.EndOfStream {
		return err
	}

	if err == extent.EndOfExtent || err == extent.EndOfStream {
		iter.noMore = true
	}
	iter.cache = nil
	offset := iter.currentOffset

	for i := range blocks {
		for _, entry := range y.ExtractLogEntry(blocks[i]) {
			if ShouldWriteValueToLSM(entry) {
				if iter.opt.Replay { //replay read
					iter.cache = append(iter.cache, &pb.EntryInfo{
						Log:           entry,
						EstimatedSize: uint64(entry.Size()),
						ExtentID:      100,
						Offset:        offset,
					})
				} else { //gc read
					iter.cache = append(iter.cache, &pb.EntryInfo{
						Log:           &pb.Entry{},
						EstimatedSize: uint64(blocks[i].BlockLength) + 512,
						ExtentID:      100,
						Offset:        offset,
					})
					break
				}
			} else {
				//big value
				entry.Value = nil
				entry.Meta |= uint32(y.BitValuePointer)
				iter.cache = append(iter.cache, &pb.EntryInfo{
					Log:           entry,
					EstimatedSize: uint64(blocks[i].BlockLength) + 512,
					ExtentID:      100,
					Offset:        offset,
				})
				//utils.AssertTrue(len(mix.Offsets) == 2)
				break
			}
		}
		offset += 512 + blocks[i].BlockLength
	}
	iter.currentOffset += SizeOfBlocks(blocks)
	return nil
}

func (iter *MockLockEntryIter) Next() *pb.EntryInfo {
	if ok, err := iter.HasNext(); !ok || err != nil {
		return nil
	}
	ret := iter.cache[0]
	iter.cache = iter.cache[1:]
	return ret
}
