package streamclient

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/pkg/errors"
)

var (
	testThreshold = 1 * MB
	errNoTruncate  = errors.New("not have to truncate")
)


type MockBlockReader struct {
	exs             map[uint64]*extent.Extent
	utils.SafeMutex //protect exs
}


func (br *MockBlockReader) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32) ([]*pb.Block, uint32, error) {
	br.RLock()
	ex := br.exs[extentID]
	br.RUnlock()
	
	if ex == nil {
		fmt.Printf("extentID is %d", extentID)
		return nil, 0, errors.New("extentID not good")
	}

	blocks, _, end, err := ex.ReadBlocks(offset, numOfBlocks, (32 << 20))
	if err == wire_errors.EndOfExtent || err == wire_errors.EndOfStream {
		return blocks, end, io.EOF
	}
	if err != nil {
		return nil, 0, err
	}
	return blocks, end, err
}

type MockStreamClient struct {
	stream          []uint64
	names           []string
	ID              uint64
	suffix          string
	br              *MockBlockReader
}


var (
	fileNamePrefix = "mockextent"
)

func fileName(id uint64, suffix string) string {
	return fmt.Sprintf("%s_%d.%s", fileNamePrefix, id, suffix)
}


func NewMockBlockReader() *MockBlockReader{
	return &MockBlockReader{
		exs: make(map[uint64]*extent.Extent),
	}
}

func NewMockStreamClient(suffix string, br *MockBlockReader) StreamClient {
	sID := uint64(rand.Uint32())
	name := fileName(sID, suffix)


	ex, err := extent.CreateExtent(name, sID)

	br.exs[ex.ID] = ex
	utils.Check(err)
	return &MockStreamClient{
		ID:     sID,
		suffix: suffix,
		br : br,
		stream: []uint64{ex.ID},
	}
}

//only open log file
func OpenMockStreamClient(si pb.StreamInfo, br *MockBlockReader) StreamClient {
	sID := si.StreamID
	for _, eID := range si.ExtentIDs {
		name := fileName(eID, "log")
		ex, err := extent.OpenExtent(name)
		utils.Check(err)
		br.exs[ex.ID] = ex
	}
	
	return &MockStreamClient{
		br: br,
		ID:     sID,
		suffix: "log",
	}
}

func (client *MockStreamClient) Truncate(ctx context.Context, extentID uint64, gabageKey string) (*pb.GabageStreams , error) {

	var i int
	for i = range client.stream {
		if client.stream[i] == extentID {
			break
		}
	}
	if i == len(client.stream) {
		return nil, errNoTruncate
	}
/*
	f := func(id uint64, start int, end int) pb.StreamInfo {
		var array []uint64
		for i := start; i < end; i++ {
			array = append(array, client.stream[i])
		}
		return pb.StreamInfo{
			StreamID:  id,
			ExtentIDs: array,
		}
	}
*/
	//update smclient...
	/*
	blobStreamID := rand.Uint64()
	blobStreamInfo := f(blobStreamID, 0, i)

	myStreamInfo := f(client.ID, i, len(client.stream))
	client.stream = client.stream[i:]//升级自己
	*/
	//生成新的blobstream,
	return nil, nil
}

//block API, entries has been batched
func (client *MockStreamClient) AppendEntries(ctx context.Context, entries []*pb.EntryInfo) (uint64, uint32, error) {
	blocks := make([]*pb.Block, 0, len(entries))

	for _, entry := range entries {
		data := utils.MustMarshal(entry.Log)
		blocks = append(blocks, &pb.Block{
			data,
		})
	}
	extentID, offsets, tail, err := client.Append(ctx, blocks)
	for i := range entries {
		entries[i].ExtentID = extentID
		entries[i].Offset = offsets[i]
	}
	//fmt.Printf("append return extentID %d\n", extentID)
	return extentID, tail, err
}

//block API
func (client *MockStreamClient) Append(ctx context.Context, blocks []*pb.Block) (uint64, []uint32, uint32, error) {
	exIndex := len(client.stream) - 1
	exID := client.stream[exIndex]
	client.br.Lock()
	ex := client.br.exs[exID]
	client.br.Unlock()

	ex.Lock()
	defer ex.Unlock()

	offsets, end, err := ex.AppendBlocks(blocks, true)

	if ex.CommitLength() > uint32(testThreshold) {
		//seal
		ex.Seal(ex.CommitLength())
		//create new
		eID := uint64(rand.Uint32())
		name := fileName(eID, client.suffix)
		newEx, err := extent.CreateExtent(name, eID)
		utils.Check(err)

		client.br.Lock()
		client.br.exs[newEx.ID] = newEx
		client.br.Unlock()

		client.stream = append(client.stream, newEx.ID)
	}
	return uint64(exID), offsets, end, err
}

func (client *MockStreamClient) Close() {
	for _, exID := range client.stream {
		name := fileName(exID, client.suffix)
		client.br.Lock()
		ex := client.br.exs[exID]
		ex.Close()
		delete(client.br.exs, exID)
		client.br.Unlock()
		os.Remove(name)
	}
}

func (client *MockStreamClient) Connect() error {
	return nil
}


func (client *MockStreamClient) NewLogEntryIter(opts ...ReadOption) LogEntryIter {

	readOpt := &readOption{}
	for _, opt := range opts {
		opt(readOpt)
	}
	x := &MockLockEntryIter{
		sc:  client,
		opt: readOpt,
	}

	if readOpt.ReadFromStart {
		x.currentOffset = 0
		x.currentIndex = 0
	} else {
		x.currentOffset = readOpt.Offset

		for i := range client.stream {
			if client.stream[i]== readOpt.ExtentID {
				x.currentIndex = i
			}
		}

	}
	if readOpt.Replay {
		x.replay = true
	}

	return x
}

type MockLockEntryIter struct {
	sc            *MockStreamClient
	currentOffset uint32
	currentIndex  int
	opt           *readOption
	noMore        bool
	cache         []*pb.EntryInfo
	replay        bool
}

func (iter *MockLockEntryIter) CheckCommitLength() error {
	return nil
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

	exID := iter.sc.stream[iter.currentIndex]
	iter.sc.br.RLock()
	ex := iter.sc.br.exs[exID]
	iter.sc.br.RUnlock()

	res, tail, err := ex.ReadEntries(iter.currentOffset, 16*KB, iter.replay)

	if len(res) > 0 {
		iter.cache = nil
		iter.cache = append(iter.cache, res...)
	}
	switch err {
	case nil:
		iter.currentOffset = tail
		return nil
	case wire_errors.EndOfExtent:
		iter.currentOffset = 0
		iter.currentIndex++
		if iter.currentIndex == len(iter.sc.stream) {
			iter.noMore = true
		}
		return nil
	case wire_errors.EndOfStream:
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

type MockEtcd struct {
	Tables []*pspb.Location
}

func (c *MockEtcd) SetRowStreamTables(id uint64, tables []*pspb.Location) error {
	c.Tables = tables
	return nil
}
