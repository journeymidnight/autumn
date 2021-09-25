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


func (br *MockBlockReader) Read(ctx context.Context, extentID uint64, offset uint32, numOfBlocks uint32, hint byte) ([]*pb.Block, uint32, error) {
	br.RLock()
	ex := br.exs[extentID]
	br.RUnlock()
	
	if ex == nil {
		fmt.Printf("extentID is %d", extentID)
		return nil, 0, errors.New("extentID not good")
	}

	blocks, _, end, err := ex.ReadBlocks(offset, numOfBlocks, (32 << 20))
	if err == wire_errors.EndOfExtent {
		return blocks, end, io.EOF
	}
	if err != nil {
		return nil, 0, err
	}
	return blocks, end, err
}

type MockStreamClient struct {
	StreamClient
	stream          []uint64
	names           []string
	ID              uint64
	suffix          string
	br              *MockBlockReader
	utils.SafeMutex
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



func (client *MockStreamClient) CommitEnd() uint32 {
	exIndex := len(client.stream) - 1
	exID := client.stream[exIndex]
	client.br.Lock()
	ex := client.br.exs[exID]
	client.br.Unlock()
	return ex.CommitLength()
	
}

func (client *MockStreamClient) CheckCommitLength() error {
	return nil
}


func (client *MockStreamClient) Truncate(ctx context.Context, extentID uint64) (error) {

	client.Lock()
	defer client.Unlock()

	var i int
	for i = range client.stream {
		if client.stream[i] == extentID {
			break
		}
	}
	if i == len(client.stream) {
		return nil
	}

	//exclude i
	client.stream = client.stream[:i]
	
	return nil
}

//block API, entries has been batched
func (client *MockStreamClient) AppendEntries(ctx context.Context, entries []*pb.EntryInfo, mustSync bool) (uint64, uint32, error) {
	blocks := make([]*pb.Block, 0, len(entries))

	for _, entry := range entries {
		data := utils.MustMarshal(entry.Log)
		blocks = append(blocks, &pb.Block{
			data,
		})
	}
	extentID, offsets, tail, err := client.Append(ctx, blocks, mustSync)
	for i := range entries {
		entries[i].ExtentID = extentID
		entries[i].Offset = offsets[i]
	}
	//fmt.Printf("append return extentID %d\n", extentID)
	return extentID, tail, err
}

//block API
func (client *MockStreamClient) Append(ctx context.Context, blocks []*pb.Block, mustSync bool) (uint64, []uint32, uint32, error) {
	exIndex := len(client.stream) - 1
	exID := client.stream[exIndex]
	client.br.Lock()
	ex := client.br.exs[exID]
	client.br.Unlock()

	ex.Lock()
	defer ex.Unlock()

	offsets, end, err := ex.AppendBlocks(blocks, mustSync)

	if ex.CommitLength() > uint32(testThreshold) {
		client.Lock()
		defer client.Unlock()
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
		//fmt.Printf("delete %s\n", name)

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

func (client *MockStreamClient) StreamInfo() *pb.StreamInfo {
	client.Lock()
	defer client.Unlock()
	tmp := make([]uint64, len(client.stream))
	copy(tmp, client.stream)
	return &pb.StreamInfo{
		StreamID: client.ID,
		ExtentIDs: tmp,
	}
}

func (client *MockStreamClient) ReadLastBlock(ctx context.Context) (*pb.Block, error) {
	exIndex := len(client.stream) - 1
	exID := client.stream[exIndex]
	client.br.Lock()
	ex := client.br.exs[exID]
	client.br.Unlock()

	ex.Lock()
	defer ex.Unlock()

	blocks, _, _, err := ex.ReadLastBlock()
	if err != nil {
		return nil, err
	}
	return blocks[0], nil
}

func (client *MockStreamClient) PunchHoles(ctx context.Context, extentIDs []uint64) error {
	client.Lock()
	defer client.Unlock()

	if len(client.stream) == 0 {
		return nil
	}
	//build index for extentIDs
	index := make(map[uint64]bool)
	lastEx := client.stream[len(client.stream)-1]
	for _, exID := range extentIDs {
		if exID != lastEx {
			index[exID] = true
		}
	}

	//remove extentIDs from stream
	for i := len(client.stream) - 1; i >= 0; i-- {
		if _, ok := index[client.stream[i]]; ok {
			//exluce this extent and delete file
			name := fileName(client.stream[i], client.suffix)
			//fmt.Printf("delete hole %s\n", name)
			os.Remove(name)
			client.stream = append(client.stream[:i], client.stream[i+1:]...)

		}
	}

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
	n                  int//number of extents we have read

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
		iter.n ++
		if iter.currentIndex == len(iter.sc.stream) || iter.n >= iter.opt.MaxExtentRead {
			iter.noMore = true
		}
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

