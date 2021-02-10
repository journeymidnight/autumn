package streamclient

import (
	"sort"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/rangepartition/y"
	"github.com/journeymidnight/autumn/utils"
)

type mixedBlock struct {
	offsets *pspb.MixedLog
	data    []byte
	tail    int
}

func NewMixedBlock() *mixedBlock {
	return &mixedBlock{
		offsets: new(pspb.MixedLog),
		data:    make([]byte, MaxMixedBlockSize, MaxMixedBlockSize),
		tail:    0,
	}
}

func (mb *mixedBlock) CanFill(entry *pb.Entry) bool {
	if mb.tail+entry.Size() > MaxMixedBlockSize || len(mb.offsets.Offsets) >= MaxEntriesInBlock {
		return false
	}
	return true
}

func (mb *mixedBlock) Fill(entry *pb.Entry) uint32 {
	mb.offsets.Offsets = append(mb.offsets.Offsets, uint32(mb.tail))
	entry.MarshalTo(mb.data[mb.tail:])
	offset := mb.tail
	mb.tail += entry.Size()
	return uint32(offset)
}

func (mb *mixedBlock) ToBlock() *pb.Block {
	mb.offsets.Offsets = append(mb.offsets.Offsets, uint32(mb.tail))
	//len(mb.offsets) == actualSizeOfLog + 1
	userData, err := mb.offsets.Marshal()
	utils.Check(err)
	block := &pb.Block{
		BlockLength: uint32(MaxMixedBlockSize),
		UserData:    userData,
		CheckSum:    utils.AdlerCheckSum(mb.data),
		Data:        mb.data,
	}
	return block
}

func ShouldWriteValueToLSM(entry *pb.Entry) bool {
	return entry.Meta&uint32(y.BitValuePointer) == 0 && len(entry.Value) <= 4*KB
}

//sort,merge into blocks
func entriesToBlocks(entries []*pb.EntryInfo) ([]*pb.Block, int, int) {

	utils.AssertTrue(len(entries) != 0)

	//sort
	sort.Slice(entries, func(i, j int) bool {
		return len(entries[i].Log.Value) < len(entries[j].Log.Value)
	})

	var blocks []*pb.Block
	var mblock *mixedBlock = nil
	i := 0

	//merge small reqs into block
	for ; i < len(entries); i++ {
		if !y.ShouldWriteValueToLSM(entries[i].Log) {
			break
		}
		if mblock == nil {
			mblock = NewMixedBlock()
		}
		if !mblock.CanFill(entries[i].Log) {
			blocks = append(blocks, mblock.ToBlock())
			mblock = NewMixedBlock()
		}
		mblock.Fill(entries[i].Log)
	}

	if mblock != nil {
		blocks = append(blocks, mblock.ToBlock())
	}

	j := i           //j is start of Value Block
	k := len(blocks) //k is start of Value block

	for ; i < len(entries); i++ {
		blockLength := utils.Ceil(uint32(entries[i].Log.Size()), 512)
		data := make([]byte, blockLength)
		entries[i].Log.MarshalTo(data)
		var mix pspb.MixedLog
		mix.Offsets = []uint32{0, uint32(entries[i].Log.Size())}
		blockUserData, err := mix.Marshal()
		utils.Check(err)

		blocks = append(blocks, &pb.Block{
			BlockLength: blockLength,
			UserData:    blockUserData,
			Data:        data,
			CheckSum:    utils.AdlerCheckSum(data),
		})

	}
	return blocks, j, k
}
