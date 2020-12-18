package table

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/ristretto"
	"github.com/dgraph-io/ristretto/z"
	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

// TableInterface is useful for testing.
type TableInterface interface {
	Smallest() []byte
	Biggest() []byte
	DoesNotHave(hash uint64) bool
}

type Table struct {
	sync.Mutex
	stream     *streamclient.StreamClient
	blockIndex []*pspb.BlockOffset
	ref        int32 // For file garbage collection. Atomic.

	// The following are initialized once and const.
	smallest, biggest []byte // Smallest and largest keys (with timestamps).

	// Stores the total size of key-values stored in this table (including the size on vlog).
	estimatedSize uint64
	bf            *z.Bloom
	Cache         *ristretto.Cache
	BfCache       *ristretto.Cache
}

// IncrRef increments the refcount (having to do with whether the file should be deleted)
func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

// DecrRef decrements the refcount and possibly deletes the table
func (t *Table) DecrRef() error {
	return nil
}

func OpenTable(stream *streamclient.StreamClient,
	extentID uint64, offset uint32) (*Table, error) {

	utils.AssertTrue(xlog.Logger != nil)

	blocks, err := stream.Read(context.Background(), extentID, offset, 1)
	if err != nil {
		return nil, err
	}
	if len(blocks) != 1 {
		return nil, errors.Errorf("len of block is not 1")
	}
	var metaBlock pspb.RawBlockMeta
	//must?
	utils.MustUnMarshal(blocks[0].UserData, &metaBlock)
	if metaBlock.Type != pspb.RawBlockType_meta {
		return nil, errors.Errorf("block type error")
	}

	var tableIndex pspb.TableIndex
	if err = tableIndex.Unmarshal(blocks[0].Data[:metaBlock.UnCompressedSize]); err != nil {
		return nil, err
	}

	t := &Table{
		blockIndex:    make([]*pspb.BlockOffset, len(tableIndex.Offsets)),
		stream:        stream,
		estimatedSize: tableIndex.EstimatedSize,
	}

	//read bloom filter
	if t.bf, err = z.JSONUnmarshal(tableIndex.BloomFilter); err != nil {
		return nil, err
	}

	//clone BlockOffset
	for i, offset := range tableIndex.Offsets {
		t.blockIndex[i] = proto.Clone(offset).(*pspb.BlockOffset)
	}

	//get range of table
	if err = t.initBiggestAndSmallest(); err != nil {
		return nil, err
	}
	return t, nil
}

//TODO: cache block
func (t *Table) block(idx int) (*pb.Block, error) {
	extentID := t.blockIndex[idx].ExtentID
	offset := t.blockIndex[idx].Offset
	blocks, err := t.stream.Read(context.Background(), extentID, offset, 1)
	if err != nil {
		return nil, err
	}
	if len(blocks) != 1 {
		return nil, errors.Errorf("len of blocks is not 1")
	}
	return blocks[0], nil
}

// Smallest is its smallest key, or nil if there are none
func (t *Table) Smallest() []byte { return t.smallest }

// Biggest is its biggest key, or nil if there are none
func (t *Table) Biggest() []byte { return t.biggest }

func (t *Table) initBiggestAndSmallest() error {
	t.smallest = t.blockIndex[0].Key

	it2 := t.NewIterator(true)
	defer it2.Close()
	it2.Rewind()
	if !it2.Valid() {
		return errors.Wrapf(it2.err, "failed to initialize biggest for table")
	}
	t.biggest = it2.Key()
	return nil
}

// NewIterator returns a new iterator of the Table
func (t *Table) NewIterator(reversed bool) *Iterator {
	t.IncrRef() // Important.
	ti := &Iterator{t: t, reversed: reversed}
	ti.next()
	return ti
}

func (t *Table) DoesNotHave(hash uint64) bool {
	return !t.bf.Has(hash)
}
