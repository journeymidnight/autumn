package table

import (
	"context"
	"sync/atomic"

	"github.com/dgraph-io/ristretto"
	"github.com/dgraph-io/ristretto/z"
	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/range_partition/y"
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
	utils.SafeMutex
	stream     streamclient.BlockReader
	blockIndex []*pspb.BlockOffset
	ref        int32 // For file garbage collection. Atomic.

	// The following are initialized once and const.
	smallest, biggest []byte // Smallest and largest keys (with timestamps).

	// Stores the total size of key-values stored in this table (including the size on vlog).
	estimatedSize uint64
	bf            *z.Bloom
	Cache         *ristretto.Cache
	BfCache       *ristretto.Cache

	Loc        pspb.Location
	LastSeq    uint64
	VpExtentID uint64
	VpOffset   uint32
}

// IncrRef increments the refcount (having to do with whether the file should be deleted)
func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

// DecrRef decrements the refcount and possibly deletes the table
func (t *Table) DecrRef() error {
	ref := atomic.AddInt32(&t.ref, ^int32(0))
	if ref == 0 {
		//TODO: remove table
	}
	return nil
}

func OpenTable(stream streamclient.StreamClient,
	extentID uint64, offset uint32) (*Table, error) {

	utils.AssertTrue(xlog.Logger != nil)

	blocks, _, err := stream.Read(context.Background(), extentID, offset, 1)
	if err != nil {
		return nil, err
	}
	if len(blocks) != 1 {
		return nil, errors.Errorf("len of block is not 1")
	}
	data := blocks[0].Data

	if len(data) <= 4 {
		return nil, errors.Errorf("meta block should be bigger than 4")
	}

	//must?
	//read checksum
	expected := y.BytesToU32(data[len(data)-4:])

	checksum := utils.NewCRC(data[:len(data)-4]).Value()
	if checksum != expected {
		return nil, errors.Errorf("expected crc is %d, but computed from data is %d", expected, checksum)
	}

	var meta pspb.BlockMeta

	err = meta.Unmarshal(data[:len(data)-4])
	if err != nil {
		return nil, err
	}

	t := &Table{
		blockIndex:    make([]*pspb.BlockOffset, len(meta.TableIndex.Offsets)),
		stream:        stream,
		estimatedSize: meta.TableIndex.EstimatedSize,
		Loc: pspb.Location{
			ExtentID: extentID,
			Offset:   offset,
		},
		LastSeq:    meta.SeqNum,
		VpExtentID: meta.VpExtentID,
		VpOffset:   meta.VpOffset,
		ref:        1,
	}

	//read bloom filter
	if t.bf, err = z.JSONUnmarshal(meta.TableIndex.BloomFilter); err != nil {
		return nil, err
	}

	//clone BlockOffset
	for i, offset := range meta.TableIndex.Offsets {
		t.blockIndex[i] = proto.Clone(offset).(*pspb.BlockOffset)
	}

	//get range of table
	if err = t.initBiggestAndSmallest(); err != nil {
		return nil, err
	}
	return t, nil
}

type entriesBlock struct {
	offset            int
	data              []byte
	checksum          uint32
	entriesIndexStart int      // start index of entryOffsets list
	entryOffsets      []uint32 // used to binary search an entry in the block.
}

//TODO: cache block: FIXME
func (t *Table) block(idx int) (*entriesBlock, error) {
	extentID := t.blockIndex[idx].ExtentID
	offset := t.blockIndex[idx].Offset
	blocks, _, err := t.stream.Read(context.Background(), extentID, offset, 1)
	if err != nil {
		return nil, err
	}
	if len(blocks) != 1 {
		return nil, errors.Errorf("len of blocks is not 1")
	}
	if len(blocks[0].Data) < 8 {
		return nil, errors.Errorf("block data should be bigger than 8")
	}

	data := blocks[0].Data
	expected := y.BytesToU32(data[len(data)-4:])
	checksum := utils.NewCRC(data[:len(data)-4]).Value()
	if checksum != expected {
		return nil, errors.Errorf("expected crc is %d, but computed from data is %d", expected, checksum)
	}

	numEntries := y.BytesToU32(data[len(data)-8:])
	entriesIndexStart := len(data) - 4 - 4 - int(numEntries)*4
	if entriesIndexStart < 0 {
		return nil, errors.Errorf("entriesIndexStart cannot be less than 0")
	}
	entriesIndexEnd := entriesIndexStart + 4*int(numEntries)

	return &entriesBlock{
		offset:            int(offset),
		data:              data[:len(data)-4], //exclude checksum
		entryOffsets:      y.BytesToU32Slice(data[entriesIndexStart:entriesIndexEnd]),
		entriesIndexStart: entriesIndexStart,
		checksum:          checksum,
	}, nil
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
