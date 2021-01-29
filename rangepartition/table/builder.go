/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package table

import (
	"context"
	"math"
	"unsafe"

	"github.com/dgraph-io/ristretto/z"
	"github.com/dgryski/go-farm"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/rangepartition/y"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

const (
	KB = 1024
	MB = KB * 1024

	// When a block is encrypted, it's length increases. We add 200 bytes of padding to
	// handle cases when block size increases. This is an approximate number.
	padding = 200
)

type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// Encode encodes the header.
func (h header) Encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// Decode decodes the header.
func (h *header) Decode(buf []byte) {
	// Copy over data from buf into h. Using *h=unsafe.pointer(...) leads to
	// pointer alignment issues. See https://github.com/dgraph-io/badger/issues/1096
	// and comment https://github.com/dgraph-io/badger/pull/1097#pullrequestreview-307361714
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

// Builder is used in building a table.
type Builder struct {
	// Typically tens or hundreds of meg. This is for one single file.
	blocks       []*pb.Block //64KB per block
	currentBlock *pb.Block
	sz           uint32

	baseKey []byte // Base key for the current block.
	//baseOffset   uint32   // Offset for the current block.
	entryOffsets []uint32 // Offsets of entries present in current block.
	tableIndex   *pspb.TableIndex
	keyHashes    []uint64 // Used for building the bloomfilter.
	stream       streamclient.StreamClient
	writeCh      chan writeBlock
	stopper      *utils.Stopper
}

// NewTableBuilder makes a new TableBuilder.
func NewTableBuilder(stream streamclient.StreamClient) *Builder {
	b := &Builder{
		tableIndex: &pspb.TableIndex{},
		keyHashes:  make([]uint64, 0, 1024), // Avoid some malloc calls.
		stream:     stream,
		writeCh:    make(chan writeBlock, 16),
		stopper:    utils.NewStopper(),
	}

	b.stopper.RunWorker(func() {
		var blocks []*pb.Block
		var size uint32
		var baseKeys [][]byte
		for {
			select {
			case wBlock, ok := <-b.writeCh:
				if !ok {
					return
				}
			slurpLoop:
				for {
					blocks = append(blocks, wBlock.b)
					size += wBlock.b.BlockLength
					baseKeys = append(baseKeys, wBlock.baseKey)
					if size > 10*MB {
						break slurpLoop
					}

					//if channel is closed or no new blocks coming, break
					select {
					case wBlock, ok = <-b.writeCh:
						if !ok {
							break slurpLoop
						}
					default:
						break slurpLoop
					}
				}

				if len(blocks) == 0 {
					return
				}

				extentID, offsets, err := b.stream.Append(context.Background(), blocks)
				utils.Check(err)

				for i, offset := range offsets {
					b.addBlockToIndex(baseKeys[i], extentID, offset)
				}
				blocks = nil
				size = 0
				baseKeys = nil
			}
		}
	})

	return b
}

// Close closes the TableBuilder.
func (b *Builder) Close() {}

// Empty returns whether it's empty.
func (b *Builder) Empty() bool { return b.sz == 0 }

func (b *Builder) getBlocks() []*pb.Block {
	return b.blocks
}

// keyDiff returns a suffix of newKey that is different from b.baseKey.
func (b *Builder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(b.baseKey); i++ {
		if newKey[i] != b.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

type blockWriteReq struct {
	b pb.Block
}

func blockGrow(block *pb.Block, n uint32) {
	newSize := utils.Ceil(block.BlockLength+n, 512)
	newBuf := make([]byte, newSize)
	copy(newBuf, block.Data)
	block.BlockLength = newSize
	block.Data = newBuf
}

//append data to current block
func (b *Builder) append(data []byte) {
	if b.currentBlock == nil {
		b.currentBlock = &pb.Block{
			Data:        make([]byte, 64*KB),
			BlockLength: 64 * KB,
			//BlockLength: uint32(size),
		}
		b.blocks = append(b.blocks, b.currentBlock)
	}
	// Ensure we have enough space to store new data.
	if b.currentBlock.BlockLength < b.sz+uint32(len(data)) {
		blockGrow(b.currentBlock, uint32(len(data)))
	}
	copy(b.currentBlock.Data[b.sz:], data)
	b.sz += uint32(len(data))
}

func (b *Builder) addHelper(key []byte, v y.ValueStruct) {
	b.keyHashes = append(b.keyHashes, farm.Fingerprint64(y.ParseKey(key)))

	// diffKey stores the difference of key with baseKey.
	var diffKey []byte
	if len(b.baseKey) == 0 {
		// Make a copy. Builder should not keep references. Otherwise, caller has to be very careful
		// and will have to make copies of keys every time they add to builder, which is even worse.
		b.baseKey = append(b.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = b.keyDiff(key)
	}

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	// store current entry's offset
	utils.AssertTrue(b.sz < math.MaxUint32)
	b.entryOffsets = append(b.entryOffsets, b.sz)

	// Layout: header, diffKey, value.
	b.append(h.Encode())
	b.append(diffKey)

	if b.currentBlock.BlockLength < b.sz+v.EncodedSize() {
		blockGrow(b.currentBlock, v.EncodedSize())
	}
	b.sz += v.Encode(b.currentBlock.Data[b.sz:])

	// Size of KV on SST.
	sstSz := uint64(uint32(headerSize) + uint32(len(diffKey)) + v.EncodedSize())
	b.tableIndex.EstimatedSize += sstSz
}

type writeBlock struct {
	b       *pb.Block
	baseKey []byte
}

/*
Structure of Block.
+-------------------+---------------------+--------------------+--------------+------------------+
| Entry1            | Entry2              | Entry3             | Entry4       | Entry5           |
+-------------------+---------------------+--------------------+--------------+------------------+
| Entry6            | ...                 | ...                | ...          | EntryN           |
+-------------------+---------------------+--------------------+--------------+------------------+
| Block Meta(contains list of offsets used| Block Meta Size    |              |                  |
| to perform binary search in the block)  | (4 Bytes)          |              |                  |
+-----------------------------------------+--------------------+--------------+------------------+
*/
// In case the data is encrypted, the "IV" is added to the end of the block.
func (b *Builder) FinishBlock() {
	b.append(y.U32SliceToBytes(b.entryOffsets))
	b.append(y.U32ToBytes(uint32(len(b.entryOffsets))))

	b.currentBlock.CheckSum = utils.AdlerCheckSum(b.currentBlock.Data)
	b.currentBlock.UserData = utils.MustMarshal(&pspb.RawBlockMeta{
		Type:             pspb.RawBlockType_data,
		CompressedSize:   0,
		UnCompressedSize: b.sz,
	})

	xlog.Logger.Debugf("real block size is %d, len of entries is %d\n", b.sz, len(b.entryOffsets))
	b.writeCh <- writeBlock{
		baseKey: y.Copy(b.baseKey),
		b:       b.currentBlock,
	}
	return
}

func (b *Builder) addBlockToIndex(baseKey []byte, extentID uint64, offset uint32) {
	// Add key to the block index.
	bo := &pspb.BlockOffset{
		Key:      baseKey,
		ExtentID: extentID,
		Offset:   offset,
	}
	b.tableIndex.Offsets = append(b.tableIndex.Offsets, bo)
}

func (b *Builder) shouldFinishBlock(key []byte, value y.ValueStruct) bool {
	// If there is no entry till now, we will return false.
	if len(b.entryOffsets) <= 0 {
		return false
	}
	// We should include current entry also in size, that's why + 4 to len(b.entryOffsets).
	entriesOffsetsSize := uint32(len(b.entryOffsets)*4 + 4) //size of list
	estimatedSize := uint32(b.sz) + uint32(headerSize) +
		uint32(len(key)) + uint32(value.EncodedSize()) + entriesOffsetsSize
	return estimatedSize > 64*KB || len(b.entryOffsets) > 1000
}

// Add adds a key-value pair to the block.
func (b *Builder) Add(key []byte, value y.ValueStruct) {
	if b.shouldFinishBlock(key, value) {

		b.FinishBlock()
		// Start a new block. Initialize the block.
		b.baseKey = []byte{}
		b.currentBlock = nil
		b.sz = 0
		utils.AssertTrue(uint32(b.sz) <= math.MaxUint32)
		b.entryOffsets = b.entryOffsets[:0]
	}
	b.addHelper(key, value)
}

// Finish finishes the table by appending the index.
/*
The table structure looks like
+---------+------------+-----------+---------------+
| Block 1 | Block 2    | Block 3   | Block 4       |
+---------+------------+-----------+---------------+
| Block 5 | Block 6    | Block ... | Block N       |
+---------+------------+-----------+---------------+
| MetaBlock |
+---------+------------+-----------+---------------+
*/
//return metablock position(extentID, offset, error)
//tailExtentID和tailOffset表示当前commitLog对应的结尾, 在打开commitlog后, 从(tailExtentID, tailOffset)开始的
//block读数据, 生成mt
func (b *Builder) FinishAll(headExtentID uint64, headOffset uint32, seqNum uint64) (uint64, uint32, error) {

	close(b.writeCh)
	b.stopper.Wait()

	bf := z.NewBloomFilter(float64(len(b.keyHashes)), 0.01)
	for _, h := range b.keyHashes {
		bf.Add(h)
	}
	// Add bloom filter to the index.
	b.tableIndex.BloomFilter = bf.JSONMarshal()

	//alloc a new meta block

	sz := utils.Ceil(uint32(b.tableIndex.Size()), 4*KB)

	metaBlock := &pb.Block{
		BlockLength: sz,
		Data:        make([]byte, sz, sz),
	}

	b.tableIndex.MarshalTo(metaBlock.Data)

	metaBlock.UserData = utils.MustMarshal(&pspb.RawBlockMeta{
		Type:             pspb.RawBlockType_meta,
		UnCompressedSize: uint32(b.tableIndex.Size()),
		CompressedSize:   0,
		VpExtentID:       headExtentID,
		VpOffset:         headOffset,
		SeqNum:           seqNum,
	})
	metaBlock.CheckSum = utils.AdlerCheckSum(metaBlock.Data)

	extentID, offsets, err := b.stream.Append(context.Background(), []*pb.Block{metaBlock})
	if err != nil {
		return 0, 0, err
	}
	return extentID, offsets[0], nil
}
