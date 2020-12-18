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
	"bytes"
	"io"
	"sort"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
)

/*
type block struct {
	extentID          uint64
	offset            uint32
	data              []byte //uncompressed size
	entriesIndexStart int    //start index of entryOffsets list
	entryOffsets      []uint32
}
*/

type blockIterator struct {
	data         []byte
	idx          int // Idx of the entry inside a block
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32

	// prevOverlap stores the overlap of the previous key with the base key.
	// This avoids unnecessary copy of base key when the overlap is same for multiple keys.
	prevOverlap uint16
}

func (itr *blockIterator) setBlock(b *pb.Block) {
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]

	var blockMeta pspb.RawBlockMeta
	utils.MustUnMarshal(b.UserData, &blockMeta)
	sz := blockMeta.UnCompressedSize
	numEntries := y.BytesToU32(b.Data[sz-4 : sz])
	entriesIndexStart := sz - 4 - numEntries*4
	itr.entryOffsets = y.BytesToU32Slice(b.Data[entriesIndexStart : sz-4])
	itr.data = b.Data[:entriesIndexStart]
}

// setIdx sets the iterator to the entry at index i and set it's key and value.
func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	startOffset := int(itr.entryOffsets[i])

	// Set base key.
	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.Decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	// idx points to the last entry in the block.
	if itr.idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
		// idx point to some entry other than the last one in the block.
		// EndOffset of the current entry is the start offset of the next entry.
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}

	entryData := itr.data[startOffset:endOffset]
	var h header
	h.Decode(entryData)
	// Header contains the length of key overlap and difference compared to the base key. If the key
	// before this one had the same or better key overlap, we can avoid copying that part into
	// itr.key. But, if the overlap was lesser, we could copy over just that portion.
	if h.overlap > itr.prevOverlap {
		itr.key = append(itr.key[:itr.prevOverlap], itr.baseKey[itr.prevOverlap:h.overlap]...)
	}
	itr.prevOverlap = h.overlap
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	itr.key = append(itr.key[:h.overlap], diffKey...)
	itr.val = entryData[valueOff:]
}

func (itr *blockIterator) Valid() bool {
	return itr != nil && itr.err == nil
}

func (itr *blockIterator) Error() error {
	return itr.err
}

func (itr *blockIterator) Close() {}

var (
	origin  = 0
	current = 1
)

// seek brings us to the first block element that is >= input key.
func (itr *blockIterator) seek(key []byte, whence int) {
	itr.err = nil
	startIndex := 0 // This tells from which index we should start binary search.

	switch whence {
	case origin:
		// We don't need to do anything. startIndex is already at 0
	case current:
		startIndex = itr.idx
	}

	foundEntryIdx := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		// If idx is less than start index then just return false.
		if idx < startIndex {
			return false
		}
		itr.setIdx(idx)
		return y.CompareKeys(itr.key, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}

// seekToFirst brings us to the first element.
func (itr *blockIterator) seekToFirst() {
	itr.setIdx(0)
}

// seekToLast brings us to the last element.
func (itr *blockIterator) seekToLast() {
	itr.setIdx(len(itr.entryOffsets) - 1)
}

func (itr *blockIterator) next() {
	itr.setIdx(itr.idx + 1)
}

func (itr *blockIterator) prev() {
	itr.setIdx(itr.idx - 1)
}

// Iterator is an iterator for a Table.
type Iterator struct {
	t    *Table //
	bpos int
	bi   blockIterator
	err  error

	// Internally, Iterator is bidirectional. However, we only expose the
	// unidirectional functionality for now.
	reversed bool
}

// Close closes the iterator (and it must be called).
func (itr *Iterator) Close() error {
	return itr.t.DecrRef()
}

func (itr *Iterator) reset() {
	itr.bpos = 0
	itr.err = nil
}

// Valid follows the y.Iterator interface
func (itr *Iterator) Valid() bool {
	return itr.err == nil
}

func (itr *Iterator) seekToFirst() {
	numBlocks := len(itr.t.blockIndex)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bpos = 0
	block, err := itr.t.block(itr.bpos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seekToFirst()
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekToLast() {
	numBlocks := len(itr.t.blockIndex)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bpos = numBlocks - 1
	block, err := itr.t.block(itr.bpos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seekToLast()
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekHelper(blockIdx int, key []byte) {
	itr.bpos = blockIdx
	block, err := itr.t.block(blockIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seek(key, origin)
	itr.err = itr.bi.Error()
}

// seekFrom brings us to a key that is >= input key.
func (itr *Iterator) seekFrom(key []byte, whence int) {
	itr.err = nil
	switch whence {
	case origin:
		itr.reset()
	case current:
	}

	idx := sort.Search(len(itr.t.blockIndex), func(idx int) bool {
		ko := itr.t.blockIndex[idx]
		return y.CompareKeys(ko.Key, key) > 0
	})
	if idx == 0 {
		// The smallest key in our table is already strictly > key. We can return that.
		// This is like a SeekToFirst.
		itr.seekHelper(0, key)
		return
	}

	// block[idx].smallest is > key.
	// Since idx>0, we know block[idx-1].smallest is <= key.
	// There are two cases.
	// 1) Everything in block[idx-1] is strictly < key. In this case, we should go to the first
	//    element of block[idx].
	// 2) Some element in block[idx-1] is >= key. We should go to that element.
	itr.seekHelper(idx-1, key)
	if itr.err == io.EOF {
		// Case 1. Need to visit block[idx].
		if idx == len(itr.t.blockIndex) {
			// If idx == len(itr.t.blockIndex), then input key is greater than ANY element of table.
			// There's nothing we can do. Valid() should return false as we seek to end of table.
			return
		}
		// Since block[idx].smallest is > key. This is essentially a block[idx].SeekToFirst.
		itr.seekHelper(idx, key)
	}
	// Case 2: No need to do anything. We already did the seek in block[idx-1].
}

// seek will reset iterator and seek to >= key.
func (itr *Iterator) seek(key []byte) {
	itr.seekFrom(key, origin)
}

// seekForPrev will reset iterator and seek to <= key.
func (itr *Iterator) seekForPrev(key []byte) {
	// TODO: Optimize this. We shouldn't have to take a Prev step.
	itr.seekFrom(key, origin)
	if !bytes.Equal(itr.Key(), key) {
		itr.prev()
	}
}

func (itr *Iterator) next() {
	itr.err = nil

	if itr.bpos >= len(itr.t.blockIndex) {
		itr.err = io.EOF
		return
	}

	if len(itr.bi.data) == 0 {
		block, err := itr.t.block(itr.bpos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi.setBlock(block)
		itr.bi.seekToFirst()
		itr.err = itr.bi.Error()
		return
	}

	itr.bi.next()
	if !itr.bi.Valid() {
		itr.bpos++
		itr.bi.data = nil
		itr.next()
		return
	}
}

func (itr *Iterator) prev() {
	itr.err = nil
	if itr.bpos < 0 {
		itr.err = io.EOF
		return
	}

	if len(itr.bi.data) == 0 {
		block, err := itr.t.block(itr.bpos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi.setBlock(block)
		itr.bi.seekToLast()
		itr.err = itr.bi.Error()
		return
	}

	itr.bi.prev()
	if !itr.bi.Valid() {
		itr.bpos--
		itr.bi.data = nil
		itr.prev()
		return
	}
}

// Key follows the y.Iterator interface.
// Returns the key with timestamp.
func (itr *Iterator) Key() []byte {
	return itr.bi.key
}

// Value follows the y.Iterator interface
func (itr *Iterator) Value() (ret y.ValueStruct) {
	ret.Decode(itr.bi.val)
	return
}

// ValueCopy copies the current value and returns it as decoded
// ValueStruct.
func (itr *Iterator) ValueCopy() (ret y.ValueStruct) {
	dst := y.Copy(itr.bi.val)
	ret.Decode(dst)
	return
}

// Next follows the y.Iterator interface
func (itr *Iterator) Next() {
	if !itr.reversed {
		itr.next()
	} else {
		itr.prev()
	}
}

// Rewind follows the y.Iterator interface
func (itr *Iterator) Rewind() {
	if !itr.reversed {
		itr.seekToFirst()
	} else {
		itr.seekToLast()
	}
}

// Seek follows the y.Iterator interface
func (itr *Iterator) Seek(key []byte) {
	if !itr.reversed {
		itr.seek(key)
	} else {
		itr.seekForPrev(key)
	}
}
