/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless  by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package extent

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/adler32"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"io"

	"github.com/pkg/errors"

	"github.com/journeymidnight/streamlayer/proto/pb"
	"github.com/journeymidnight/streamlayer/utils"
)

//TODO: block的元数据, 和持久化的index都需要做checksum

//FIXME: put all errors into errors directory
func align(n uint64) bool {
	return n != 0 && n%512 == 0
}

func formatExtentName(id uint64) string {
	//some name
	return fmt.Sprintf("store/extents/%d.ext", id)
}

func formatExtentIndexName(id uint64) string {
	return fmt.Sprintf("store/index/%d.index", id)
}

type Extent struct {
	//sync.Mutex //only one AppendBlocks could be called at a time
	utils.SafeMutex
	isSeal        int32  //atomic
	commitLength  uint32 //atomic
	fileName      string
	indexFileName string
	ID            uint64

	file  *os.File
	index Index
	//FIXME: add SSD Chanel

}

const (
	extentMagicNumber = "EXTENTXX"
)

type extentHeader struct {
	magicNumber [8]byte
	ID          uint64
}

func newExtentHeader(ID uint64) *extentHeader {
	eh := extentHeader{
		ID: ID,
	}
	copy(eh.magicNumber[:], extentMagicNumber)
	return &eh
}

func (eh *extentHeader) Size() uint32 {
	return 16
}

func (eh extentHeader) Marshal(w io.Writer) error {
	var buf [512]byte
	copy(buf[:], eh.magicNumber[:])
	binary.BigEndian.PutUint64(buf[:8], eh.ID)

	n, err := w.Write(buf[:])
	if n != 512 || err != nil {
		return errors.Errorf("failed to create extent file")
	}
	return nil
}

func (eh extentHeader) Unmarshal(r io.Reader) error {
	var buf [512]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return err
	}

	copy(eh.magicNumber[:], buf[:8])
	eh.ID = binary.BigEndian.Uint64(buf[8:16])

	if string(eh.magicNumber[:]) != extentMagicNumber {
		errors.Errorf("extent magic number failed")
	}
	return nil
}

func CreateExtent(fileName string, ID uint64) (*Extent, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	extentHeader := newExtentHeader(ID)
	if err = extentHeader.Marshal(f); err != nil {
		return nil, err
	}
	//write header of Extent
	return &Extent{
		isSeal:       0,
		commitLength: 512,
		fileName:     fileName,
		file:         f,
		index:        NewDynamicIndex(),
	}, nil

}

func OpenExtent(fileName string, indexFileName string) (*Extent, error) {
	//if extent is a sealed extent
	if indexFileName != "" {
		indexFile, err := os.Open(indexFileName)
		if err != nil {
			return nil, err
		}
		index := NewStaticIndex()
		if err = index.Unmarshal(indexFile); err != nil {
			return nil, err
		}

		file, err := os.Open(fileName)
		if err != nil {
			return nil, err
		}
		info, _ := file.Stat()
		if info.Size() > math.MaxUint32 {
			return nil, errors.Errorf("check extent file, the extent file is too big")
		}
		//check extent header

		eh := newExtentHeader(0)
		if err = eh.Unmarshal(file); err != nil {
			return nil, err
		}

		return &Extent{
			isSeal:        1,
			commitLength:  uint32(info.Size()),
			fileName:      fileName,
			indexFileName: indexFileName,
			file:          file,
			index:         index,
			ID:            eh.ID,
		}, nil
	}

	/*
		如果extent是意外关闭
		1. 3副本很可能不一致. 如果有新的写入, 在primary上面是Append, 在secondary上面的API
		是检查Offset的Append, 如果这3个任何一个失败, client就找sm把extent变成:truncate/Sealed.
		2. 由于写入是多个sector,也会有一致性问题:
		   2a. 如果存在SSD journal, 需要从SSD journal恢复成功的extent(因为SSD写入成功后,就已经返回OK了, 需要确保已经返回的数据的原子性)
		   2b. 如果没有SSD的存在,比如要写入4个sector, 但是只写入的2个, 只有metaBlock和一部分block, 需要truncate到之前的版本, 保证
		   原子性
	*/

	//replay the extent file, 这里的replay重新读了所有文件内容, 也许不需要,
	//前面可以只读block的meta, 直到最后一个block再读文件数据, 检查checksum
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	info, _ := f.Stat()
	currentSize := uint32(info.Size())
	index := NewDynamicIndex()

	eh := newExtentHeader(0)
	err = eh.Unmarshal(f)
	if err != nil {
		return nil, err
	}
	offset := uint32(512)
	for offset < currentSize {
		b, err := readBlock(f)
		if err != nil {
			//this block is corrupt, so, truncate extent to current offset
			if err = f.Truncate(int64(offset)); err != nil {
				return nil, err
			}
			if err = f.Sync(); err != nil {
				return nil, err
			}
			break
		}
		//BlockOffset and Offset should be the same
		index.Put(offset, &pb.BlockMeta{
			BlockLength: b.BlockLength,
			BlockOffset: offset,
			Offset:      offset,
		})
		offset += b.BlockLength + 512
	}

	return &Extent{
		isSeal:        0,
		commitLength:  offset,
		fileName:      fileName,
		indexFileName: indexFileName,
		file:          f,
		index:         index,
		ID:            eh.ID,
	}, nil
}

//support multple threads
//limit max read size
type extentBlockReader struct {
	extent   *Extent
	position uint32
	n        uint32 // max bytes remaining
}

func (r *extentBlockReader) Read(p []byte) (n int, err error) {
	if r.n <= 0 {
		return 0, io.EOF
	}
	if uint32(len(p)) > r.n {
		p = p[0:r.n]

	}
	n, err = r.extent.file.ReadAt(p, int64(r.position))
	if err != nil {
		return n, err
	}
	r.position += uint32(n)
	r.n -= uint32(n)

	return n, nil
}

func (ex *Extent) Seal(commit uint32, indexFileName string) error {
	if indexFileName == "" {
		return errors.Errorf("index file is empty")
	}
	ex.Lock()
	defer ex.Unlock()
	atomic.StoreInt32(&ex.isSeal, 1)

	currentLength := ex.commitLength
	if currentLength < commit {
		return errors.Errorf("commit is less than current commit length")
	} else if currentLength > commit {
		ex.file.Truncate(int64(commit))
	}
	//write index file
	//TODO: cleanup index:如果有任何大于commit的entry, 也要删除, 保证数据完整
	ex.indexFileName = indexFileName
	f, err := os.OpenFile(ex.indexFileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if err = ex.index.Marshal(f); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	//TODO : convert dynamic index into static index to save memory when sealing,
	//request a atomic swap to implement this operation
	return nil
}

func (ex *Extent) IsSeal() bool {
	return atomic.LoadInt32(&ex.isSeal) == 1
}
func (ex *Extent) GetReader(offset uint32) (io.Reader, error) {
	meta, ok := ex.index.Get(offset)
	if !ok {
		return nil, errors.Errorf("can not find offset %d", offset)
	}
	return &extentBlockReader{
		extent:   ex,
		position: meta.BlockOffset,
		n:        meta.BlockLength + 512,
	}, nil

}

//Close function is not thread-safe
func (ex *Extent) Close() {
	ex.Lock()
	defer ex.Unlock()
	ex.file.Close()
	ex.index = nil
}

func (ex *Extent) ReadBlocks(offsets []uint32) ([]pb.Block, error) {

	var ret []pb.Block
	//TODO: fix block number
	for _, offset := range offsets {
		current := atomic.LoadUint32(&ex.commitLength)
		if current <= offset {
			return nil, errors.Errorf("offset is too big")
		}
		r, err := ex.GetReader(offset)
		if err != nil {
			return nil, err
		}
		block, err := readBlock(r)
		if err != nil {
			return nil, err
		}
		ret = append(ret, block)
	}
	return ret, nil
}

func (ex *Extent) CommitLength() uint32 {
	return atomic.LoadUint32(&ex.commitLength)
}

func (ex *Extent) AppendBlocks(blocks []pb.Block, lastCommit uint32) (ret []uint32, err error) {
	ex.AssertLock()

	if atomic.LoadInt32(&ex.isSeal) == 1 {
		return nil, errors.Errorf("immuatble")
	}

	//for secondary extents, it must check lastCommit.
	if lastCommit != 0 && lastCommit != ex.CommitLength() {
		return nil, errors.Errorf("offset not match...")
	}
	/*
		wrap <offset + blocks>
		offset := ex.commitLength
	*/
	for _, block := range blocks {
		if err = writeBlock(ex.file, block); err != nil {
			return nil, err
		}
		//if we have ssd journal, do not have to sync every time.
		//TODO: wait ssd channel,  这里分情况, 如果有SSD journal, 就不需要调用sync
		//如果没有SSD journal,就需要调用sync
		ex.file.Sync()

		currentLength := atomic.LoadUint32(&ex.commitLength)
		ret = append(ret, currentLength)
		ex.index.Put(currentLength, &pb.BlockMeta{
			BlockLength: block.BlockLength,
			BlockOffset: ex.commitLength,
			Offset:      ex.commitLength,
		})
		atomic.AddUint32(&ex.commitLength, block.BlockLength+512)
	}
	return
}

func writeBlock(w io.Writer, block pb.Block) (err error) {
	if len(block.Name) > 256 {
		return errors.Errorf("block name is too long :%d", len(block.Name))
	}

	if !align(uint64(block.BlockLength)) {
		return errors.Errorf("block is not  aligned %d", block.BlockLength)
	}
	//checkSum

	if block.CheckSum != AdlerCheckSum(block.Data) {
		return errors.Errorf("alder32 checksum not match")
	}
	padding := 512 - (4 + 4 + 4 + len(block.Name))

	//write block metadata
	binary.Write(w, binary.BigEndian, block.CheckSum)
	binary.Write(w, binary.BigEndian, block.BlockLength)
	binary.Write(w, binary.BigEndian, uint32(len(block.Name)))
	_, err = w.Write([]byte(block.Name))
	if err != nil {
		return err
	}

	_, err = w.Write(make([]byte, padding))
	if err != nil {
		return err
	}

	//write block data
	_, err = w.Write(block.Data)
	return err
}

var (
	hashPool = sync.Pool{
		New: func() interface{} {
			return adler32.New()
		},
	}
)

func AdlerCheckSum(data []byte) uint32 {
	hash := hashPool.Get().(hash.Hash32)
	defer hashPool.Put(hash)
	hash.Reset()
	hash.Write(data)
	return hash.Sum32()
}

func readBlock(reader io.Reader) (pb.Block, error) {

	var buf [512]byte

	_, err := io.ReadFull(reader, buf[:])

	if err != nil {
		return pb.Block{}, err
	}

	checkSum := binary.BigEndian.Uint32(buf[:4])
	blockLength := binary.BigEndian.Uint32(buf[4:8])
	nameLength := binary.BigEndian.Uint32(buf[8:12])
	if nameLength > 256 {
		return pb.Block{}, errors.Errorf("block name is too long :%d", nameLength)
	}
	if !align(uint64(blockLength)) {
		return pb.Block{}, errors.Errorf("block is not aligned %d", blockLength)
	}
	name := buf[12 : 12+nameLength]

	data := make([]byte, blockLength, blockLength)
	_, err = io.ReadFull(reader, data)

	if err != nil && err != io.EOF {
		return pb.Block{}, err
	}

	//checkSum
	if AdlerCheckSum(data) != checkSum {
		return pb.Block{}, errors.Errorf("alder32 checksum not match")
	}

	return pb.Block{
		CheckSum:    checkSum,
		BlockLength: blockLength,
		Name:        string(name),
		Data:        data,
	}, nil
}
