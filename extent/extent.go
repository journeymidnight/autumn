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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sync/atomic"

	"io"

	"github.com/pkg/errors"
	"github.com/pkg/xattr"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
)

/*
+-----------+-->
| check sum |
+-----------+
|blockLength|
+-----------+  512 Bytes
|           |
|  User Data|
|           |
|           |
+-------------->
|           |
|           |
|  DATA     |  <BlockLength> Bytes
|           |
|           |
|           |
+-----------+-->
*/

//FIXME: put all errors into errors directory
func align(n uint64) bool {
	return n != 0 && n%512 == 0
}

func formatExtentName(id uint64) string {
	//some name
	return fmt.Sprintf("store/extents/%d.ext", id)
}

type Extent struct {
	//sync.Mutex //only one AppendBlocks could be called at a time
	utils.SafeMutex
	isSeal       int32  //atomic
	commitLength uint32 //atomic
	ID           uint64
	fileName     string
	file         *os.File
	//FIXME: add SSD Chanel

}

const (
	extentMagicNumber = "EXTENTXX"
)

type extentHeader struct {
	magicNumber []byte
	ID          uint64
}

func newExtentHeader(ID uint64) *extentHeader {
	eh := extentHeader{
		ID: ID,
	}
	eh.magicNumber = []byte(extentMagicNumber)
	return &eh
}

func (eh *extentHeader) Size() uint32 {
	return 16
}

func (eh *extentHeader) Marshal(w io.Writer) error {
	var buf [512]byte
	copy(buf[:], eh.magicNumber[:])
	binary.BigEndian.PutUint64(buf[8:], eh.ID)

	n, err := w.Write(buf[:])
	if n != 512 || err != nil {
		return errors.Errorf("failed to create extent file")
	}
	return nil
}

func (eh *extentHeader) Unmarshal(r io.Reader) error {
	var buf [512]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return err
	}
	if bytes.Compare(buf[:8], []byte(extentMagicNumber)) != 0 {
		return errors.Errorf("magic number fail")
	}
	eh.magicNumber = []byte(extentMagicNumber)
	copy(eh.magicNumber, buf[:8])
	eh.ID = binary.BigEndian.Uint64(buf[8:16])
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
	f.Sync()
	//write header of Extent
	return &Extent{
		ID:           ID,
		isSeal:       0,
		commitLength: 512,
		fileName:     fileName,
		file:         f,
	}, nil

}

func OpenExtent(fileName string) (*Extent, error) {

	d, err := xattr.LGet(fileName, "seal")

	//if extent is a sealed extent
	if err == nil && bytes.Compare(d, []byte("true")) == 0 {
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
			isSeal:       1,
			commitLength: uint32(info.Size()),
			fileName:     fileName,
			file:         file,
			ID:           eh.ID,
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
		offset += b.BlockLength + 512
	}

	return &Extent{
		isSeal:       0,
		commitLength: offset,
		fileName:     fileName,
		file:         f,
		ID:           eh.ID,
	}, nil
}

//support multple threads
//limit max read size
type extentBlockReader struct {
	extent   *Extent
	position uint32
}

func (r *extentBlockReader) Read(p []byte) (n int, err error) {
	n, err = r.extent.file.ReadAt(p, int64(r.position))
	if err != nil {
		return n, err
	}
	r.position += uint32(n)
	return n, nil
}

func (ex *Extent) Seal(commit uint32) error {
	ex.Lock()
	defer ex.Unlock()
	atomic.StoreInt32(&ex.isSeal, 1)

	currentLength := ex.commitLength
	if currentLength < commit {
		return errors.Errorf("commit is less than current commit length")
	} else if currentLength > commit {
		ex.file.Truncate(int64(commit))
	}

	if err := xattr.FSet(ex.file, "seal", []byte("true")); err != nil {
		return err
	}

	return nil

}

func (ex *Extent) IsSeal() bool {
	return atomic.LoadInt32(&ex.isSeal) == 1
}
func (ex *Extent) GetReader(offset uint32) io.Reader {
	return &extentBlockReader{
		extent:   ex,
		position: offset,
	}

}

//Close function is not thread-safe
func (ex *Extent) Close() {
	ex.Lock()
	defer ex.Unlock()
	ex.file.Close()
}

var (
	EndOfExtent = errors.Errorf("EndOfExtent")
	EndOfStream = errors.Errorf("EndOfStream")
)

func (ex *Extent) ReadBlocks(offset uint32, maxNumOfBlocks uint32, maxTotalSize uint32) ([]*pb.Block, error) {

	var ret []*pb.Block
	//TODO: fix block number
	current := atomic.LoadUint32(&ex.commitLength)
	if current <= offset {
		if ex.IsSeal() {
			return nil, EndOfExtent
		} else {
			return nil, EndOfStream
		}
	}
	size := uint32(0)
	for i := uint32(0); i < maxNumOfBlocks; i++ {
		r := ex.GetReader(offset)

		block, err := readBlock(r)

		if err == io.EOF {
			if ex.IsSeal() {
				return ret, EndOfExtent
			} else {
				return ret, EndOfStream
			}
		}

		if err != nil {
			return nil, err
		}

		ret = append(ret, &block)
		offset += block.BlockLength + 512
		size += block.BlockLength + 512
		if size > maxTotalSize || err == io.EOF {
			break
		}
	}
	return ret, nil
}

func (ex *Extent) CommitLength() uint32 {
	return atomic.LoadUint32(&ex.commitLength)
}

func (ex *Extent) AppendBlocks(blocks []*pb.Block, lastCommit uint32) (ret []uint32, err error) {
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
	currentLength := atomic.LoadUint32(&ex.commitLength)
	for _, block := range blocks {
		if err = writeBlock(ex.file, block); err != nil {
			return nil, err
		}
		//if we have ssd journal, do not have to sync every time.
		//TODO: wait ssd channel,  这里分情况, 如果有SSD journal, 就不需要调用sync
		//如果没有SSD journal,就需要调用sync
		ret = append(ret, currentLength)
		currentLength += block.BlockLength + 512
	}
	ex.file.Sync()
	atomic.StoreUint32(&ex.commitLength, currentLength)
	return
}

func writeBlock(w io.Writer, block *pb.Block) (err error) {

	if !align(uint64(block.BlockLength)) {
		return errors.Errorf("block is not  aligned %d", block.BlockLength)
	}
	//checkSum

	if block.CheckSum != utils.AdlerCheckSum(block.Data) {
		return errors.Errorf("alder32 checksum not match  %d vs %d", block.CheckSum, utils.AdlerCheckSum(block.Data))
	}

	var buf [512]byte

	if 512 < (4 + 4 + 4 + len(block.UserData)) {
		return errors.Errorf("user data is too big %d", block.UserData)
	}
	binary.BigEndian.PutUint32(buf[:], block.CheckSum)
	binary.BigEndian.PutUint32(buf[4:], block.BlockLength)
	if len(block.UserData) != 0 {
		binary.BigEndian.PutUint32(buf[8:], uint32(len(block.UserData)))
		//w.Write(block.UserData)
		copy(buf[12:], block.UserData)
	}

	w.Write(buf[:])

	//write block data
	_, err = w.Write(block.Data)
	return err
}

func readBlock(reader io.Reader) (pb.Block, error) {

	var buf [512]byte

	_, err := io.ReadFull(reader, buf[:])

	if err != nil {
		return pb.Block{}, err
	}

	checkSum := binary.BigEndian.Uint32(buf[:4])
	blockLength := binary.BigEndian.Uint32(buf[4:8])
	len := binary.BigEndian.Uint32(buf[8:12])
	var UserData []byte
	if len > 0 && len < 512 {
		UserData = buf[12 : 12+len]
	}

	data := make([]byte, blockLength, blockLength)
	_, err = io.ReadFull(reader, data)

	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return pb.Block{}, err
	}

	//checkSum
	if utils.AdlerCheckSum(data) != checkSum {
		return pb.Block{}, errors.Errorf("alder32 checksum not match")
	}
	if !align(uint64(blockLength)) {
		return pb.Block{}, errors.Errorf("block is not aligned %d", blockLength)
	}

	return pb.Block{
		CheckSum:    checkSum,
		BlockLength: blockLength,
		Data:        data,
		UserData:    UserData,
	}, nil
}
