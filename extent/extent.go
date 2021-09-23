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
	"fmt"
	"io"
	"io/ioutil"
	"strconv"

	"github.com/pkg/errors"

	"bytes"
	"encoding/json"
	"math"
	"os"
	"sync/atomic"

	"github.com/journeymidnight/autumn/extent/record"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/xattr"
)

const (
	extentMagicNumber = "EXTENTXX"
	XATTRMETA         = "user.EXTENTMETA"
	XATTRSEAL         = "user.XATTRSEAL"
	XATTRREV          = "user.REV"
)

type Extent struct {
	//sync.Mutex //only one AppendBlocks could be called at a time
	utils.SafeMutex
	isSeal       int32  //atomic
	commitLength uint32 //atomic
	ID           uint64
	fileName     string
	file         *os.File
	//FIXME: add SSD Chanel
	writer *record.LogWriter
	lastRevision int64
}

//format to JSON
type extentHeader struct {
	MagicNumber []byte
	ID          uint64
}

func (eh *extentHeader) Marshal() []byte {
	data, err := json.Marshal(eh)
	utils.Check(err)
	return data
}

func (eh *extentHeader) Unmarshal(data []byte) error {
	return json.Unmarshal(data, eh)
}

func newExtentHeader(ID uint64) *extentHeader {
	eh := extentHeader{
		ID: ID,
	}
	eh.MagicNumber = []byte(extentMagicNumber)
	return &eh
}

func readExtentHeader(file *os.File) (*extentHeader, error) {

	d, err := xattr.FGet(file, XATTRMETA)
	if err != nil {
		return nil, err
	}

	eh := newExtentHeader(0)

	if err = eh.Unmarshal(d); err != nil {
		return nil, err
	}

	if bytes.Compare(eh.MagicNumber, []byte(extentMagicNumber)) != 0 {
		return nil, errors.New("meta data is not corret")
	}
	return eh, nil

}

func CreateCopyExtent(fileName string, ID uint64) (string, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return "", err
	}
	f.Sync()
	defer f.Close()
	extentHeader := newExtentHeader(ID)
	value := extentHeader.Marshal()

	if err := xattr.FSet(f, XATTRMETA, value); err != nil {
		return "", err
	}
	return fileName, nil
}

func CreateExtent(fileName string, ID uint64) (*Extent, error) {
	//FIXME: lock file
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	extentHeader := newExtentHeader(ID)
	value := extentHeader.Marshal()

	if err := xattr.FSet(f, XATTRMETA, value); err != nil {
		return nil, err
	}

	//f.Sync()
	//write header of Extent
	ex := &Extent{
		ID:           ID,
		isSeal:       0,
		commitLength: 0,
		fileName:     fileName,
		file:         f,
	}
	ex.resetWriter()
	return ex, nil

}

func OpenExtent(fileName string) (*Extent, error) {

	d, err := xattr.LGet(fileName, XATTRSEAL)

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
		
		eh, err := readExtentHeader(file)
		if err != nil {
			return nil, err
		}

		ex := &Extent{
			isSeal:       1,
			commitLength: uint32(info.Size()),
			fileName:     fileName,
			file:         file,
			ID:           eh.ID,
		}
		return ex, nil
	}

	/*
		如果extent是意外关闭
		1. 他的3副本很可能不一致. 如果有新的写入, 在primary上面是Append, 在secondary上面的API
		是检查Offset的Append, 如果这3个任何一个失败, client就找sm把extent变成:truncate/Sealed.
		2. 由于写入是多个sector(record.BlockSize至少32KB),也会有一致性问题:
		log的格式是n * record.BlockSize + tail, 一般不一致是在tail部分, 和leveldb类似, 在replayWAL
		时, 如果发现错误, 则create新的extent或者总是create新extent(rocksdb或者leveldb逻辑)
	*/
	f, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	info, _ := f.Stat()
	currentSize := uint32(info.Size())

	eh, err := readExtentHeader(f)
	if err != nil {
		return nil, err
	}

	var rev = int64(0)
	hex, _ := xattr.FGet(f, XATTRREV)
	rev, _ = strconv.ParseInt(string(hex), 10, 64)
	
	ex := &Extent{
		isSeal:       0,
		commitLength: currentSize,
		fileName:     fileName,
		file:         f,
		ID:           eh.ID,
		lastRevision: rev,
	}
	ex.resetWriter()
	return ex, nil
}

//support multple threads
//limit max read size
type extentReader struct {
	extent *Extent
	pos    int64
}

func (r *extentReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekEnd:
		r.pos += offset //offset is nagative
	case io.SeekStart:
		r.pos = offset
	case io.SeekCurrent:
		r.pos += offset
	default:
		return 0, errors.New("not supported")
	}
	return int64(r.pos), nil

}

//readfull
func (r *extentReader) Read(p []byte) (n int, err error) {
	n, err = r.extent.file.ReadAt(p, r.pos)
	if err != nil && err != io.EOF {
		return -1, err
	}
	if n == 0 {
		return 0, io.EOF
	}

	r.pos += int64(n)
	return n, nil
}

//Seal requires LOCK
//if commit is bigger than currentLength, return error
func (ex *Extent) Seal(commit uint32) error {

	ex.AssertLock()

	if ex.writer != nil {
		ex.writer.Close()
		ex.writer = nil
	}
	
	currentLength := atomic.LoadUint32(&ex.commitLength)
	if currentLength < commit {
		ex.resetWriter()
		return errors.New("commit is less than current commit length")
	}
	ex.commitLength = commit
	ex.file.Truncate(int64(commit))
	if err := xattr.FSet(ex.file, XATTRSEAL, []byte("true")); err != nil {
		return err
	}

	atomic.StoreInt32(&ex.isSeal, 1)
	return nil

}

func (ex *Extent) IsSeal() bool {
	return atomic.LoadInt32(&ex.isSeal) == 1
}

func (ex *Extent) GetReader() *extentReader {
	return &extentReader{
		extent: ex,
		pos:    int64(0),
	}

}
type rawWriter struct{
	io.WriteSeeker
	extent *Extent
}

func (fw *rawWriter) Write(p []byte) (n int, err error) {
	//extent will be sealed, no need to update lastRevision
	n, err = fw.extent.file.Write(p)
	fw.extent.commitLength += uint32(n)
	return n, err
}

func (fw *rawWriter) Seek(offset int64, whence int) (int64, error) {
	//no-op
	return 0, nil
}


//fixWriter is used to fill gaps between
func (ex *Extent) GetRawWriter() *rawWriter{
	ex.AssertLock()

	if ex.writer != nil {
		utils.Check(ex.writer.Close())
		ex.writer = nil
	}
	ex.file.Seek(int64(ex.commitLength), io.SeekStart)
	return &rawWriter{
		extent: ex,
	}
}

//Close requeset LOCK
func (ex *Extent) Close() {
	ex.Lock()
	defer ex.Unlock()
	if ex.writer != nil {
		ex.writer.Close()
	}
	ex.file.Close()
}


func (ex *Extent) Truncate(length uint32) error {
	ex.AssertLock()

	ex.commitLength = length
	ex.file.Truncate(int64(length))
	return ex.resetWriter()
}

func (ex *Extent) HasLock(revision int64) bool {
	if ex.lastRevision == revision {
		return true
	} else if ex.lastRevision < revision {
		ex.lastRevision = revision
		xattr.FSet(ex.file, XATTRREV, []byte(strconv.FormatInt(revision, 10)))
		return true
	}
	return false
}

func (ex *Extent) resetWriter() error {
	if ex.writer != nil {
		ex.writer.Close()
	}
	if atomic.LoadInt32(&ex.isSeal) == 1 {
		return nil
	}

	utils.AssertTrue(ex.IsSeal() == false)
	info, err := ex.file.Stat()
	utils.Check(err)

	currentLength := atomic.LoadUint32(&ex.commitLength)
	utils.AssertTrue(currentLength == uint32(info.Size()))

	ex.file.Seek(int64(currentLength), os.SEEK_SET)
	bn := (currentLength / record.BlockSize)
	offset := currentLength % record.BlockSize
	newWriter := record.NewLogWriter(ex.file, int64(bn), int32(offset))
	ex.writer = newWriter
	return nil
}

func (ex *Extent) ResetWriter() error {
	return ex.resetWriter()
}

func (ex *Extent) RecoveryData(start uint32, rev int64, blocks []*pb.Block) error {

	if ex.IsSeal() {
		return nil
	}
	//因为如果能写入log, 说明rev一定是更高的, 这里调用
	//HasLock更新lastRevision
	ex.HasLock(rev)

	expectedEnd := start


	for _, block := range blocks {
		expectedEnd = record.ComputeEnd(expectedEnd, uint32(len(block.Data)))
	}

	currentLength := atomic.LoadUint32(&ex.commitLength)

	if expectedEnd <= currentLength {
		return nil
	}
	ex.file.Seek(int64(start), os.SEEK_SET)

	xlog.Logger.Debugf("fixing %d blocks from %d\n", len(blocks), start)
	//fix current extent
	bn := (start / record.BlockSize)
	offset := start % record.BlockSize
	newWriter := record.NewLogWriter(ex.file, int64(bn), int32(offset))
	for _, block := range blocks {
		if _, _, err := newWriter.WriteRecord(block.Data); err != nil {
			return err
		}
	}

	newWriter.Close() //close will force flush data to underlying file. but doesn't close file

	info, err := ex.file.Stat()
	utils.Check(err)
	utils.AssertTrue(expectedEnd == uint32(info.Size()))
	atomic.StoreUint32(&ex.commitLength, expectedEnd)
	return nil
}

func (ex *Extent) Sync() {
	ex.writer.Sync()
}

func (ex *Extent) AppendBlocks(blocks []*pb.Block, doSync bool) ([]uint32, uint32, error) {

	ex.AssertLock()

	if atomic.LoadInt32(&ex.isSeal) == 1 {
		return nil, 0, errors.Errorf("immuatble")
	}

	currentLength := ex.commitLength

	truncate := func() {
		ex.writer.Flush()

		utils.Check(ex.file.Truncate(int64(currentLength)))
		ex.file.Sync()
		atomic.StoreUint32(&ex.commitLength, currentLength)
		//reset writer
		ex.resetWriter()
	}

	utils.AssertTrue(ex.writer != nil)

	var offsets []uint32
	var start int64
	end := int64(currentLength)
	var err error
	for _, block := range blocks {
		start, end, err = ex.writer.WriteRecord(block.Data)
		utils.AssertTrue(end <= math.MaxUint32)
		if err != nil {
			truncate()
			return nil, 0, err
		}
		offsets = append(offsets, uint32(start))
	}
	ex.writer.Flush()
	if doSync {
		ex.writer.Sync()
	}
	utils.AssertTrue(end <= math.MaxUint32)

	atomic.StoreUint32(&ex.commitLength, uint32(end))
	return offsets, uint32(end), nil
}


//ValidAllBlocks表示从start,就存在一个合法的数据block(不是64KB BLOCK)
func (ex *Extent) ValidAllBlocks(start int64) (uint32, error) {
	var err error

	wrapReader := ex.GetReader() //thread-safe

	rr := record.NewReader(wrapReader)
	rr.SeekRecord(start)
	var end uint32
	var rec io.Reader
	for {
		rec, err = rr.Next()
		if err == io.EOF {
			return end, nil
		}
		if err != nil {
			//err happens...
			return end, err
		}

		_, err = ioutil.ReadAll(rec)
		if err != nil {
			return end, err
		}
		end = uint32(rr.End())
	}
}

func (ex *Extent) ReadBlocks(offset uint32, maxNumOfBlocks uint32, maxTotalSize uint32) ([]*pb.Block, []uint32, uint32, error) {

	var ret []*pb.Block
	//TODO: fix block number

	currentLength := atomic.LoadUint32(&ex.commitLength)

	if currentLength <= offset {
		return nil, nil, 0, wire_errors.EndOfExtent
	}

	wrapReader := ex.GetReader() //thread-safe
	rr := record.NewReader(wrapReader)
	err := rr.SeekRecord(int64(offset))
	if err != nil {
		return nil, nil, 0, err
	}

	var offsets []uint32
	var end uint32
	for i := 0; uint32(i) < maxNumOfBlocks; i++ {
		reader, err := rr.Next()
		start := rr.Offset()
		if err == io.EOF {
			return ret, offsets, end, wire_errors.EndOfExtent
		}

		if err != nil {
			fmt.Printf("DISK ReadBlocks: err != nil %s\n", err)
			xlog.Logger.Errorf("extent %d readBlock from %d , err is %s", ex.ID, offset, err)
			return nil, nil, 0, err
		}



		data, err := ioutil.ReadAll(reader)

		if rr.End() - int64(offset) > int64(maxTotalSize) && len(ret) > 0 {
			end = uint32(start)
			break
		}

		ret = append(ret, &pb.Block{Data: data})
		offsets = append(offsets, uint32(start))
		end = uint32(rr.End())
	}
	utils.AssertTrue(end <= ex.commitLength)
	//fmt.Printf("read block at extent ID %d, end is %d\n", ex.ID, end)
	return ret, offsets, end, nil
}

//return data, offset, end, error
func (ex *Extent) ReadLastBlock() ([]*pb.Block, []uint32, uint32, error) {

	wrapReader := ex.GetReader() //thread-safe
	offset := int64(atomic.LoadUint32(&ex.commitLength) & ^uint32(record.BlockSizeMask))
	//floor of offset for record.BlockSize
	var data []byte
	var reader io.Reader
	var err error
	var end uint32
	var start uint32
	LOOP:
	for ; offset >= 0; offset -= record.BlockSize{
		rr := record.NewReader(wrapReader)
		rr.SeekRecord(offset)
		for {
			reader, err = rr.Next()
			if err == io.EOF {
				if data != nil {
					break LOOP
				}
				break
			}
			if err != nil {
				//block crc is bad:
				break
			}
			
			start = uint32(rr.Offset())
			data, err = ioutil.ReadAll(reader)
			if err != nil {
				//block crc is bad:
				break
			}
			end = uint32(rr.End())
		}
	}
	if data == nil {
		return nil, nil, 0, wire_errors.NotFound
	}
	return []*pb.Block{{Data: data}}, []uint32{start}, end, nil
}

func (ex *Extent) CommitLength() uint32 {
	return atomic.LoadUint32(&ex.commitLength)
}

//helper function, block could be pb.Entries, support ReadEntries
//ReadEntries can only be called on replicated extent
//node_service will never call this function, this function is only for test
func (ex *Extent) ReadEntries(offset uint32, maxTotalSize uint32, replay bool) ([]*pb.EntryInfo, uint32, error) {

	
	blocks, offsets, end, err := ex.ReadBlocks(offset, 10, maxTotalSize)
	if err != nil && err != wire_errors.EndOfExtent {
		return nil, 0, err
	}
	var ret []*pb.EntryInfo
	for i := range blocks {
		e, err := ExtractEntryInfo(blocks[i], ex.ID, offsets[i], replay)
		if err != nil {
			xlog.Logger.Error(err)
			continue
		}
		ret = append(ret, e)
	}

	return ret, end, err

}


func ExtractEntryInfo(b *pb.Block, extentID uint64, offset uint32, replay bool) (*pb.EntryInfo, error) {
	entry := new(pb.Entry)
	if err := entry.Unmarshal(b.Data); err != nil {
		return nil, err
	}

	if y.ShouldWriteValueToLSM(entry) {
		if replay { //replay read
			return &pb.EntryInfo{
				Log:           entry,
				EstimatedSize: uint64(entry.Size()),
				ExtentID:      extentID,
				Offset:        offset,
			}, nil
		} else { //gc read
			entry.Value = nil //直接返回空entry
			return &pb.EntryInfo{
				Log:           entry,
				EstimatedSize: uint64(entry.Size()),
				ExtentID:      extentID,
				Offset:        offset,
			}, nil
		}
	} else {
		//big value
		//keep entry.Value and make sure BitValuePointer
		entry.Meta |= uint32(y.BitValuePointer)
		//set value to nil to save network bandwidth
		if replay {
			entry.Value = nil
		}
		return &pb.EntryInfo{
			Log:           entry,
			EstimatedSize: uint64(entry.Size()),
			ExtentID:      extentID,
			Offset:        offset,
		}, nil
	}
}