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
	"os"
	"sync"
	"sync/atomic"

	"io"

	"github.com/pkg/errors"

	"github.com/journeymidnight/streamlayer/proto/pb"
)

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
	//Writer
	//Reader
	isSeal       int32
	commitLength uint64
	fileName     string
	//FIXME: add SSD Chanel
	sync.Mutex
	file *os.File
}

func OpenExtent(fileName string) *Extent {

}

func (ex *Extent) AppendBlocks(blocks []pb.Block) (err error) {
	ex.Lock()
	defer ex.Unlock()

	if atomic.LoadInt32(&ex.isSeal) == 1 {
		return errors.Errorf("immuatble")
	}
	/*
		wrap <offset + blocks>
		offset := ex.commitLength
	*/
	for i := range blocks {
		err = WriteBlock(ex.file, blocks[i])
		if err != nil {
			return
		}
		/*
			wait ssd channel
		*/

		ex.index.Put(ex.commitLength, ex.commitLength, blocks[i].Name) //
		ex.commitLength += uint64(blocks[i].Length + 512)
	}
	//update commitLength
	return nil
}

func WriteBlock(w io.Writer, block pb.Block) (err error) {

	if len(block.Name) > 256 || align(uint64(block.Length)) {
		return errors.Errorf("block name is too long :%d", len(block.Name))
	}
	padding := 512 - (4 + 4 + 4 + len(block.Name))

	binary.Write(w, binary.BigEndian, block.CheckSum)
	binary.Write(w, binary.BigEndian, block.Length)
	binary.Write(w, binary.BigEndian, uint32(len(block.Name)))
	_, err = w.Write([]byte(block.Name))
	if err != nil {
		return err
	}
	_, err = w.Write(make([]byte, padding))
	if err != nil {
		return err
	}
	_, err = w.Write(block.Data)
	return err
}

func ReadBlock(reader io.Reader) (pb.Block, error) {
	var buf [512]byte
	_, err := io.ReadFull(reader, buf[:])

	if err != nil {
		return pb.Block{}, err
	}
	checkSum := binary.BigEndian.Uint32(buf[:4])
	length := binary.BigEndian.Uint32(buf[4:8])
	nameLength := binary.BigEndian.Uint32(buf[4:12])
	if align(uint64(length)) || nameLength > 256 {
		return pb.Block{}, errors.Errorf("align(length) || nameLength > 256")
	}
	name := buf[12 : 12+nameLength]

	data := make([]byte, length, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return pb.Block{}, err
	}
	//TODO, CRC check
	return pb.Block{
		CheckSum: checkSum,
		Length:   length,
		Name:     string(name),
		Data:     data,
	}, nil
}
