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
	"io"
	"sort"
	"sync"

	"github.com/journeymidnight/streamlayer/proto/pb"
	"github.com/pkg/errors"
)

type Index interface {
	Get(k uint32) (*pb.BlockMeta, bool)
	Put(k uint32, v *pb.BlockMeta)
	//if range is ordered by k, application can get better performance
	//Range(fn func(k, v pb.BlockMeta))
	Marshal(w io.Writer) error
	Unmarshal(r io.Reader) error
}

const (
	indexMagicNumber = "EXT__IND"
)

//TODO: 修改成google/btree
type DynamicIndex struct {
	sync.RWMutex
	x map[uint32]*pb.BlockMeta
}

func NewDynamicIndex() *DynamicIndex {
	return &DynamicIndex{
		x: make(map[uint32]*pb.BlockMeta),
	}
}

func (di *DynamicIndex) Get(k uint32) (*pb.BlockMeta, bool) {
	di.RWMutex.RLock()
	defer di.RWMutex.RUnlock()
	v, ok := di.x[k]
	if !ok {
		return &pb.BlockMeta{}, false
	}
	return v, true

}

func (di *DynamicIndex) Put(k uint32, v *pb.BlockMeta) {
	di.Lock()
	defer di.Unlock()
	di.x[k] = v
}

func (di *DynamicIndex) Unmarshal(r io.Reader) error {
	di.Lock()
	defer di.Unlock()
	var buf [8]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return err
	}
	if string(buf[:]) != indexMagicNumber {
		return errors.Errorf("Unmarshal failed, magic number is %s", buf)
	}

	var numberOfBlocks uint32
	if err = binary.Read(r, binary.BigEndian, &numberOfBlocks); err != nil {
		return err
	}

	var len uint32
	var meta pb.BlockMeta
	for i := uint32(0); i < numberOfBlocks; i++ {
		err = binary.Read(r, binary.BigEndian, &len) //read len
		if err != nil {
			return err
		}

		buf := make([]byte, len)
		if _, err = io.ReadFull(r, buf); err != nil {
			return err
		}
		meta.Unmarshal(buf)
		di.x[meta.Offset] = &meta
	}
	return nil

}

func (di *DynamicIndex) Marshal(w io.Writer) error {
	di.Lock()
	defer di.Unlock()

	//magic number is 8 bytes
	_, err := w.Write([]byte(indexMagicNumber))
	if err != nil {
		return err
	}
	binary.Write(w, binary.BigEndian, uint32(len(di.x)))

	var offsets []uint32

	//sort pb.BlockMeta by offset
	for k := range di.x {
		offsets = append(offsets, k)
	}

	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	for _, offset := range offsets {
		v := di.x[offset]
		data, err := v.Marshal()
		binary.Write(w, binary.BigEndian, uint32(v.Size()))
		_, err = w.Write(data)
		if err != nil {
			return err
		}
	}
	return err
}
