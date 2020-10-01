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
	"sync"

	"github.com/pkg/errors"
)

type Index interface {
	Get(k uint64) (uint64, bool)
	Put(k uint64, v uint64, name string)
	//if range is ordered by k, application can get better performance
	Range(fn func(k, v uint64, name string))
	Marshal(w io.Writer)
	Unmarshal(r io.Reader)
}

type diValue struct {
	v    uint64
	name string
}

const (
	indexMagicNumber = "Dyna InX"
)

type DynamicIndex struct {
	x *sync.Map
}

func (di *DynamicIndex) Get(k uint64) (uint64, bool) {
	v, ok := di.x.Load(k)
	if !ok {
		return 0, false
	}
	r := v.(diValue)
	return r.v, true
}

func (di *DynamicIndex) Put(k uint64, v uint64, name string) {
	di.x.Store(k, diValue{
		v:    v,
		name: name,
	})
}

func (di *DynamicIndex) Unmarshal(r io.Reader) error {
	var buf [8]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return err
	}
	if string(buf[:]) != indexMagicNumber {
		return errors.Errorf("Unmarshal failed, magic number is %s", buf)
	}
	var k uint64
	var v uint64
	var nameBuf []byte
	var nameLength uint32
	for {
		if err = binary.Read(r, binary.BigEndian, &k); err != nil {
			return err
		}

		if err = binary.Read(r, binary.BigEndian, &v); err != nil {
			return err
		}

		if err = binary.Read(r, binary.BigEndian, &nameLength); err != nil {
			return err
		}
		nameBuf = make([]byte, nameLength, nameLength)
		if _, err = io.ReadFull(r, nameBuf); err != nil {
			return err
		}
		di.x.Store(k, diValue{
			v:    v,
			name: string(nameBuf),
		})

	}

}

func (di *DynamicIndex) Marshal(w io.Writer) error {
	//magic number is 8 bytes
	_, err := w.Write([]byte(indexMagicNumber))
	if err != nil {
		return err
	}
	di.x.Range(func(key interface{}, value interface{}) bool {
		k := key.(uint64)
		v := value.(diValue)

		if err = binary.Write(w, binary.BigEndian, k); err != nil {
			return false
		}

		if err = binary.Write(w, binary.BigEndian, v); err != nil {
			return false

		}

		if err = binary.Write(w, binary.BigEndian, uint32(len(v.name))); err != nil {
			return false
		}

		if _, err = w.Write([]byte(v.name)); err != nil {
			return false
		}
		return true
	})
	return err
}
