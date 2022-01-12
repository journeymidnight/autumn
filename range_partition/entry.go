/*
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
package range_partition

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/journeymidnight/autumn/utils"
	"github.com/pkg/errors"
)

/*
Format of Entry
+------------------+
|  key length      | 4 bytes
+------------------+
|  keyWithTs       | //len(keyWithTs) = key length + 8
+------------------+
|  ExpireTime      | 8 bytes
+------------------+
+ MetaData         | 4 bytes
+-length of Value--+ 4 bytes
+------------------+
|  Value           |
+------------------+
*/

type block = []byte

const (
	BitDelete       byte = 1 << 0    // Set if the key has been deleted.
	BitValuePointer byte = 1 << 1    // Set if the value is NOT stored directly next to key.
	ValueThrottle        = (4 << 10) // 4 * KB
)

func ShouldWriteValueToLSM(e *Entry) bool {
	return e.Meta&uint32(BitValuePointer) == 0 && len(e.Value) <= ValueThrottle
}

type Entry struct {
	Key       []byte //slice of inner
	Value     []byte //slice of inner
	ExpiresAt uint64 //copy from inner
	Meta      uint32 //copy from inner

	inner []byte
	pos   int //end of data in inner

	//used in memory, not in log
	ExtentID uint64
	Offset   uint32
	End      uint32
}

func NewDeleteEntry(userKey []byte) *Entry {
	entry := &Entry{
		inner: make([]byte, len(userKey)+28),
	}
	entry.writeMeta(userKey, 0, uint32(BitDelete), 0)
	entry.Decode()
	return entry
}

func NewPutKVEntry(k, v []byte, expireAt uint64) *Entry {
	entry := NewPutEntry(k, expireAt, uint32(len(v)))
	entry.WriteValue(v)
	entry.FinishWrite()
	return entry
}

func NewPutEntry(userKey []byte, expireAt uint64, lenOfValue uint32) *Entry {
	entry := &Entry{
		inner: make([]byte, len(userKey)+int(lenOfValue)+28),
	}
	var meta uint32
	if lenOfValue > ValueThrottle {
		meta |= uint32(BitValuePointer)
	}
	entry.writeMeta(userKey, expireAt, meta, lenOfValue)
	return entry
}

func (entry *Entry) writeUint32(n uint32) {
	binary.BigEndian.PutUint32(entry.inner[entry.pos:], n)
	entry.pos += 4
}

func (entry *Entry) writeUint64(n uint64) {
	binary.BigEndian.PutUint64(entry.inner[entry.pos:], n)
	entry.pos += 8
}

func (entry *Entry) writeMeta(userKey []byte, expireAt uint64, meta uint32, lenOfValue uint32) {
	keyWithTs := y.KeyWithTs(userKey, 0)
	entry.writeUint32(uint32(len(keyWithTs)))
	entry.write(keyWithTs)
	entry.writeUint64(expireAt)
	entry.writeUint32(meta)
	entry.writeUint32(lenOfValue)
}

func (entry *Entry) WriteValue(d []byte) error {
	return entry.write(d)
}

func (entry *Entry) Format() string {
	return fmt.Sprintf("key: %s, ttl: %d, meta: %d, offset: %d, end: %d", string(entry.Key), entry.ExpiresAt, entry.Meta, entry.Offset, entry.End)
}

func (entry *Entry) UpdateTS(ts uint64) {
	binary.BigEndian.PutUint64(entry.Key[len(entry.Key)-8:], math.MaxUint64-ts)
}

func (entry *Entry) FinishWrite() error {
	return entry.Decode()
}

func (entry *Entry) Valid() bool {
	return entry.pos == len(entry.inner)
}

func (entry *Entry) write(buf []byte) error {
	if entry.pos+len(buf) > len(entry.inner) {
		return errors.New("buffer is not enough")
	}
	copy(entry.inner[entry.pos:], buf)
	entry.pos += len(buf)
	return nil
}

func (entry *Entry) Encode() []byte {
	utils.AssertTruef(entry.pos == len(entry.inner), "entry.pos:%d, len(entry.inner):%d", entry.pos, len(entry.inner))
	return entry.inner
}

func (entry *Entry) Size() int {
	utils.AssertTruef(entry.pos == len(entry.inner), "entry.pos:%d, len(entry.inner):%d", entry.pos, len(entry.inner))
	return len(entry.inner)
}

func DecodeEntry(data []byte) (*Entry, error) {
	entry := &Entry{
		inner: data,
	}
	entry.pos = len(data)
	if err := entry.Decode(); err != nil {
		return nil, err
	}
	return entry, nil
}

func (entry *Entry) Decode() error {
	if entry.pos > len(entry.inner) {
		return errors.New("buffer is not enough")
	}
	keyLength := binary.BigEndian.Uint32(entry.inner[:4])
	entry.Key = entry.inner[4 : 4+keyLength]
	entry.ExpiresAt = binary.BigEndian.Uint64(entry.inner[4+keyLength : 4+keyLength+8])
	entry.Meta = binary.BigEndian.Uint32(entry.inner[4+keyLength+8 : 4+keyLength+8+4])
	valueLength := binary.BigEndian.Uint32(entry.inner[4+keyLength+8+4 : 4+keyLength+8+4+4])
	entry.Value = entry.inner[4+keyLength+8+4+4 : 4+keyLength+8+4+4+valueLength]
	return nil
}
