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

package y

import (
	"bytes"
	"encoding/binary"

	"github.com/journeymidnight/autumn/utils"
)

// ValueStruct represents the value info that can be associated with a key, but also the internal
// Meta field.
type ValueStruct struct {
	Meta      byte
	ExpiresAt uint64
	Value     []byte

	Version uint64 // This field is not serialized. Only for internal usage.
}

// EncodeTo should be kept in sync with the Encode function above. The reason
// this function exists is to avoid creating byte arrays per key-value pair in
// table/builder.go.
func (v *ValueStruct) Write(buf *bytes.Buffer) {
	buf.WriteByte(v.Meta)
	var enc [binary.MaxVarintLen64]byte
	sz := binary.PutUvarint(enc[:], v.ExpiresAt)
	buf.Write(enc[:sz])
}

// EncodedSize is the size of the ValueStruct when encoded
func (v *ValueStruct) EncodedSize() uint32 {
	sz := len(v.Value) + 1 // meta
	/*
		if v.ExpiresAt == 0 {
			return uint32(sz + 1)
		}
	*/

	enc := utils.SizeVarint(v.ExpiresAt)
	return uint32(sz + enc)
}

// Decode uses the length of the slice to infer the length of the Value field.
func (v *ValueStruct) Decode(b []byte) {
	v.Meta = b[0]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(b[1:])
	v.Value = b[1+sz:]
}

// Encode expects a slice of length at least v.EncodedSize().
func (v *ValueStruct) Encode(b []byte) uint32 {
	b[0] = v.Meta
	sz := binary.PutUvarint(b[1:], v.ExpiresAt)
	n := copy(b[1+sz:], v.Value)
	return uint32(1 + sz + n)
}

// EncodeTo should be kept in sync with the Encode function above. The reason
// this function exists is to avoid creating byte arrays per key-value pair in
// table/builder.go.
func (v *ValueStruct) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(v.Meta)
	var enc [binary.MaxVarintLen64]byte
	sz := binary.PutUvarint(enc[:], v.ExpiresAt)

	buf.Write(enc[:sz])
	buf.Write(v.Value)
}

// Iterator is an interface for a basic iterator.
type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() ValueStruct
	Valid() bool

	// All iterators should be closed so that file garbage collection works.
	Close() error
}
