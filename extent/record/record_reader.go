// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package record reads and writes sequences of records. Each record is a stream
// of bytes that completes before the next record starts.
//
// When reading, call Next to obtain an io.Reader for the next record. Next will
// return io.EOF when there are no more records. It is valid to call Next
// without reading the current record to exhaustion.
//
// When writing, call Next to obtain an io.Writer for the next record. Calling
// Next finishes the current record. Call Close to finish the final record.
//
// Optionally, call Flush to finish the current record and flush the underlying
// writer without starting a new record. To start a new record after flushing,
// call Next.
//
// Neither Readers or Writers are safe to use concurrently.
//
// Example code:
//	func read(r io.Reader) ([]string, error) {
//		var ss []string
//		records := record.NewReader(r)
//		for {
//			rec, err := records.Next()
//			if err == io.EOF {
//				break
//			}
//			if err != nil {
//				log.Printf("recovering from %v", err)
//				r.Recover()
//				continue
//			}
//			s, err := ioutil.ReadAll(rec)
//			if err != nil {
//				log.Printf("recovering from %v", err)
//				r.Recover()
//				continue
//			}
//			ss = append(ss, string(s))
//		}
//		return ss, nil
//	}
//
//	func write(w io.Writer, ss []string) error {
//		records := record.NewWriter(w)
//		for _, s := range ss {
//			rec, err := records.Next()
//			if err != nil {
//				return err
//			}
//			if _, err := rec.Write([]byte(s)), err != nil {
//				return err
//			}
//		}
//		return records.Close()
//	}
//
// The wire format is that the stream is divided into 32KiB blocks, and each
// block contains a number of tightly packed chunks. Chunks cannot cross block
// boundaries. The last block may be shorter than 32 KiB. Any unused bytes in a
// block must be zero.
//
// A record maps to one or more chunks. Each chunk has a 7 byte header (a 4
// byte checksum, a 2 byte little-endian uint16 length, and a 1 byte chunk type)
// followed by a payload. The checksum is over the chunk type and the payload.
//
// There are four chunk types: whether the chunk is the full record, or the
// first, middle or last chunk of a multi-chunk record. A multi-chunk record
// has one first chunk, zero or more middle chunks, and one last chunk.
//
// The wire format allows for limited recovery in the face of data corruption:
// on a format error (such as a checksum mismatch), the reader moves to the
// next block and looks for the next full or first chunk.
package record // import "github.com/petermattis/pebble/internal/record"

// The C++ Level-DB code calls this the log, but it has been renamed to record
// to avoid clashing with the standard log package, and because it is generally
// useful outside of logging. The C++ code also uses the term "physical record"
// instead of "chunk", but "chunk" is shorter and less confusing.

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/journeymidnight/autumn/utils"
)

// These constants are part of the wire format and should not be changed.
const (
	fullChunkType   = 1
	firstChunkType  = 2
	middleChunkType = 3
	lastChunkType   = 4
)

const (
	BlockSize     = 32 * 1024
	BlockSizeMask = BlockSize - 1
	HeaderSize    = 7
)

var (
	// ErrNotAnIOSeeker is returned if the io.Reader underlying a Reader does not implement io.Seeker.
	ErrNotAnIOSeeker = errors.New("pebble/record: reader does not implement io.Seeker")

	// ErrNoLastRecord is returned if LastRecordOffset is called and there is no previous record.
	ErrNoLastRecord = errors.New("pebble/record: no last record exists")
)

type flusher interface {
	Flush() error
}

type syncer interface {
	Sync() error
}

// Reader reads records from an underlying io.Reader.
type Reader struct {
	// r is the underlying reader.
	r io.Reader
	// seq is the sequence number of the current record.
	seq int
	// buf[i:j] is the unread portion of the current chunk's payload.
	// The low bound, i, excludes the chunk header.
	i, j int
	// n is the number of bytes of buf that are valid. Once reading has started,
	// only the final block can have n < blockSize.
	n int
	// started is whether Next has been called at all.
	started bool
	// recovering is true when recovering from corruption.
	recovering bool
	// last is whether the current chunk is the last chunk of the record.
	last bool
	// err is any accumulated error.
	err error
	// buf is the buffer.
	buf [BlockSize]byte
}

// NewReader returns a new reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: r,
	}
}

// nextChunk sets r.buf[r.i:r.j] to hold the next chunk's payload, reading the
// next block into the buffer if necessary.
func (r *Reader) nextChunk(wantFirst bool) error {
	for {
		if r.j+HeaderSize <= r.n {
			checksum := binary.LittleEndian.Uint32(r.buf[r.j+0 : r.j+4])
			length := binary.LittleEndian.Uint16(r.buf[r.j+4 : r.j+6])
			chunkType := r.buf[r.j+6]

			if checksum == 0 && length == 0 && chunkType == 0 {
				if wantFirst || r.recovering {
					// Skip the rest of the block, if it looks like it is all
					// zeroes. This is common if the record file was created
					// via mmap.
					//
					// Set r.err to be an error so r.Recover actually recovers.
					r.err = errors.New("pebble/record: block appears to be zeroed")
					if !r.recovering {
						return r.err
					}
					r.Recover()
					continue
				}
				return errors.New("pebble/record: invalid chunk")
			}

			r.i = r.j + HeaderSize
			r.j = r.j + HeaderSize + int(length)
			if r.j > r.n {
				if r.recovering {
					r.Recover()
					continue
				}
				return errors.New("pebble/record: invalid chunk (length overflows block)")
			}
			if checksum != utils.NewCRC(r.buf[r.i-1:r.j]).Value() {
				//if checksum != crc.New(r.buf[r.i-1:r.j]).Value() {
				if r.recovering {
					r.Recover()
					continue
				}
				return errors.New("pebble/record: invalid chunk (checksum mismatch)")
			}
			if wantFirst {
				if chunkType != fullChunkType && chunkType != firstChunkType {
					continue
				}
			}
			r.last = chunkType == fullChunkType || chunkType == lastChunkType
			r.recovering = false
			return nil
		}
		if r.n < BlockSize && r.started {
			if r.j != r.n {
				return io.ErrUnexpectedEOF
			}
			return io.EOF
		}
		n, err := io.ReadFull(r.r, r.buf[:])
		if err != nil && err != io.ErrUnexpectedEOF {
			return err
		}
		r.i, r.j, r.n = 0, 0, n
	}
}

// Next returns a reader for the next record. It returns io.EOF if there are no
// more records. The reader returned becomes stale after the next Next call,
// and should no longer be used.
func (r *Reader) Next() (io.Reader, error) {
	r.seq++
	if r.err != nil {
		return nil, r.err
	}
	r.i = r.j
	r.err = r.nextChunk(true)
	if r.err != nil {
		return nil, r.err
	}
	r.started = true
	return singleReader{r, r.seq}, nil
}

// Recover clears any errors read so far, so that calling Next will start
// reading from the next good 32KiB block. If there are no such blocks, Next
// will return io.EOF. Recover also marks the current reader, the one most
// recently returned by Next, as stale. If Recover is called without any
// prior error, then Recover is a no-op.
func (r *Reader) Recover() {
	if r.err == nil {
		return
	}
	r.recovering = true
	r.err = nil
	// Discard the rest of the current block.
	r.i, r.j, r.last = r.n, r.n, false
	// Invalidate any outstanding singleReader.
	r.seq++
}

// SeekRecord seeks in the underlying io.Reader such that calling r.Next
// returns the record whose first chunk header starts at the provided offset.
// Its behavior is undefined if the argument given is not such an offset, as
// the bytes at that offset may coincidentally appear to be a valid header.
//
// It returns ErrNotAnIOSeeker if the underlying io.Reader does not implement
// io.Seeker.
//
// SeekRecord will fail and return an error if the Reader previously
// encountered an error, including io.EOF. Such errors can be cleared by
// calling Recover. Calling SeekRecord after Recover will make calling Next
// return the record at the given offset, instead of the record at the next
// good 32KiB block as Recover normally would. Calling SeekRecord before
// Recover has no effect on Recover's semantics other than changing the
// starting point for determining the next good 32KiB block.
//
// The offset is always relative to the start of the underlying io.Reader, so
// negative values will result in an error as per io.Seeker.
func (r *Reader) SeekRecord(offset int64) error {
	r.seq++
	if r.err != nil {
		return r.err
	}

	s, ok := r.r.(io.Seeker)
	if !ok {
		return ErrNotAnIOSeeker
	}

	// Only seek to an exact block offset.
	c := int(offset & BlockSizeMask)
	if _, r.err = s.Seek(offset&^BlockSizeMask, io.SeekStart); r.err != nil {
		return r.err
	}

	// Clear the state of the internal reader.
	r.i, r.j, r.n = 0, 0, 0
	r.started, r.recovering, r.last = false, false, false
	if r.err = r.nextChunk(false); r.err != nil {
		return r.err
	}

	// Now skip to the offset requested within the block. A subsequent
	// call to Next will return the block at the requested offset.
	r.i, r.j = c, c

	return nil
}

type singleReader struct {
	r   *Reader
	seq int
}

func (x singleReader) Read(p []byte) (int, error) {
	r := x.r
	if r.seq != x.seq {
		return 0, errors.New("pebble/record: stale reader")
	}
	if r.err != nil {
		return 0, r.err
	}
	for r.i == r.j {
		if r.last {
			return 0, io.EOF
		}
		if r.err = r.nextChunk(false); r.err != nil {
			return 0, r.err
		}
	}
	n := copy(p, r.buf[r.i:r.j])
	r.i += n
	return n, nil
}

//remove NewWriter
