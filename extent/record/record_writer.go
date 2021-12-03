// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	_ "fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/journeymidnight/autumn/utils"
)

type block struct {
	// buf[:written] has already been filled with fragments. Updated atomically.
	written int32
	// buf[:flushed] has already been flushed to w.
	flushed int32
	buf     [BlockSize]byte
}

// LogWriter writes records to an underlying io.Writer.
type LogWriter struct {
	// w is the underlying writer.
	w io.Writer
	// c is w as a closer.
	c io.Closer
	// f is w as a flusher.
	f flusher
	// s is w as a syncer.
	s syncer
	// blockNumber is the zero based block number for the current block.
	blockNumber int64
	// err is any accumulated error. TODO(peter): This needs to be protected in
	// some fashion. Perhaps using atomic.Value.
	err error
	// block is the current block being written. Protected by flusher.Mutex.
	block *block
	free  chan *block

	// Protects against concurrent calls to Flush/Sync().
	flushMu sync.Mutex

	flusher struct {
		sync.Mutex
		// Cond var signalled when there are blocks to flush or the Writer has been
		// closed.
		ready sync.Cond
		// Cond var signalled when flushing of pending blocks has been completed.
		done sync.Cond
		// Is flushing currently active?
		flushing bool
		// Has the writer been closed?
		closed bool
		// Accumulated flush error.
		err     error
		pending []*block
	}
}

// NewLogWriter returns a new LogWriter.
func NewLogWriter(w io.Writer, bn int64, offset int32) *LogWriter {
	c, _ := w.(io.Closer)
	f, _ := w.(flusher)
	s, _ := w.(syncer)
	r := &LogWriter{
		w:           w,
		c:           c,
		f:           f,
		s:           s,
		free:        make(chan *block, 4),
		blockNumber: bn,
	}
	for i := 0; i < cap(r.free); i++ {
		r.free <- &block{}
	}
	r.block = <-r.free
	r.block.flushed, r.block.written = offset, offset
	r.flusher.ready.L = &r.flusher.Mutex
	r.flusher.done.L = &r.flusher.Mutex
	go r.flushLoop()
	return r
}

func (w *LogWriter) flushLoop() {
	f := &w.flusher
	f.Lock()
	defer f.Unlock()

	for {
		for {
			if f.closed {
				return
			}
			if f.flushing {
				f.done.Wait()
				continue
			}
			if len(f.pending) == 0 {
				f.ready.Wait()
				continue
			}
			break
		}

		pending := f.pending
		f.pending = nil
		f.flushing = true

		f.Unlock()

		var err error
		for _, b := range pending {
			if err = w.flushBlock(b); err != nil {
				break
			}
		}

		f.Lock()
		f.err = err
		if f.err != nil {
			return
		}
		f.flushing = false
		f.done.Signal()
	}
}

func (w *LogWriter) flushBlock(b *block) error {
	//fmt.Printf("write %d\n", len(b.buf[b.flushed:]))
	if _, err := w.w.Write(b.buf[b.flushed:]); err != nil {
		return err
	}
	b.written = 0
	b.flushed = 0
	w.free <- b
	return nil
}

// queueBlock queues the current block for writing to the underlying writer,
// allocates a new block and reserves space for the next header.
func (w *LogWriter) queueBlock() {
	// Allocate a new block, blocking until one is available. We do this first
	// because w.block is protected by w.flusher.Mutex.
	nextBlock := <-w.free

	f := &w.flusher
	f.Lock()
	f.pending = append(f.pending, w.block)
	w.block = nextBlock
	f.ready.Signal()
	w.err = w.flusher.err
	f.Unlock()

	//fmt.Printf("queueBlock blockNumber%d\n", w.blockNumber)
	w.blockNumber++
}

// Close flushes any unwritten data and closes the writer.
func (w *LogWriter) Close() error {
	w.flusher.Lock()
	w.flusher.closed = true
	w.flusher.ready.Signal()
	w.flusher.Unlock()
	if err := w.Sync(); err != nil {
		return err
	}
	//Closing LogWriter do not close the file.
	/*
		if w.c != nil {
			if err := w.c.Close(); err != nil {
				return err
			}
		}
	*/
	w.err = errors.New("pebble/record: closed LogWriter")
	return nil
}

func (w *LogWriter) closed() bool {
	w.flusher.Lock()
	closed := w.flusher.closed
	w.flusher.Unlock()
	return closed
}

// Flush flushes unwritten data. May be called concurrently with Write, Sync
// and itself.
func (w *LogWriter) Flush() error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	return w.flushLocked()
}

func (w *LogWriter) flushLocked() error {
	if w.err != nil {
		if w.closed() {
			return nil
		}
		return w.err
	}

	w.flusher.Lock()
	// Wait for any existing flushing to complete.
	for w.flusher.flushing {
		w.flusher.done.Wait()
	}
	// Block any new flushing from starting.
	w.flusher.flushing = true
	// Grab the list of pending blocks to be flushed.
	pending := w.flusher.pending
	w.flusher.pending = nil
	// Grab the portion of the current block that requires flushing. Note that
	// the current block can be added to the pending blocks list after we release
	// the flusher lock, but it won't be part of pending.
	written := atomic.LoadInt32(&w.block.written)
	data := w.block.buf[w.block.flushed:written]
	w.block.flushed = written
	w.flusher.Unlock()

	// Flush any pending blocks.
	var err error
	for _, t := range pending {
		if err = w.flushBlock(t); err != nil {
			break
		}
	}
	if err == nil && len(data) > 0 {
		//fmt.Printf("writing tail %d\n", len(data))
		_, err = w.w.Write(data)
	}

	// Release the flush loop.
	w.flusher.Lock()
	w.err = err
	w.flusher.err = err
	w.flusher.flushing = false
	w.flusher.done.Signal()
	w.flusher.Unlock()

	if w.f != nil {
		w.err = w.f.Flush()
		return w.err
	}
	return nil
}

// Sync flushes unwritten data and synchronizes the underlying file. May be
// called concurrently with Write, Flush and itself.
func (w *LogWriter) Sync() error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if err := w.flushLocked(); err != nil {
		if w.closed() {
			return nil
		}
		return err
	}

	if w.s != nil {
		w.err = w.s.Sync()
		if w.err != nil {
			if w.closed() {
				return nil
			}
		}
		return w.err
	}
	return nil
}

//ComputeEnd caculate the absolute end of appeding some "length" bytes
func ComputeEnd(start uint32, length uint32) uint32 {
	fix := func(end uint32) uint32 {
		written := end & BlockSizeMask
		if BlockSize-written < HeaderSize {
			return utils.Ceil(end, BlockSize)
		}
		return end
	}

	var middle, last uint32
	written := start & BlockSizeMask
	if length+HeaderSize <= BlockSize-written {
		return fix(start + HeaderSize + length) //full
	}

	n := length - (BlockSize - written - HeaderSize) //remaining data to write
	first := start + BlockSize - written
	middle = (n / (BlockSize - HeaderSize)) * BlockSize
	if (n % (BlockSize - HeaderSize)) > 0 {
		last = ((n % (BlockSize - HeaderSize)) + HeaderSize)
	}
	return fix(first + middle + last)

}

// WriteRecord writes a complete record. Returns the offset just past the end
// of the record.
func (w *LogWriter) WriteRecord(p []byte) (int64, int64, error) {
	if w.err != nil {
		return -1, -1, w.err
	}
	lp := len(p)
	start := w.blockNumber*BlockSize + int64(w.block.written)
	for i := 0; i == 0 || len(p) > 0; i++ {
		p = w.emitFragment(i, p)
	}

	end := w.blockNumber*BlockSize + int64(w.block.written)
	xend := ComputeEnd(uint32(start), uint32(lp))

	if uint32(end) != xend {
		panic(fmt.Sprintf("start %d, len %d, end %d, your end is %d\n", start, lp, end, xend))
	}

	return start, end, w.err
}

func (w *LogWriter) emitFragment(n int, p []byte) []byte {
	b := w.block
	i := b.written
	first := n == 0
	last := BlockSize - i - HeaderSize >= int32(len(p))

	if last {
		if first {
			b.buf[i+8] = fullChunkType
		} else {
			b.buf[i+8] = lastChunkType
		}
	} else {
		if first {
			b.buf[i+8] = firstChunkType
		} else {
			b.buf[i+8] = middleChunkType
		}
	}

	r := copy(b.buf[i+HeaderSize:], p)
	j := i + int32(HeaderSize+r)
	binary.LittleEndian.PutUint32(b.buf[i+0:i+4], utils.NewCRC(b.buf[i+8:j]).Value())
	binary.LittleEndian.PutUint32(b.buf[i+4:i+8], uint32(r))
	atomic.StoreInt32(&b.written, j)

	if BlockSize-b.written < HeaderSize {
		// There is no room for another fragment in the block, so fill the
		// remaining bytes with zeros and queue the block for flushing.
		for i := b.written; i < BlockSize; i++ {
			b.buf[i] = 0
		}
		atomic.StoreInt32(&b.written, j)
		w.queueBlock()
	}
	return p[r:]
}
