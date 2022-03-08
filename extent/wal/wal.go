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

package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/journeymidnight/autumn/extent/record"
	"github.com/journeymidnight/autumn/extent/storage"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

type Wal struct {
	dir        string
	oldWALs    []string
	currentWAL *storage.SyncingFile
	last       uint64
	writeCh    chan *request
	stopper    *utils.Stopper
	writer     *record.LogWriter
	walOffset  int64
	userSync   func()
	syncing    int32
	gracefull  bool
	syncWg     sync.WaitGroup
}

var maxWalSize = int64(250 << 20) //250MB
var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

//FIXME:extent may use storage.File in the future
func OpenWal(dir string, userSync func()) (*Wal, error) {
	if userSync == nil {
		return nil, errors.New("no userSync functions")
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var oldWals []string
	last := uint64(0)
	for _, file := range files {
		//filter "*.wal"
		if !strings.HasSuffix(file.Name(), ".wal") {
			continue
		}

		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-4], 16, 64)
		if err != nil {
			return nil, err
		}
		if fid <= last {
			return nil, errors.New("duplicated wal")
		}
		last = fid
		//fd, err := storage.Default.Open(filepath.Join(dir, file.Name()))
		//utils.AssertTrue(err == nil)

		oldWals = append(oldWals, filepath.Join(dir, file.Name()))
	}

	//some filesystem can not guarantee readdir is order
	sort.Slice(oldWals, func(i, j int) bool {
		return strings.Compare(oldWals[i], oldWals[j]) < 0
	})

	//create new wal
	last++
	fd, err := os.Create(filepath.Join(dir, fmt.Sprintf("%016x.wal", last)))

	if err != nil {
		return nil, err
	}
	currentWal := storage.NewSyncingFile(fd, storage.SyncingFileOptions{
		BytesPerSync:    10 << 20,
		PreallocateSize: 64 << 20,
	})

	w := &Wal{
		dir:        dir,
		oldWALs:    oldWals,
		currentWAL: currentWal,
		last:       last,
		writeCh:    make(chan *request, 5),
		stopper:    utils.NewStopper(context.Background()),
		writer:     record.NewLogWriter(currentWal, 0, 0),
		walOffset:  0,
		syncing:    0,
		userSync:   userSync,
	}
	w.stopper.RunWorker(w.doWrites)
	return w, nil

}

func (wal *Wal) cleanPendingWal() {
	for _, f := range wal.oldWALs {
		os.Remove(f)
	}
	wal.oldWALs = wal.oldWALs[0:]
}

//rotate will  non-block
func (wal *Wal) rotate() {
	atomic.StoreInt32(&wal.syncing, 1)
	wal.syncWg.Add(1)
	go func() {

		wal.userSync()

		//wait user sync end
		wal.cleanPendingWal()
		atomic.StoreInt32(&wal.syncing, 0)
		wal.syncWg.Done()
	}()
}

//doRequest is a single thread function
func (wal *Wal) doRequest(reqs []*request) error {

	done := func(err error) {
		for _, r := range reqs {
			r.err = err
			r.wg.Done()
		}
	}

	//rotate wal
	if wal.walOffset > maxWalSize && atomic.LoadInt32(&wal.syncing) == 0 {
		//non-block
		wal.oldWALs = append(wal.oldWALs, pathName(wal.dir, wal.currentWAL))
		wal.writer.Close()

		wal.rotate()
		wal.last++
		fd, err := os.Create(filepath.Join(wal.dir, fmt.Sprintf("%016x.wal", wal.last)))
		if err != nil {
			done(err)
			return err
		}
		wal.currentWAL = storage.NewSyncingFile(fd, storage.SyncingFileOptions{
			BytesPerSync:    10 << 20,
			PreallocateSize: 64 << 20,
		})
		wal.writer = record.NewLogWriter(wal.currentWAL, 0, 0)
		/*
			info, _ := fd.Stat()
			fmt.Printf("new file %s\n", info.Name())
		*/
	}

	//normal write
	buf := new(bytes.Buffer)
	for _, req := range reqs {
		buf.Reset()
		req.encodeTo(buf)
		_, end, err := wal.writer.WriteRecord(buf.Bytes())
		if err != nil {
			done(err)
			return err
		}
		wal.walOffset = end
	}
	if err := wal.writer.Sync(); err != nil {
		done(err)
		return err
	}
	done(nil)
	return nil
}

func (wal *Wal) doWrites() {
	pendingCh := make(chan struct{}, 1)
	writeRequests := func(reqs []*request) {
		if err := wal.doRequest(reqs); err != nil {
			xlog.Logger.Errorf(err.Error())
		}
		<-pendingCh
	}

	reqs := make([]*request, 0, 10)
	for {
		var r *request

		select {
		case r = <-wal.writeCh:
		case <-wal.stopper.ShouldStop():
			goto closedCase
		}

		for {
			reqs = append(reqs, r)

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-wal.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-wal.stopper.ShouldStop():
				goto closedCase
			}

		}

	closedCase:
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		for {
			select {
			case r = <-wal.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{} // Push to pending before doing a write.
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
	}
}

//write will block until write is done
//thread-safe
func (wal *Wal) Write(extentID uint64, start uint32, rev int64, blocks [][]byte) error {
	req := requestPool.Get().(*request)
	req.reset()
	req.data = blocks
	req.extentID = extentID
	req.start = start
	req.rev = rev
	req.wg.Add(1)
	wal.writeCh <- req

	req.wg.Wait()
	requestPool.Put(req)
	return req.err
}

func pathName(dir string, f *storage.SyncingFile) string {
	info, _ := f.File.Stat()
	name := info.Name()
	return filepath.Join(dir, name)
}

func (wal *Wal) Close() {
	wal.stopper.Stop()
	wal.syncWg.Wait()
	//wait for currrent syncing stop
	if wal.gracefull {
		wal.oldWALs = append(wal.oldWALs, pathName(wal.dir, wal.currentWAL))
		wal.currentWAL.Close()
		wal.userSync()
		wal.cleanPendingWal()
	} else {
		wal.currentWAL.Close()
	}
}

func (wal *Wal) Replay(callback func(uint64, uint32, int64, [][]byte)) error {
	//delete pendingWals after replay
	for _, fname := range wal.oldWALs {
		f, err := os.Open(fname)
		if err != nil {
			return err
		}
		n := 0
		records := record.NewReader(f)
		for {
			var req request
			rec, err := records.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				xlog.Logger.Warnf("%s read %d record, meet err : recovering: %v", fname, n, err)
				break
			}
			s, err := ioutil.ReadAll(rec)
			if err != nil {
				xlog.Logger.Warnf("%s read %d record, meet err : recovering: %v", fname, n, err)
				break
			}
			req.decode(s)
			callback(req.extentID, req.start, req.rev, req.data)
			n++
		}
	}
	wal.cleanPendingWal()
	return nil
}

type request struct {
	extentID uint64   //encoding
	start    uint32   //encoding
	data     [][]byte //encoding
	rev      int64    //encoding

	//return value
	err error
	wg  sync.WaitGroup
}

func (req *request) reset() {
	req.extentID = 0
	req.start = 0
	req.data = nil
	req.err = nil
	req.rev = 0
	req.wg = sync.WaitGroup{}
}

func (r *request) encodeTo(buf *bytes.Buffer) {
	var enc [binary.MaxVarintLen64]byte
	sz := binary.PutUvarint(enc[:], r.extentID)
	buf.Write(enc[:sz])
	sz = binary.PutUvarint(enc[:], uint64(r.start))
	buf.Write(enc[:sz])
	sz = binary.PutVarint(enc[:], r.rev)
	buf.Write(enc[:sz])
	for _, block := range r.data {
		sz := binary.PutUvarint(enc[:], uint64(len(block)))
		buf.Write(enc[:sz])
		buf.Write(block)
	}
}

func (r *request) decode(buf []byte) {
	var sz int
	var off int
	var start uint64
	r.extentID, sz = binary.Uvarint(buf)
	off += sz
	start, sz = binary.Uvarint(buf[off:])
	r.start = uint32(start)
	off += sz
	r.rev, sz = binary.Varint(buf[off:])
	off += sz
	buf = buf[off:]
	for len(buf) > 0 {
		size, sz := binary.Uvarint(buf)
		r.data = append(r.data,
			buf[sz:sz+int(size)],
		)

		buf = buf[sz+int(size):]
	}
}
