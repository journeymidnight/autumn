// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/journeymidnight/autumn/utils"
	"github.com/stretchr/testify/require"
)

func short(s string) string {
	if len(s) < 64 {
		return s
	}
	return fmt.Sprintf("%s...(skipping %d bytes)...%s", s[:20], len(s)-40, s[len(s)-20:])
}

// big returns a string of length n, composed of repetitions of partial.
func big(partial string, n int) string {
	return strings.Repeat(partial, n/len(partial)+1)[:n]
}

type recordWriter interface {
	WriteRecord([]byte) (int64, int64, error)
	Close() error
}

func testGeneratorWriter(
	t *testing.T,
	reset func(),
	gen func() (string, bool),
	newWriter func(io.Writer) recordWriter,
) {
	buf := new(bytes.Buffer)

	reset()
	w := newWriter(buf)
	for {
		s, ok := gen()
		if !ok {
			break
		}
		if _, _, err := w.WriteRecord([]byte(s)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reset()
	r := NewReader(buf)
	for {
		s, ok := gen()
		if !ok {
			break
		}
		rr, err := r.Next()
		if err != nil {
			t.Fatalf("reader.Next: %v", err)
		}
		x, err := ioutil.ReadAll(rr)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(x) != s {
			t.Fatalf("got %q, want %q", short(string(x)), short(s))
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("got %v, want %v", err, io.EOF)
	}
}

func testGenerator(t *testing.T, reset func(), gen func() (string, bool)) {
	t.Run("LogWriter", func(t *testing.T) {
		testGeneratorWriter(t, reset, gen, func(w io.Writer) recordWriter {
			return NewLogWriter(w, 0, 0)
		})
	})
}

func testLiterals(t *testing.T, s []string) {
	var i int
	reset := func() {
		i = 0
	}
	gen := func() (string, bool) {
		if i == len(s) {
			return "", false
		}
		i++
		return s[i-1], true
	}
	testGenerator(t, reset, gen)
}

func TestMany(t *testing.T) {
	const n = 1e5
	var i int
	reset := func() {
		i = 0
	}
	gen := func() (string, bool) {
		if i == n {
			return "", false
		}
		i++
		return fmt.Sprintf("%d.", i-1), true
	}
	testGenerator(t, reset, gen)
}

func TestRandom(t *testing.T) {
	const n = 1e2
	var (
		i int
		r *rand.Rand
	)
	reset := func() {
		i, r = 0, rand.New(rand.NewSource(0))
	}
	gen := func() (string, bool) {
		if i == n {
			return "", false
		}
		i++
		return strings.Repeat(string(uint8(i)), r.Intn(2*BlockSize+16)), true
	}
	testGenerator(t, reset, gen)
}

func TestBasic(t *testing.T) {
	testLiterals(t, []string{
		strings.Repeat("a", 1000),
		strings.Repeat("b", 97270),
		strings.Repeat("c", 8000),
	})
}

func TestBoundary(t *testing.T) {
	for i := BlockSize - 16; i < BlockSize+16; i++ {
		s0 := big("abcd", i)
		for j := BlockSize - 16; j < BlockSize+16; j++ {
			s1 := big("ABCDE", j)
			testLiterals(t, []string{s0, s1})
			testLiterals(t, []string{s0, "", s1})
			testLiterals(t, []string{s0, "x", s1})
		}
	}
}

func TestNonExhaustiveRead(t *testing.T) {
	const n = 100
	buf := new(bytes.Buffer)
	p := make([]byte, 10)
	rnd := rand.New(rand.NewSource(1))

	w := NewLogWriter(buf, 0, 0)
	for i := 0; i < n; i++ {
		length := len(p) + rnd.Intn(3*BlockSize)
		s := string(uint8(i)) + "123456789abcdefgh"
		_, _, _ = w.WriteRecord([]byte(big(s, length)))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := NewReader(buf)
	for i := 0; i < n; i++ {
		rr, _ := r.Next()
		_, err := io.ReadFull(rr, p)
		if err != nil {
			t.Fatalf("ReadFull: %v", err)
		}
		want := string(uint8(i)) + "123456789"
		if got := string(p); got != want {
			t.Fatalf("read #%d: got %q want %q", i, got, want)
		}
	}
}

func TestStaleReader(t *testing.T) {
	buf := new(bytes.Buffer)

	w := NewLogWriter(buf, 0, 0)
	if _, _, err := w.WriteRecord([]byte("0")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.WriteRecord([]byte("11")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v\n", err)
	}

	r := NewReader(buf)
	r0, err := r.Next()
	if err != nil {
		t.Fatalf("reader.Next: %v", err)
	}
	r1, err := r.Next()
	if err != nil {
		t.Fatalf("reader.Next: %v", err)
	}
	p := make([]byte, 1)
	if _, err := r0.Read(p); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale read #0: unexpected error: %v", err)
	}
	if _, err := r1.Read(p); err != nil {
		t.Fatalf("fresh read #1: got %v want nil error", err)
	}
	if p[0] != '1' {
		t.Fatalf("fresh read #1: byte contents: got '%c' want '1'", p[0])
	}
}

type testRecords struct {
	records [][]byte // The raw value of each record.
	offsets []int64  // The offset of each record within buf, derived from writer.LastRecordOffset.
	buf     []byte   // The serialized records form of all records.
}

// corruptBlock corrupts the checksum of the record that starts at the
// specified block offset. The number of the block offset is 0 based.
func corruptBlock(buf []byte, blockNum int) {
	// Ensure we always permute at least 1 byte of the checksum.
	if buf[BlockSize*blockNum] == 0x00 {
		buf[BlockSize*blockNum] = 0xff
	} else {
		buf[BlockSize*blockNum] = 0x00
	}

	buf[BlockSize*blockNum+1] = 0x00
	buf[BlockSize*blockNum+2] = 0x00
	buf[BlockSize*blockNum+3] = 0x00
}

func TestReopenLogWriter(t *testing.T) {
	defer os.Remove("test.ext")

	f, err := os.OpenFile("test.ext", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	require.Nil(t, err)

	w := NewLogWriter(f, 0, 0)
	w.WriteRecord([]byte("hello"))
	w.WriteRecord([]byte("world"))
	w.Close()
	f.Close()

	f, err = os.OpenFile("test.ext", os.O_APPEND|os.O_RDWR, 0644)
	require.Nil(t, err)
	st, err := f.Stat()
	bn := (st.Size() / BlockSize)
	offset := uint32(st.Size()) % BlockSize
	fmt.Println(offset)
	fmt.Println(bn)
	w = NewLogWriter(f, bn, int32(offset))

	data := make([]byte, (32 << 10))
	utils.SetRandStringBytes(data)
	start, end, _ := w.WriteRecord(data)
	require.Equal(t, int64(24), start)
	xend := ComputeEnd(uint32(start), uint32(len(data)))

	require.Equal(t, end, int64(xend))

}
func TestBasicReadWrite(t *testing.T) {
	f, err := os.OpenFile("test.ext", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	require.Nil(t, err)
	defer os.Remove("test.ext")

	w := NewLogWriter(f, 0, 0)

	start, end, _ := w.WriteRecord([]byte("hello"))
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(12), end) //len("hello") + 7

	start, end, _ = w.WriteRecord([]byte("world"))
	require.Equal(t, int64(12), start)
	require.Equal(t, int64(24), end) //len("world") + 7 + start

	data := make([]byte, (64 << 10))
	utils.SetRandStringBytes(data)
	start, end, _ = w.WriteRecord(data)

	w.Sync()

	reader := NewReader(f)

	var buf bytes.Buffer

	err = reader.SeekRecord(0)
	require.Nil(t, err)
	//reader.Recover()
	expected := []int{5, 5, 64 << 10}
	for i := 0; i < 3; i++ {
		r, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		_, err = io.Copy(&buf, r)
		require.Equal(t, expected[i], buf.Len())
		buf.Reset()

	}

	f.Seek(0, io.SeekStart)
	reader = NewReader(f)
	buf.Reset()
	reader.SeekRecord(12)
	r, err := reader.Next()
	require.Nil(t, err)
	d, err := ioutil.ReadAll(r)
	require.Nil(t, err)
	require.Equal(t, "world", string(d))
}

// makeTestRecords generates test records of specified lengths.
// The first record will consist of repeating 0x00 bytes, the next record of
// 0x01 bytes, and so forth. The values will loop back to 0x00 after 0xff.
func makeTestRecords(recordLengths ...int) (*testRecords, error) {
	ret := &testRecords{}
	ret.records = make([][]byte, len(recordLengths))
	ret.offsets = make([]int64, len(recordLengths))
	for i, n := range recordLengths {
		ret.records[i] = bytes.Repeat([]byte{byte(i)}, n)
	}

	buf := new(bytes.Buffer)
	w := NewLogWriter(buf, 0, 0)
	for i, rec := range ret.records {
		start, _, err := w.WriteRecord(rec)
		if err != nil {
			return nil, err
		}

		ret.offsets[i] = start
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	ret.buf = buf.Bytes()
	return ret, nil
}
func TestRecoverNoOp(t *testing.T) {
	recs, err := makeTestRecords(
		BlockSize-HeaderSize,
		BlockSize-HeaderSize,
		BlockSize-HeaderSize,
	)
	if err != nil {
		t.Fatalf("makeTestRecords: %v", err)
	}

	r := NewReader(bytes.NewReader(recs.buf))
	_, err = r.Next()
	if err != nil || r.err != nil {
		t.Fatalf("reader.Next: %v reader.err: %v", err, r.err)
	}

	seq, i, j, n := r.seq, r.i, r.j, r.n

	// Should be a no-op since r.err == nil.
	r.Recover()

	// r.err was nil, nothing should have changed.
	if seq != r.seq || i != r.i || j != r.j || n != r.n {
		t.Fatal("reader.Recover when no error existed, was not a no-op")
	}
}

func TestBasicRecover(t *testing.T) {
	recs, err := makeTestRecords(
		BlockSize-HeaderSize,
		BlockSize-HeaderSize,
		BlockSize-HeaderSize,
	)
	if err != nil {
		t.Fatalf("makeTestRecords: %v", err)
	}

	// Corrupt the checksum of the second record r1 in our file.
	corruptBlock(recs.buf, 1)

	underlyingReader := bytes.NewReader(recs.buf)
	r := NewReader(underlyingReader)

	// The first record r0 should be read just fine.
	r0, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	r0Data, err := ioutil.ReadAll(r0)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(r0Data, recs.records[0]) {
		t.Fatal("Unexpected output in r0's data")
	}

	// The next record should have a checksum mismatch.
	_, err = r.Next()
	if err == nil {
		t.Fatal("Expected an error while reading a corrupted record")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("Unexpected error returned: %v", err)
	}

	// Recover from that checksum mismatch.
	r.Recover()
	currentOffset, err := underlyingReader.Seek(0, os.SEEK_CUR)
	if err != nil {
		t.Fatalf("current offset: %v", err)
	}
	if currentOffset != BlockSize*2 {
		t.Fatalf("current offset: got %d, want %d", currentOffset, BlockSize*2)
	}

	// The third record r2 should be read just fine.
	r2, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	r2Data, err := ioutil.ReadAll(r2)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(r2Data, recs.records[2]) {
		t.Fatal("Unexpected output in r2's data")
	}
}

func TestRecoverSingleBlock(t *testing.T) {
	// The first record will be BlockSize * 3 bytes long. Since each block has
	// a 7 byte header, the first record will roll over into 4 blocks.
	recs, err := makeTestRecords(
		BlockSize*3,
		BlockSize-HeaderSize,
		BlockSize/2,
	)
	if err != nil {
		t.Fatalf("makeTestRecords: %v", err)
	}

	// Corrupt the checksum for the portion of the first record that exists in
	// the 4th block.
	corruptBlock(recs.buf, 3)

	// The first record should fail, but only when we read deeper beyond the
	// first block.
	r := NewReader(bytes.NewReader(recs.buf))
	r0, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	// Reading deeper should yield a checksum mismatch.
	_, err = ioutil.ReadAll(r0)
	if err == nil {
		t.Fatal("Expected a checksum mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("Unexpected error returned: %v", err)
	}

	// Recover from that checksum mismatch.
	r.Recover()

	// All of the data in the second record r1 is lost because the first record
	// r0 shared a partial block with it. The second record also overlapped
	// into the block with the third record r2. Recovery should jump to that
	// block, skipping over the end of the second record and start parsing the
	// third record.
	r2, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	r2Data, _ := ioutil.ReadAll(r2)
	if !bytes.Equal(r2Data, recs.records[2]) {
		t.Fatal("Unexpected output in r2's data")
	}
}

func TestRecoverMultipleBlocks(t *testing.T) {
	recs, err := makeTestRecords(
		// The first record will consume 3 entire blocks but a fraction of the 4th.
		BlockSize*3,
		// The second record will completely fill the remainder of the 4th block.
		3*(BlockSize-HeaderSize)-2*BlockSize-2*HeaderSize,
		// Consume the entirety of the 5th block.
		BlockSize-HeaderSize,
		// Consume the entirety of the 6th block.
		BlockSize-HeaderSize,
		// Consume roughly half of the 7th block.
		BlockSize/2,
	)
	if err != nil {
		t.Fatalf("makeTestRecords: %v", err)
	}

	// Corrupt the checksum for the portion of the first record that exists in the 4th block.
	corruptBlock(recs.buf, 3)

	// Now corrupt the two blocks in a row that correspond to recs.records[2:4].
	corruptBlock(recs.buf, 4)
	corruptBlock(recs.buf, 5)

	// The first record should fail, but only when we read deeper beyond the first block.
	r := NewReader(bytes.NewReader(recs.buf))
	r0, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	// Reading deeper should yield a checksum mismatch.
	_, err = ioutil.ReadAll(r0)
	if err == nil {
		t.Fatal("Exptected a checksum mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("Unexpected error returned: %v", err)
	}

	// Recover from that checksum mismatch.
	r.Recover()

	// All of the data in the second record is lost because the first
	// record shared a partial block with it. The following two records
	// have corrupted checksums as well, so the call above to r.Recover
	// should result in r.Next() being a reader to the 5th record.
	r4, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	r4Data, _ := ioutil.ReadAll(r4)
	if !bytes.Equal(r4Data, recs.records[4]) {
		t.Fatal("Unexpected output in r4's data")
	}
}

// verifyLastBlockRecover reads each record from recs expecting that the
// last record will be corrupted. It will then try Recover and verify that EOF
// is returned.
func verifyLastBlockRecover(recs *testRecords) error {
	r := NewReader(bytes.NewReader(recs.buf))
	// Loop to one element larger than the number of records to verify EOF.
	for i := 0; i < len(recs.records)+1; i++ {
		_, err := r.Next()
		switch i {
		case len(recs.records) - 1:
			if err == nil {
				return errors.New("Expected a checksum mismatch error, got nil")
			}
			r.Recover()
		case len(recs.records):
			if err != io.EOF {
				return fmt.Errorf("Expected io.EOF, got %v", err)
			}
		default:
			if err != nil {
				return fmt.Errorf("Next: %v", err)
			}
		}
	}
	return nil
}

func TestRecoverLastPartialBlock(t *testing.T) {
	recs, err := makeTestRecords(
		// The first record will consume 3 entire blocks but a fraction of the 4th.
		BlockSize*3,
		// The second record will completely fill the remainder of the 4th block.
		3*(BlockSize-HeaderSize)-2*BlockSize-2*HeaderSize,
		// Consume roughly half of the 5th block.
		BlockSize/2,
	)
	if err != nil {
		t.Fatalf("makeTestRecords: %v", err)
	}

	// Corrupt the 5th block.
	corruptBlock(recs.buf, 4)

	// Verify Recover works when the last block is corrupted.
	if err := verifyLastBlockRecover(recs); err != nil {
		t.Fatalf("verifyLastBlockRecover: %v", err)
	}
}

func TestRecoverLastCompleteBlock(t *testing.T) {
	recs, err := makeTestRecords(
		// The first record will consume 3 entire blocks but a fraction of the 4th.
		BlockSize*3,
		// The second record will completely fill the remainder of the 4th block.
		3*(BlockSize-HeaderSize)-2*BlockSize-2*HeaderSize,
		// Consume the entire 5th block.
		BlockSize-HeaderSize,
	)
	if err != nil {
		t.Fatalf("makeTestRecords: %v", err)
	}

	// Corrupt the 5th block.
	corruptBlock(recs.buf, 4)

	// Verify Recover works when the last block is corrupted.
	if err := verifyLastBlockRecover(recs); err != nil {
		t.Fatalf("verifyLastBlockRecover: %v", err)
	}
}


func BenchmarkNormalWrite(b *testing.B) {
	for _, size := range []int{8, 16, 32, 64, 128, 1 << 20} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			w := ioutil.Discard
			buf := make([]byte, size)

			b.SetBytes(int64(len(buf)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := w.Write(buf); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
func BenchmarkRecordWrite(b *testing.B) {
	for _, size := range []int{8, 16, 32, 64, 128, 1 << 20} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			w := NewLogWriter(ioutil.Discard, 0, 0)
			defer w.Close()
			buf := make([]byte, size)

			b.SetBytes(int64(len(buf)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := w.WriteRecord(buf); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
