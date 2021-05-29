package utils

import (
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

/*
var (
	hashPool = sync.Pool{
		New: func() interface{} {
			return adler32.New()
		},
	}
)
*/

func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		panic(fmt.Sprintf("%+v", errors.Errorf("Assert failed")))
	}
}

func EqualUint32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func SetRandStringBytes(data []byte) {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for i := range data {
		data[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
}

/*
func AdlerCheckSum(data []byte) uint32 {
	hash := adler32.New()
	hash.Write(data)
	return hash.Sum32()
}
*/

func Check(err error) {
	if err != nil {
		xlog.Logger.Fatalf("%+v", errors.Wrap(err, ""))
	}
}

func HumanReadableThroughput(t float64) string {
	if t < 0 || t < 1e-9 { //if t <=0 , return ""
		return ""
	}
	units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	power := int(math.Log10(t) / 3)
	if power >= len(units) {
		return ""
	}

	return fmt.Sprintf("%.2f%s/sec", t/math.Pow(1000, float64(power)), units[power])
}

func SplitAndTrim(s string, sep string) []string {
	parts := strings.Split(s, sep)
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func Ceil(size uint32, align uint32) uint32 {
	AssertTrue(align&(align-1)==0)
	//fallback to? return (size + align - 1) / align * align
	mask := align - 1
	return (size + mask) & ^mask
}

func Floor(size uint32, align uint32) uint32 {
	AssertTrue(align&(align-1)==0)
	mask := align - 1
	//fallback to? return size / align * align?
	return size & (^mask)
}

func MustMarshal(msg proto.Message) []byte {
	data, err := proto.Marshal(msg)
	Check(err)
	return data
}

func MustUnMarshal(data []byte, msg proto.Message) {
	Check(proto.Unmarshal(data, msg))
}

func SizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// RandomTicker is similar to time.Ticker but ticks at random intervals between
// the min and max duration values (stored internally as int64 nanosecond
// counts).
type RandomTicker struct {
	C     chan time.Time
	stopc chan chan struct{}
	min   int64
	max   int64
}


func init() {
	rand.Seed(time.Now().UnixNano()) //set global rand seed
}

// NewRandomTicker returns a pointer to an initialized instance of the
// RandomTicker. Min and max are durations of the shortest and longest allowed
// ticks. Ticker will run in a goroutine until explicitly stopped.
func NewRandomTicker(min, max time.Duration) *RandomTicker {
	rt := &RandomTicker{
		C:     make(chan time.Time),
		stopc: make(chan chan struct{}),
		min:   min.Nanoseconds(),
		max:   max.Nanoseconds(),
	}
	go rt.loop()
	return rt
}

// Stop terminates the ticker goroutine and closes the C channel.
func (rt *RandomTicker) Stop() {
	c := make(chan struct{})
	rt.stopc <- c
	<-c
}

func (rt *RandomTicker) loop() {
	defer close(rt.C)
	t := time.NewTimer(rt.nextInterval())
	for {
		// either a stop signal or a timeout
		select {
		case c := <-rt.stopc:
			t.Stop()
			close(c)
			return
		case <-t.C:
			select {
			case rt.C <- time.Now():
				t.Stop()
				t = time.NewTimer(rt.nextInterval())
			default:
				// there could be noone receiving...
			}
		}
	}
}

func (rt *RandomTicker) nextInterval() time.Duration {
	interval := rand.Int63n(rt.max-rt.min) + rt.min
	return time.Duration(interval) * time.Nanosecond
}


//thread-safe rand
type LockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func (r *LockedSource) Int63() int64 {
	r.lk.Lock()
	defer r.lk.Unlock()
	return r.src.Int63()
}

func (r *LockedSource) Seed(seed int64) {
	r.lk.Lock()
	defer r.lk.Unlock()
	r.src.Seed(seed)
}

var table = crc32.MakeTable(crc32.Castagnoli)

type CRC uint32

func NewCRC(b []byte) CRC {
	return CRC(0).Update(b)
}

func (c CRC) Update(b []byte) CRC {
	return CRC(crc32.Update(uint32(c), table, b))
}

func (c CRC) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}


func SizeOfBlocks(blocks []*pb.Block) uint32 {
	ret := uint32(0)
	for i := range blocks {
		ret += uint32(len(blocks[i].Data))
	}
	return ret
}

//struct memory is for test
type memory struct {
	vec  []byte
	pos  int64
	end  int64
	size int64
}

func NewMemory(size int) *memory {
	return &memory{
		vec:  make([]byte, size),
		pos:  0,
		end:  0,
		size: int64(size),
	}
}

func (f *memory) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekStart:
		f.pos = offset
	case io.SeekEnd:
		f.pos -= offset
	default:
		return 0, errors.New("bytes.Reader.Seek: only support SeekCurrent")
	}
	return int64(f.pos), nil
}

func (f *memory) Read(buf []byte) (n int, err error) {
	if f.pos >= f.end {
		return 0, io.EOF
	}
	n = copy(buf, f.vec[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *memory) Write(p []byte) (n int, err error) {
	if f.pos >= f.size {
		return -1, io.ErrShortBuffer
	}
	d := Min(len(p), int(f.size-f.end))
	n = copy(f.vec[f.pos:], p[:d])
	f.pos += int64(n)
	if f.pos > f.end {
		f.end = f.pos
	}
	return n, nil
}