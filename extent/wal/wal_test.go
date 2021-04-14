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
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestWalEncode(t *testing.T) {
	x := request{
		extentID: 10,
		start:    3,
		data:     make([]*pb.Block, 2),
	}
	x.data[0] = &pb.Block{make([]byte, 99)}
	x.data[1] = &pb.Block{make([]byte, 567)}

	buf := new(bytes.Buffer)
	x.encodeTo(buf)

	out := buf.Bytes()
	var y request
	y.decode(out)
	require.Equal(t, uint64(10), y.extentID)
	require.Equal(t, uint32(3), y.start)
	require.Equal(t, 2, len(y.data))
	require.Equal(t, 99, len(y.data[0].Data))
	require.Equal(t, 567, len(y.data[1].Data))
}

func init() {
	xlog.InitLog([]string{"wal_test.log"}, zapcore.DebugLevel)
}

func TestWalWrite(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	defer os.RemoveAll(p)

	wal, err := OpenWal(p, func() {
		fmt.Printf("syncing...\n")
		time.Sleep(time.Second)
	})
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		wal.Write(10, 10, []*pb.Block{&pb.Block{Data: make([]byte, 100)}, &pb.Block{Data: make([]byte, 9)}})
	}
	wal.Close()

	wal, err = OpenWal(p, func() {})
	require.Nil(t, err)

	wal.Replay(func(id uint64, start uint32, data []*pb.Block) {
		require.Equal(t, uint32(10), start)
		require.Equal(t, 2, len(data))
	})
}

func TestMultiWal(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	defer os.RemoveAll(p)
	wal, err := OpenWal(p, func() {
		fmt.Printf("syncing...\n")
		//time.Sleep(10 * time.Second)
	})
	maxWalSize = (1 << 20) //for debug
	require.Nil(t, err)
	for i := 0; i < 100; i++ {
		wal.Write(10, 10, []*pb.Block{&pb.Block{Data: make([]byte, 50240)}, &pb.Block{Data: make([]byte, 9)}})
	}

	wal.Close()
	//0000000000000005.wal should be in directory

	files, err := ioutil.ReadDir(p)
	require.Nil(t, err)
	for _, file := range files {
		require.Equal(t, "0000000000000005.wal", file.Name())
	}
}

func BenchmarkRecordWrite(b *testing.B) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(p)

	wal, err := OpenWal(p, func() {
		fmt.Printf("syncing...\n")
		time.Sleep(time.Second)
	})
	for _, size := range []int{8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			buf := make([]byte, size)

			b.SetBytes(int64(len(buf)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := wal.Write(10, 10, []*pb.Block{{Data: buf}})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
