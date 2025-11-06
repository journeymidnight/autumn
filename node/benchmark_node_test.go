package node

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/extent"
)

// BenchmarkAppendWithWal_BySize 测试不同大小数据块的 AppendWithWal 性能
func BenchmarkAppendWithWal_BySize(b *testing.B) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "node_append_wal")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	node := &ExtentNode{nodeID: 1}
	testSizes := []int{1024, 4096, 16384} // 1KB, 4KB, 16KB

	for _, size := range testSizes {
		sizeName := formatSize(size)
		b.Run(sizeName, func(b *testing.B) {
			// 为每次运行创建唯一的文件名
			extentPath := filepath.Join(tmpdir, fmt.Sprintf("test_%s_%d.ext", sizeName, time.Now().UnixNano()))
			// 确保文件不存在
			os.Remove(extentPath)

			extentID := uint64(100 + size)
			ext, err := extent.CreateExtent(extentPath, extentID)
			if err != nil {
				b.Skip("Extent creation failed:", err)
			}
			defer ext.Close()

			data := make([][]byte, 1)
			data[0] = make([]byte, size)
			for i := range data[0] {
				data[0][i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			// 使用Go benchmark标准方式，让框架自动处理
			b.SetBytes(int64(size))

			for i := 0; i < b.N; i++ {
				ext.Lock()
				// 使用 mustSync=false 测试纯append性能（不使用WAL）
				_, _, err := node.AppendWithWal(ext, int64(i), data, false)
				ext.Unlock()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAppendWithWal_SyncMode 测试同步vs异步写入的性能差异
func BenchmarkAppendWithWal_SyncMode(b *testing.B) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "wal_sync_test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	node := &ExtentNode{nodeID: 2}
	size := 4096
	data := make([][]byte, 1)
	data[0] = make([]byte, size)
	for i := range data[0] {
		data[0][i] = byte(i % 256)
	}

	b.Run("NoSync", func(b *testing.B) {
		extentPath := filepath.Join(tmpdir, fmt.Sprintf("nosync_%d.ext", time.Now().UnixNano()))
		os.Remove(extentPath)
		ext, err := extent.CreateExtent(extentPath, 100)
		if err != nil {
			b.Skip("Extent creation failed:", err)
		}
		defer ext.Close()

		b.ResetTimer()
		b.ReportAllocs()

		maxOps := 100
		actualOps := b.N
		if actualOps > maxOps {
			actualOps = maxOps
		}

		for i := 0; i < actualOps; i++ {
			ext.Lock()
			node.AppendWithWal(ext, 0, data, false)
			ext.Unlock()
		}
	})

	b.Run("WithSync", func(b *testing.B) {
		extentPath := filepath.Join(tmpdir, fmt.Sprintf("withsync_%d.ext", time.Now().UnixNano()))
		os.Remove(extentPath)
		ext, err := extent.CreateExtent(extentPath, 101)
		if err != nil {
			b.Skip("Extent creation failed:", err)
		}
		defer ext.Close()

		b.ResetTimer()
		b.ReportAllocs()

		maxOps := 100
		actualOps := b.N
		if actualOps > maxOps {
			actualOps = maxOps
		}

		for i := 0; i < actualOps; i++ {
			ext.Lock()
			node.AppendWithWal(ext, 0, data, true)
			ext.Unlock()
		}
	})
}

func formatSize(bytes int) string {
	if bytes >= 1024*1024 {
		return fmt.Sprintf("%dMB", bytes/(1024*1024))
	} else if bytes >= 1024 {
		return fmt.Sprintf("%dKB", bytes/1024)
	}
	return fmt.Sprintf("%dB", bytes)
}
