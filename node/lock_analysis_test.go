package node

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/utils"
)

// 分析 extent.Lock() 为什么这么慢

// 1. 基准：SafeMutex 本身
func BenchmarkIsolatedSafeMutex(b *testing.B) {
	var s utils.SafeMutex
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Lock()
		s.Unlock()
	}
}

// 2. 嵌入到结构体中的 SafeMutex
type SimpleStruct struct {
	utils.SafeMutex
	ID uint64
}

func BenchmarkEmbeddedSafeMutex(b *testing.B) {
	obj := &SimpleStruct{ID: 123}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.Lock()
		_ = obj.ID
		obj.Unlock()
	}
}

// 3. 类似 Extent 大小的结构体
type ExtentLikeStruct struct {
	utils.SafeMutex
	isSeal       int32
	commitLength uint32
	ID           uint64
	fileName     string
	filePtr      *os.File    // 指针字段
	writerPtr    interface{} // 模拟 *record.LogWriter
	lastRevision int64
}

func BenchmarkExtentLikeStruct(b *testing.B) {
	obj := &ExtentLikeStruct{
		ID:        999,
		fileName:  "test.ext",
		filePtr:   nil,
		writerPtr: nil,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.Lock()
		_ = obj.ID
		obj.Unlock()
	}
}

// 4. 带真实文件句柄的结构体
func BenchmarkExtentLikeStructWithFile(b *testing.B) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "lock_analysis")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	filePath := filepath.Join(tmpdir, "test.dat")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	obj := &ExtentLikeStruct{
		ID:        999,
		fileName:  filePath,
		filePtr:   f,
		writerPtr: nil,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.Lock()
		_ = obj.ID
		obj.Unlock()
	}
}

// 5. 真实的 Extent 对象（但不访问任何字段）
func BenchmarkRealExtentLockOnly(b *testing.B) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "lock_analysis")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	extentPath := filepath.Join(tmpdir, "test.ext")
	ext, err := extent.CreateExtent(extentPath, 99999)
	if err != nil {
		b.Skip("Extent creation failed:", err)
	}
	defer ext.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ext.Lock()
		ext.Unlock()
	}
}

// 6. 真实的 Extent 对象（访问 ID 字段）
func BenchmarkRealExtentLockWithIDAccess(b *testing.B) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "lock_analysis")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	extentPath := filepath.Join(tmpdir, "test.ext")
	ext, err := extent.CreateExtent(extentPath, 99999)
	if err != nil {
		b.Skip("Extent creation failed:", err)
	}
	defer ext.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ext.Lock()
		_ = ext.ID
		ext.Unlock()
	}
}

// 7. 模拟连续多次操作（更接近真实场景）
func BenchmarkExtentMultipleOps(b *testing.B) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "lock_analysis")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	extentPath := filepath.Join(tmpdir, "test.ext")
	ext, err := extent.CreateExtent(extentPath, 99999)
	if err != nil {
		b.Skip("Extent creation failed:", err)
	}
	defer ext.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 第一次操作
		ext.Lock()
		_ = ext.ID
		ext.Unlock()

		// 第二次操作
		ext.Lock()
		_ = ext.ID
		ext.Unlock()

		// 第三次操作
		ext.Lock()
		_ = ext.ID
		ext.Unlock()
	}
}

// 8. 测试 Extent 创建的开销
func BenchmarkExtentCreation(b *testing.B) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "lock_analysis")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extentPath := filepath.Join(tmpdir, fmt.Sprintf("test_%d_%d.ext", i, time.Now().UnixNano()))
		ext, err := extent.CreateExtent(extentPath, uint64(100000+i))
		if err != nil {
			b.Fatal(err)
		}
		ext.Close()
		os.Remove(extentPath)
	}
}

// 9. 测试预先创建的 Extent vs 每次创建
func BenchmarkPreCreatedVsFresh(b *testing.B) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "lock_analysis")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	b.Run("PreCreated", func(b *testing.B) {
		extentPath := filepath.Join(tmpdir, "precreated.ext")
		ext, err := extent.CreateExtent(extentPath, 99999)
		if err != nil {
			b.Fatal(err)
		}
		defer ext.Close()

		// 预热：让所有缓存都加载好
		for i := 0; i < 100; i++ {
			ext.Lock()
			_ = ext.ID
			ext.Unlock()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ext.Lock()
			_ = ext.ID
			ext.Unlock()
		}
	})

	b.Run("FreshEachTime", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			extentPath := filepath.Join(tmpdir, fmt.Sprintf("fresh_%d.ext", i))
			ext, err := extent.CreateExtent(extentPath, uint64(100000+i))
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()

			ext.Lock()
			_ = ext.ID
			ext.Unlock()

			b.StopTimer()
			ext.Close()
			os.Remove(extentPath)
		}
	})
}
