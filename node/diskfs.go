package node

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgryski/go-farm"
	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

const (
	diskLevel = 1
)

type diskFS struct {
	baseDir string
	baseFd  *os.File
}

func OpenDiskFS(dir string) (*diskFS, error) {
	var err error
	s := &diskFS{
		baseDir: dir,
	}
	s.baseFd, err = os.Open(dir) //diretory is readonly
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *diskFS) pathName(ID uint64, level int) string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], ID)
	h := farm.Hash32(buf[:])
	utils.AssertTrue(level <= 4)
	fpath := make([]string, level+2)
	fpath[0] = s.baseDir
	for i := 1; i <= level; i++ {
		n := (h >> (4 - i) * 8) & 0xFF
		fpath[i] = fmt.Sprintf("%02x", n)
	}
	fpath[level-1] = fmt.Sprintf("%d.ext", ID)

	return filepath.Join(fpath...)
}

func (s *diskFS) AllocExtent(ID uint64) (*extent.Extent, error) {
	fpath := s.pathName(ID, diskLevel)
	ex, err := extent.CreateExtent(fpath, ID)
	if err != nil {
		return nil, err
	}
	return ex, nil
}

func (s *diskFS) LoadExtents(callback func(ex *extent.Extent)) {
	//walk all exts files
	filepath.Walk(s.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			xlog.Logger.DPanic(err)
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".ext") {
			return nil
		}
		ex, err := extent.OpenExtent(path)
		if err != nil {
			xlog.Logger.Errorf("failed to open extent %s", path)
			return nil
		}
		callback(ex)
		return nil
	})
}

//FIXME:以后修改到storage.Default
func (s *diskFS) Syncfs() {
	syncfs(s.baseFd.Fd())
}

func (s diskFS) Close() {
	s.baseFd.Close()
}
