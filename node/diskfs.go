package node

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dgryski/go-farm"
	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

const (
	diskLevel = 1
)

type diskFS struct {
	baseDir string
	baseFd  *os.File
}

func OpenDiskFS(dir string, nodeID uint64) (*diskFS, error) {
	var err error
	s := &diskFS{
		baseDir: dir,
	}
	s.baseFd, err = os.Open(dir) //diretory is readonly
	if err != nil {
		return nil, err
	}

	idString, err := ioutil.ReadFile(filepath.Join(dir, "node_id"))
	if err != nil {
		return nil, errors.Errorf("can not parse file: node_id %v", err)
	}

	id, err := strconv.ParseUint(string(idString), 10, 64)
	if err != nil {
		return nil, errors.Errorf("can not parse file: node_id %v", err)
	}
	if id != nodeID {
		return nil, errors.Errorf("the node_id on disk is different,%d != %d", id, nodeID)
	}

	return s, nil
}

func (s *diskFS) pathName(extentID uint64) string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], extentID)
	h := farm.Hash32(buf[:])
	pathLen := diskLevel + 2
	fpath := make([]string, pathLen)
	fpath[0] = s.baseDir
	for i := 1; i < pathLen-1; i++ {
		n := (h >> (4 - i) * 8) & 0xFF
		fpath[i] = fmt.Sprintf("%02x", n)
	}
	fpath[pathLen-1] = fmt.Sprintf("%d.ext", extentID)

	return filepath.Join(fpath...)
}

func (s *diskFS) AllocExtent(ID uint64) (*extent.Extent, error) {
	fpath := s.pathName(ID)
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
			xlog.Logger.Fatal(err)
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".ext") {
			return nil
		}
		ex, err := extent.OpenExtent(path)
		if err != nil {
			xlog.Logger.Errorf("failed to open extent %s, %s", path, err)
			return err
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

func mkHashDir(dir string, level int) error {
	if level == 0 {
		return nil
	}
	dirs := make([]string, 256)
	for i := 0; i < 256; i++ {
		dirName := filepath.Join(dir, fmt.Sprintf("%02x", i))
		if err := os.Mkdir(dirName, 0755); err != nil {
			return err
		}
		dirs[i] = dirName
	}

	for _, d := range dirs {
		if err := mkHashDir(d, level-1); err != nil {
			return err
		}
	}
	return nil
}

func FormatDisk(dir string) error {
	if err := mkHashDir(dir, diskLevel); err != nil {
		return err
	}
	return nil
}