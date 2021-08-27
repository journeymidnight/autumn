package node

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/dgryski/go-farm"
	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const (
	diskLevel = 1
)

type diskFS struct {
	baseDir string
	baseFd  *os.File
	diskID uint64
	online uint32  //atomic value
	total uint64
	free uint64
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

	diskIDString, err := ioutil.ReadFile(filepath.Join(dir, "disk_id"))
	if err != nil {
		return nil, errors.Errorf("can not parse file: disk_id %v", err)
	}

	diskID, err := strconv.ParseUint(string(diskIDString), 10, 64)
	if err != nil {
		return nil, err
	}
	s.diskID = diskID
	s.online = 1
	return s, nil
}
//Df() return (total, free, error)
func (s *diskFS) Df() (uint64, uint64 , error){
	total, free,  err := getDiskInfo(s.baseDir)
	if err != nil {
		return 0,0, err
	}
	s.total = total
	s.free = free
	return total, free, err
}

func (s *diskFS) SetOffline() {
	atomic.StoreUint32(&s.online, 0)
	//FIXME: write flag on disk. bad sector or bad file?
}

func (s *diskFS) Online() bool {
	return atomic.LoadUint32(&s.online) == 1
}

func (s *diskFS) pathName(extentID uint64, suffix string) string {
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
	fpath[pathLen-1] = fmt.Sprintf("%d.%s", extentID, suffix)

	return filepath.Join(fpath...)
}

func (s *diskFS) AllocCopyExtent(extentID uint64, ReplaceNodeID uint64) (string, error) {
	utils.AssertTrue(s!=nil)
	fpath := s.pathName(extentID, fmt.Sprintf("%d.copy", ReplaceNodeID))
	return extent.CreateCopyExtent(fpath, extentID)
}

func (s *diskFS) AllocExtent(ID uint64) (*extent.Extent, error) {
	fpath := s.pathName(ID, "ext")
	ex, err := extent.CreateExtent(fpath, ID)
	if err != nil {
		return nil, err
	}
	return ex, nil
}

func (s *diskFS) LoadExtents(normalExt func(string, uint64), copyExt func(string, uint64)) {
	//walk all exts files
	filepath.Walk(s.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			xlog.Logger.Fatal(err)
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(info.Name(), ".ext") {
			normalExt(path, s.diskID)
			return nil
		} else if strings.HasSuffix(info.Name(), ".copy") {
			copyExt(path, s.diskID)
			return nil
		} 
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

func FormatDisk(dir string) (string, error) {
	var err error
	if err = mkHashDir(dir, diskLevel); err != nil {
		return "", err
	}
	//create uuid
	//TODO: use filesystem's UUID
	us := uuid.NewV4().String()
	var f *os.File

	if f, err = os.OpenFile(path.Join(dir, us), os.O_CREATE, 0644); err != nil {
		return "", err
	}
	f.Close()
	return us, nil
}
