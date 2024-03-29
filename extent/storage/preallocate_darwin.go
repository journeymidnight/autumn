// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build darwin
// +build darwin

package storage

import (
	"syscall"
	"unsafe"
)

func preallocExtend(fd uintptr, offset, length int64) error {
	if err := preallocFixed(fd, offset, length); err != nil {
		return err
	}
	return syscall.Ftruncate(int(fd), offset+length)
}

func preallocFixed(fd uintptr, offset, length int64) error {
	// allocate all requested space or no space at all
	// TODO: allocate contiguous space on disk with F_ALLOCATECONTIG flag
	fstore := &syscall.Fstore_t{
		Flags:   syscall.F_ALLOCATEALL,
		Posmode: syscall.F_PEOFPOSMODE,
		Length:  length}
	p := unsafe.Pointer(fstore)
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(syscall.F_PREALLOCATE), uintptr(p))
	if errno == 0 || errno == syscall.ENOTSUP {
		return nil
	}

	// wrong argument to fallocate syscall
	if errno == syscall.EINVAL {
		// filesystem "st_blocks" are allocated in the units of
		// "Allocation Block Size" (run "diskutil info /" command)
		var stat syscall.Stat_t
		syscall.Fstat(int(fd), &stat)

		// syscall.Statfs_t.Bsize is "optimal transfer block size"
		// and contains matching 4096 value when latest OS X kernel
		// supports 4,096 KB filesystem block size
		var statfs syscall.Statfs_t
		syscall.Fstatfs(int(fd), &statfs)
		blockSize := int64(statfs.Bsize)

		if stat.Blocks*blockSize >= offset+length {
			// enough blocks are already allocated
			return nil
		}
	}
	return errno
}
