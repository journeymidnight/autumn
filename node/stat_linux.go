// +build linux,!s390x,!arm,!386

// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package node

import (
	"fmt"
	"syscall"
)

// GetInfo returns total and free bytes available in a directory, e.g. `/`.
func getDiskInfo(path string) (uint64, uint64, err error) {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(path, &s)
	if err != nil {
		return 0,0, err
	}
	reservedBlocks := s.Bfree - s.Bavail

	total := uint64(s.Frsize) * (s.Blocks - reservedBlocks)
	free := uint64(s.Frsize) * s.Bavail
	// Check for overflows.
	// https://github.com/minio/minio/issues/8035
	// XFS can show wrong values at times error out
	// in such scenarios.
	if free > total {
		return info, fmt.Errorf("detected free space (%d) > total disk space (%d), fs corruption at (%s). please run 'fsck'", free, total, path)
	}

	return total, free, nil
}
