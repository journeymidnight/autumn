// +build linux

package node

import "syscall"

func syncfs(fd uintptr) error {
	_, _, err := syscall.Syscall(306, uintptr(fd), 0, 0)
	if err != 0 {
		return err
	}
	return nil
}
