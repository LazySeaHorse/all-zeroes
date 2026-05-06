//go:build !windows

package api

import "syscall"

func scratchDiskStats(dir string) (freeBytes, totalBytes uint64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, 0
	}
	return stat.Bavail * uint64(stat.Bsize), stat.Blocks * uint64(stat.Bsize)
}
