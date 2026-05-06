//go:build windows

package api

func scratchDiskStats(_ string) (freeBytes, totalBytes uint64) {
	return 0, 0
}
