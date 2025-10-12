//go:build windows

package recentfile

import (
	"syscall"
	"unsafe"
)

var (
	kernel32        = syscall.NewLazyDLL("kernel32.dll")
	procOpenProcess = kernel32.NewProc("OpenProcess")
	procCloseHandle = kernel32.NewProc("CloseHandle")
)

const (
	processQueryLimitedInformation = 0x1000
)

// isProcessRunning checks if a process with the given PID is running on Windows.
// Uses OpenProcess to check if we can get a handle to the process.
func isProcessRunning(pid int) bool {
	// Try to open the process
	handle, _, _ := procOpenProcess.Call(
		uintptr(processQueryLimitedInformation),
		uintptr(0),
		uintptr(pid),
	)

	if handle == 0 {
		return false // Process doesn't exist or we can't access it
	}

	// Close the handle
	procCloseHandle.Call(handle)
	return true
}
