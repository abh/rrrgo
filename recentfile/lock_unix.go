//go:build unix || darwin || linux

package recentfile

import (
	"syscall"
)

// isProcessRunning checks if a process with the given PID is running.
// Uses kill(pid, 0) which checks if we can send a signal to the process.
func isProcessRunning(pid int) bool {
	// Send signal 0 (null signal) to check if process exists
	err := syscall.Kill(pid, syscall.Signal(0))
	if err == nil {
		return true // Process exists
	}

	// Check error type
	if err == syscall.ESRCH {
		return false // No such process
	}

	// EPERM means process exists but we don't have permission
	// In this case, consider it running
	return true
}
