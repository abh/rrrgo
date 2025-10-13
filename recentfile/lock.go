package recentfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Lock acquires an exclusive lock on the recentfile.
// Uses directory-based locking (mkdir is atomic on POSIX systems).
func (rf *Recentfile) Lock() error {
	rf.mu.Lock()
	if rf.locked {
		rf.mu.Unlock()
		return fmt.Errorf("already locked")
	}
	rf.mu.Unlock()

	lockDir := rf.Rfile() + ".lock"
	timeout := rf.lockTimeout
	if timeout == 0 {
		timeout = 600 * time.Second // Default 10 minutes
	}

	start := time.Now()
	sleepDuration := 10 * time.Millisecond

	for {
		// Try to create lock directory
		err := os.Mkdir(lockDir, 0o755)
		if err == nil {
			// Success! We got the lock
			if err := rf.writeLockPID(lockDir); err != nil {
				os.Remove(lockDir)
				return fmt.Errorf("write lock PID: %w", err)
			}

			rf.mu.Lock()
			rf.locked = true
			rf.lockDir = lockDir
			rf.mu.Unlock()

			return nil
		}

		// Lock directory already exists
		if !os.IsExist(err) {
			return fmt.Errorf("mkdir %s: %w", lockDir, err)
		}

		// Check if lock is stale
		if stale, err := rf.checkStaleLock(lockDir); err != nil {
			return fmt.Errorf("check stale lock: %w", err)
		} else if stale {
			// Remove stale lock and try again
			if err := os.RemoveAll(lockDir); err != nil {
				return fmt.Errorf("remove stale lock: %w", err)
			}
			continue
		}

		// Check timeout
		if time.Since(start) > timeout {
			return fmt.Errorf("lock timeout after %v", timeout)
		}

		// Wait and retry
		time.Sleep(sleepDuration)

		// Exponential backoff up to 1 second
		sleepDuration *= 2
		if sleepDuration > time.Second {
			sleepDuration = time.Second
		}
	}
}

// Unlock releases the lock on the recentfile.
func (rf *Recentfile) Unlock() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.locked {
		return fmt.Errorf("not locked")
	}

	// Remove lock directory
	if err := os.RemoveAll(rf.lockDir); err != nil {
		return fmt.Errorf("remove lock directory: %w", err)
	}

	rf.locked = false
	rf.lockDir = ""

	return nil
}

// writeLockPID writes the current process PID to the lock directory.
func (rf *Recentfile) writeLockPID(lockDir string) error {
	pidFile := filepath.Join(lockDir, "process")
	pid := os.Getpid()

	data := []byte(fmt.Sprintf("%d\n", pid))
	if err := os.WriteFile(pidFile, data, 0o644); err != nil {
		return fmt.Errorf("write PID file: %w", err)
	}

	return nil
}

// checkStaleLock checks if the lock is stale (process no longer running).
func (rf *Recentfile) checkStaleLock(lockDir string) (bool, error) {
	pidFile := filepath.Join(lockDir, "process")

	// Read PID from lock directory
	data, err := os.ReadFile(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			// No PID file, consider it stale
			return true, nil
		}
		return false, fmt.Errorf("read PID file: %w", err)
	}

	// Parse PID
	pidStr := string(data)
	if len(pidStr) == 0 {
		// Empty PID file, consider it stale
		return true, nil
	}
	// Remove trailing newline if present
	if pidStr[len(pidStr)-1] == '\n' {
		pidStr = pidStr[:len(pidStr)-1]
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		// Invalid PID, consider it stale
		return true, nil
	}

	// Check if process is running
	return !isProcessRunning(pid), nil
}

// Locked returns true if this recentfile is currently locked.
func (rf *Recentfile) Locked() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.locked
}
