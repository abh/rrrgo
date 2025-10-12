package recentfile

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestLockUnlock(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Lock
	if err := rf.Lock(); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Verify locked
	if !rf.Locked() {
		t.Error("Recentfile should be locked")
	}

	// Verify lock directory exists
	lockDir := rf.Rfile() + ".lock"
	if _, err := os.Stat(lockDir); err != nil {
		t.Errorf("Lock directory doesn't exist: %v", err)
	}

	// Verify PID file exists
	pidFile := filepath.Join(lockDir, "process")
	if _, err := os.Stat(pidFile); err != nil {
		t.Errorf("PID file doesn't exist: %v", err)
	}

	// Read PID and verify
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("Read PID file failed: %v", err)
	}
	pidStr := string(data)
	pid, err := strconv.Atoi(pidStr[:len(pidStr)-1])
	if err != nil {
		t.Fatalf("Parse PID failed: %v", err)
	}
	if pid != os.Getpid() {
		t.Errorf("PID in lock = %d, want %d", pid, os.Getpid())
	}

	// Unlock
	if err := rf.Unlock(); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Verify unlocked
	if rf.Locked() {
		t.Error("Recentfile should be unlocked")
	}

	// Verify lock directory removed
	if _, err := os.Stat(lockDir); !os.IsNotExist(err) {
		t.Error("Lock directory still exists after unlock")
	}
}

func TestDoubleLock(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// First lock should succeed
	if err := rf.Lock(); err != nil {
		t.Fatalf("First lock failed: %v", err)
	}
	defer rf.Unlock()

	// Second lock should fail
	if err := rf.Lock(); err == nil {
		t.Error("Second lock should fail")
	}
}

func TestUnlockWithoutLock(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Unlock without lock should fail
	if err := rf.Unlock(); err == nil {
		t.Error("Unlock without lock should fail")
	}
}

func TestConcurrentLocking(t *testing.T) {
	tmpDir := t.TempDir()

	rf1 := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	rf2 := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	// Lock with rf1
	if err := rf1.Lock(); err != nil {
		t.Fatalf("Lock rf1 failed: %v", err)
	}

	// Try to lock with rf2 (should block/timeout)
	rf2.lockTimeout = 100 * time.Millisecond

	errChan := make(chan error, 1)
	go func() {
		errChan <- rf2.Lock()
	}()

	// Wait for timeout
	select {
	case err := <-errChan:
		if err == nil {
			t.Error("rf2.Lock() should timeout")
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("rf2.Lock() didn't timeout")
	}

	// Unlock rf1
	if err := rf1.Unlock(); err != nil {
		t.Fatalf("Unlock rf1 failed: %v", err)
	}

	// Now rf2 should be able to lock
	rf2.lockTimeout = 1 * time.Second
	if err := rf2.Lock(); err != nil {
		t.Errorf("rf2.Lock() after rf1 unlock failed: %v", err)
	}
	rf2.Unlock()
}

func TestStaleLockDetection(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	lockDir := rf.Rfile() + ".lock"

	// Create a stale lock with a non-existent PID
	if err := os.Mkdir(lockDir, 0o755); err != nil {
		t.Fatalf("Create lock dir failed: %v", err)
	}

	// Write a very high PID that's unlikely to exist
	stalePID := 999999999
	pidFile := filepath.Join(lockDir, "process")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(stalePID)+"\n"), 0o644); err != nil {
		t.Fatalf("Write stale PID failed: %v", err)
	}

	// Now try to lock - should detect stale lock and succeed
	if err := rf.Lock(); err != nil {
		t.Fatalf("Lock with stale lock failed: %v", err)
	}

	// Verify we got the lock
	if !rf.Locked() {
		t.Error("Should have acquired lock after removing stale lock")
	}

	// Verify PID is now ours
	data, _ := os.ReadFile(pidFile)
	pidStr := string(data)
	pid, _ := strconv.Atoi(pidStr[:len(pidStr)-1])
	if pid != os.Getpid() {
		t.Errorf("PID = %d, want %d", pid, os.Getpid())
	}

	rf.Unlock()
}

func TestLockWithMissingPIDFile(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	lockDir := rf.Rfile() + ".lock"

	// Create lock directory but no PID file (simulates corruption)
	if err := os.Mkdir(lockDir, 0o755); err != nil {
		t.Fatalf("Create lock dir failed: %v", err)
	}

	// Try to lock - should detect corrupted lock and succeed
	if err := rf.Lock(); err != nil {
		t.Fatalf("Lock with corrupted lock failed: %v", err)
	}

	if !rf.Locked() {
		t.Error("Should have acquired lock after removing corrupted lock")
	}

	rf.Unlock()
}

func TestLockWithInvalidPID(t *testing.T) {
	tmpDir := t.TempDir()

	rf := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)

	lockDir := rf.Rfile() + ".lock"

	// Create lock directory with invalid PID
	if err := os.Mkdir(lockDir, 0o755); err != nil {
		t.Fatalf("Create lock dir failed: %v", err)
	}

	pidFile := filepath.Join(lockDir, "process")
	if err := os.WriteFile(pidFile, []byte("not-a-number\n"), 0o644); err != nil {
		t.Fatalf("Write invalid PID failed: %v", err)
	}

	// Try to lock - should detect invalid lock and succeed
	if err := rf.Lock(); err != nil {
		t.Fatalf("Lock with invalid PID failed: %v", err)
	}

	if !rf.Locked() {
		t.Error("Should have acquired lock after removing invalid lock")
	}

	rf.Unlock()
}

func TestLockTimeout(t *testing.T) {
	tmpDir := t.TempDir()

	rf1 := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	rf2 := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	rf2.lockTimeout = 50 * time.Millisecond

	// Lock with rf1
	if err := rf1.Lock(); err != nil {
		t.Fatalf("Lock rf1 failed: %v", err)
	}
	defer rf1.Unlock()

	// Try to lock with rf2 - should timeout
	start := time.Now()
	err := rf2.Lock()
	elapsed := time.Since(start)

	if err == nil {
		t.Error("rf2.Lock() should timeout")
		rf2.Unlock()
	}

	// Verify timeout happened roughly at the expected time
	if elapsed < 40*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Errorf("Timeout took %v, expected ~50ms", elapsed)
	}
}

func TestLockBackoff(t *testing.T) {
	tmpDir := t.TempDir()

	rf1 := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	rf2 := New(
		WithLocalRoot(tmpDir),
		WithInterval("1h"),
	)
	rf2.lockTimeout = 200 * time.Millisecond

	// Lock with rf1
	if err := rf1.Lock(); err != nil {
		t.Fatalf("Lock rf1 failed: %v", err)
	}

	// Track when rf2 starts trying to acquire lock
	start := time.Now()

	// Try to lock with rf2 in goroutine
	done := make(chan bool)
	go func() {
		rf2.Lock()
		done <- true
	}()

	// Wait a bit, then unlock rf1
	time.Sleep(50 * time.Millisecond)
	rf1.Unlock()

	// rf2 should acquire lock within reasonable time
	select {
	case <-done:
		elapsed := time.Since(start)
		t.Logf("rf2 acquired lock after %v", elapsed)
		rf2.Unlock()
	case <-time.After(500 * time.Millisecond):
		t.Error("rf2 didn't acquire lock after rf1 released")
	}
}

func TestMultipleConcurrentLocks(t *testing.T) {
	tmpDir := t.TempDir()

	const numGoroutines = 10
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			rf := New(
				WithLocalRoot(tmpDir),
				WithInterval("1h"),
			)
			rf.lockTimeout = 2 * time.Second

			if err := rf.Lock(); err != nil {
				// Lock failed, that's ok in this test
				return
			}

			// Hold lock briefly
			time.Sleep(10 * time.Millisecond)

			mu.Lock()
			successCount++
			mu.Unlock()

			rf.Unlock()
		}(i)
	}

	wg.Wait()

	// At least one should have succeeded
	if successCount == 0 {
		t.Error("No goroutine acquired lock")
	}

	t.Logf("%d/%d goroutines successfully acquired lock", successCount, numGoroutines)
}

func TestIsProcessRunning(t *testing.T) {
	// Test with current process (should be running)
	if !isProcessRunning(os.Getpid()) {
		t.Error("Current process should be detected as running")
	}

	// Test with PID 1 (init/systemd on Unix, usually exists)
	// On some systems this might not exist, so we just log
	if isProcessRunning(1) {
		t.Log("PID 1 is running")
	} else {
		t.Log("PID 1 is not running")
	}

	// Test with very high PID (very unlikely to exist)
	if isProcessRunning(999999999) {
		t.Log("PID 999999999 is running (unusual)")
	}
}
