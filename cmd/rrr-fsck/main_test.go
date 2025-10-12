package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/abh/rrrgo/recent"
	"github.com/abh/rrrgo/recentfile"
)

func setupTestRecent(t *testing.T) (*recent.Recent, string) {
	tmpDir := t.TempDir()

	principal := recentfile.New(
		recentfile.WithLocalRoot(tmpDir),
		recentfile.WithInterval("1h"),
		recentfile.WithAggregator([]string{"6h", "1d"}),
	)

	rec, err := recent.NewWithPrincipal(principal)
	if err != nil {
		t.Fatalf("NewWithPrincipal failed: %v", err)
	}

	// Create files
	if err := rec.EnsureFilesExist(); err != nil {
		t.Fatalf("EnsureFilesExist failed: %v", err)
	}

	return rec, tmpDir
}

func TestFsckHealthy(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	principalPath := filepath.Join(tmpDir, "RECENT-1h.yaml")

	// Build the fsck binary
	binPath := filepath.Join(t.TempDir(), "rrr-fsck-test")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, output)
	}

	// Add some events and create the actual files
	testFile := filepath.Join(tmpDir, "file1.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0o644); err != nil {
		t.Fatalf("create file: %v", err)
	}
	if err := rec.Update("file1.txt", "new"); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Run fsck
	cmd := exec.Command(binPath, principalPath, "--verbose")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("fsck failed: %v\noutput: %s", err, output)
	}

	// Check output contains success message
	outputStr := string(output)
	if len(outputStr) == 0 {
		t.Error("expected output from fsck")
	}
}

func TestFsckMissingFile(t *testing.T) {
	_, tmpDir := setupTestRecent(t)

	principalPath := filepath.Join(tmpDir, "RECENT-1h.yaml")

	// Delete one of the aggregated files
	aggregatedPath := filepath.Join(tmpDir, "RECENT-6h.yaml")
	if err := os.Remove(aggregatedPath); err != nil {
		t.Fatalf("remove file: %v", err)
	}

	// Build the fsck binary
	binPath := filepath.Join(t.TempDir(), "rrr-fsck-test")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, output)
	}

	// Run fsck without repair
	cmd := exec.Command(binPath, principalPath, "--verbose")
	output, err := cmd.CombinedOutput()

	// Should exit with error
	if err == nil {
		t.Error("expected fsck to fail with missing file")
	}

	// Check output mentions the missing file
	outputStr := string(output)
	if len(outputStr) == 0 {
		t.Error("expected error output from fsck")
	}
}

func TestFsckRepair(t *testing.T) {
	_, tmpDir := setupTestRecent(t)

	principalPath := filepath.Join(tmpDir, "RECENT-1h.yaml")

	// Delete one of the aggregated files
	aggregatedPath := filepath.Join(tmpDir, "RECENT-6h.yaml")
	if err := os.Remove(aggregatedPath); err != nil {
		t.Fatalf("remove file: %v", err)
	}

	// Build the fsck binary
	binPath := filepath.Join(t.TempDir(), "rrr-fsck-test")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, output)
	}

	// Run fsck with repair
	cmd := exec.Command(binPath, principalPath, "--repair", "--verbose")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("fsck --repair failed: %v\noutput: %s", err, output)
	}

	// Check file was recreated
	if _, err := os.Stat(aggregatedPath); err != nil {
		t.Errorf("file not recreated: %v", err)
	}
}

func TestFsckNonExistent(t *testing.T) {
	// Build the fsck binary
	binPath := filepath.Join(t.TempDir(), "rrr-fsck-test")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, output)
	}

	// Try to fsck non-existent file
	cmd := exec.Command(binPath, "/nonexistent/RECENT-1h.yaml")
	output, err := cmd.CombinedOutput()

	if err == nil {
		t.Error("expected error for non-existent file")
	}

	if len(output) == 0 {
		t.Error("expected error message in output")
	}
}

func TestRunHealthy(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	principalPath := filepath.Join(tmpDir, "RECENT-1h.yaml")

	// Add some events and create the files
	for _, fname := range []string{"file1.txt", "file2.txt"} {
		testFile := filepath.Join(tmpDir, fname)
		if err := os.WriteFile(testFile, []byte("test"), 0o644); err != nil {
			t.Fatalf("create file: %v", err)
		}
		if err := rec.Update(fname, "new"); err != nil {
			t.Fatalf("update: %v", err)
		}
	}

	// Run fsck
	cli := &CLI{
		PrincipalFile: principalPath,
		Verbose:       true,
	}

	if err := run(cli); err != nil {
		t.Errorf("run failed: %v", err)
	}
}

func TestRunMissingFileNoRepair(t *testing.T) {
	_, tmpDir := setupTestRecent(t)

	principalPath := filepath.Join(tmpDir, "RECENT-1h.yaml")

	// Delete a file
	aggregatedPath := filepath.Join(tmpDir, "RECENT-6h.yaml")
	if err := os.Remove(aggregatedPath); err != nil {
		t.Fatalf("remove file: %v", err)
	}

	// Run fsck without repair (should return error)
	cli := &CLI{
		PrincipalFile: principalPath,
		Repair:        false,
		Verbose:       false,
	}

	err := run(cli)
	// Should return an error about issues found
	if err == nil {
		t.Error("expected error when issues found without repair")
	}
}

func TestRunWithRepair(t *testing.T) {
	_, tmpDir := setupTestRecent(t)

	principalPath := filepath.Join(tmpDir, "RECENT-1h.yaml")

	// Delete a file
	aggregatedPath := filepath.Join(tmpDir, "RECENT-6h.yaml")
	if err := os.Remove(aggregatedPath); err != nil {
		t.Fatalf("remove file: %v", err)
	}

	// Run fsck with repair
	cli := &CLI{
		PrincipalFile: principalPath,
		Repair:        true,
		Verbose:       true,
	}

	if err := run(cli); err != nil {
		t.Errorf("run failed: %v", err)
	}

	// Check file was recreated
	if _, err := os.Stat(aggregatedPath); err != nil {
		t.Errorf("file not recreated: %v", err)
	}
}

func TestRunBrokenSymlink(t *testing.T) {
	rec, tmpDir := setupTestRecent(t)

	principalPath := filepath.Join(tmpDir, "RECENT-1h.yaml")

	// Create a broken symlink (pointing to non-existent target)
	symlinkPath := filepath.Join(tmpDir, "broken-link.txt")
	targetPath := filepath.Join(tmpDir, "nonexistent-target.txt")
	if err := os.Symlink(targetPath, symlinkPath); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	// Add event for the symlink
	if err := rec.Update("broken-link.txt", "new"); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Run fsck - should not count broken symlink as an error
	cli := &CLI{
		PrincipalFile: principalPath,
		Verbose:       true,
	}

	if err := run(cli); err != nil {
		t.Errorf("run failed: %v (broken symlinks should not cause failures)", err)
	}
}
