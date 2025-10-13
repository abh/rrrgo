package main

import (
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.ntppool.org/common/metricsserver"
	"go.ntppool.org/common/version"
)

func TestServerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Create temporary directory
	tmpDir := t.TempDir()

	// Build the server binary
	binPath := filepath.Join(t.TempDir(), "rrr-server-test")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, output)
	}

	// Start the server
	cmd := exec.Command(binPath,
		tmpDir,
		"--interval", "1h",
		"--aggregator", "6h",
		"--batch-size", "10",
		"--batch-delay", "100ms",
		"--aggregate-interval", "1s",
		"--verbose",
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}

	// Ensure we clean up
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
			cmd.Wait()
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Create some test files
	testFiles := []string{"file1.txt", "file2.txt", "subdir/file3.txt"}

	// Create subdirectory
	subDir := filepath.Join(tmpDir, "subdir")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	for _, name := range testFiles {
		path := filepath.Join(tmpDir, name)
		if err := os.WriteFile(path, []byte("test content"), 0o644); err != nil {
			t.Fatalf("write file %s: %v", name, err)
		}
	}

	// Wait for events to be processed
	time.Sleep(500 * time.Millisecond)

	// Modify a file
	if err := os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("modified"), 0o644); err != nil {
		t.Fatalf("modify file: %v", err)
	}

	// Delete a file
	if err := os.Remove(filepath.Join(tmpDir, "file2.txt")); err != nil {
		t.Fatalf("remove file: %v", err)
	}

	// Wait for more events
	time.Sleep(500 * time.Millisecond)

	// Send SIGTERM to shutdown gracefully
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal SIGTERM: %v", err)
	}

	// Wait for server to exit
	if err := cmd.Wait(); err != nil {
		// Exit code 0 is expected, anything else is an error
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() != 0 {
				t.Fatalf("server exited with code %d", exitErr.ExitCode())
			}
		}
	}

	// Verify RECENT files were created
	recentFiles := []string{
		"RECENT-1h.yaml",
		"RECENT-6h.yaml",
	}

	for _, name := range recentFiles {
		path := filepath.Join(tmpDir, name)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("RECENT file not created: %s", name)
		} else {
			// Check file is not empty
			data, err := os.ReadFile(path)
			if err != nil {
				t.Errorf("read %s: %v", name, err)
			}
			if len(data) == 0 {
				t.Errorf("%s is empty", name)
			}
		}
	}
}

func TestServerInvalidRoot(t *testing.T) {
	// Build the server binary
	binPath := filepath.Join(t.TempDir(), "rrr-server-test")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, output)
	}

	// Try to start with non-existent directory
	cmd := exec.Command(binPath, "/nonexistent/directory")
	output, err := cmd.CombinedOutput()

	if err == nil {
		t.Error("expected error for non-existent directory")
	}

	if len(output) == 0 {
		t.Error("expected error message in output")
	}
}

func TestServerFileOnly(t *testing.T) {
	// Create a file (not directory)
	tmpFile := filepath.Join(t.TempDir(), "file.txt")
	if err := os.WriteFile(tmpFile, []byte("test"), 0o644); err != nil {
		t.Fatalf("create file: %v", err)
	}

	// Build the server binary
	binPath := filepath.Join(t.TempDir(), "rrr-server-test")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, output)
	}

	// Try to start with file instead of directory
	cmd := exec.Command(binPath, tmpFile)
	output, err := cmd.CombinedOutput()

	if err == nil {
		t.Error("expected error for file instead of directory")
	}

	if len(output) == 0 {
		t.Error("expected error message in output")
	}
}

func TestCreateOrLoadRecent(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test logger
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Test creating new collection (default YAML)
	rec, err := createOrLoadRecent(tmpDir, "1h", "yaml", []string{"6h", "1d"}, log)
	if err != nil {
		t.Fatalf("createOrLoadRecent (new): %v", err)
	}

	if rec == nil {
		t.Fatal("createOrLoadRecent returned nil")
	}

	intervals := rec.Intervals()
	if len(intervals) != 3 {
		t.Errorf("expected 3 intervals, got %d", len(intervals))
	}

	// Verify principal file was created
	principalPath := filepath.Join(tmpDir, "RECENT-1h.yaml")
	if _, err := os.Stat(principalPath); err != nil {
		t.Errorf("principal file not created: %v", err)
	}

	// Test loading existing collection
	rec2, err := createOrLoadRecent(tmpDir, "1h", "yaml", []string{"6h", "1d"}, log)
	if err != nil {
		t.Fatalf("createOrLoadRecent (load): %v", err)
	}

	if rec2 == nil {
		t.Fatal("createOrLoadRecent (load) returned nil")
	}

	intervals2 := rec2.Intervals()
	if len(intervals2) != len(intervals) {
		t.Errorf("loaded collection has %d intervals, expected %d", len(intervals2), len(intervals))
	}
}

func TestCreateOrLoadRecentJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test logger
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Test creating new collection with JSON format
	rec, err := createOrLoadRecent(tmpDir, "1h", "json", []string{"6h", "1d"}, log)
	if err != nil {
		t.Fatalf("createOrLoadRecent (new, JSON): %v", err)
	}

	if rec == nil {
		t.Fatal("createOrLoadRecent returned nil")
	}

	intervals := rec.Intervals()
	if len(intervals) != 3 {
		t.Errorf("expected 3 intervals, got %d", len(intervals))
	}

	// Verify principal file was created with .json extension
	principalPath := filepath.Join(tmpDir, "RECENT-1h.json")
	if _, err := os.Stat(principalPath); err != nil {
		t.Errorf("principal JSON file not created: %v", err)
	}

	// Verify aggregator files were also created with .json extension
	aggPath := filepath.Join(tmpDir, "RECENT-6h.json")
	if _, err := os.Stat(aggPath); err != nil {
		t.Errorf("aggregator JSON file not created: %v", err)
	}

	// Test loading existing JSON collection
	rec2, err := createOrLoadRecent(tmpDir, "1h", "json", []string{"6h", "1d"}, log)
	if err != nil {
		t.Fatalf("createOrLoadRecent (load, JSON): %v", err)
	}

	if rec2 == nil {
		t.Fatal("createOrLoadRecent (load) returned nil")
	}

	intervals2 := rec2.Intervals()
	if len(intervals2) != len(intervals) {
		t.Errorf("loaded collection has %d intervals, expected %d", len(intervals2), len(intervals))
	}
}

func TestCreateOrLoadRecentYAMLDefault(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test logger
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Test creating new collection with YAML format (default)
	rec, err := createOrLoadRecent(tmpDir, "1h", "yaml", []string{"6h"}, log)
	if err != nil {
		t.Fatalf("createOrLoadRecent (new, YAML): %v", err)
	}

	if rec == nil {
		t.Fatal("createOrLoadRecent returned nil")
	}

	// Verify principal file was created with .yaml extension
	principalPath := filepath.Join(tmpDir, "RECENT-1h.yaml")
	if _, err := os.Stat(principalPath); err != nil {
		t.Errorf("principal YAML file not created: %v", err)
	}
}

func TestBuildInfoMetric(t *testing.T) {
	// Create a metrics server with custom registry
	metricsSrv := metricsserver.New()

	// Register build_info metric
	version.RegisterMetric("rrr", metricsSrv.Registry())

	// Gather metrics from registry
	metricFamilies, err := metricsSrv.Registry().Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// Check if rrr_build_info metric is present
	found := false
	var buildInfoMetric string
	for _, mf := range metricFamilies {
		if mf.GetName() == "rrr_build_info" {
			found = true
			buildInfoMetric = mf.String()
			break
		}
	}

	if !found {
		t.Error("rrr_build_info metric not found in registry")
	}

	// Verify the metric has expected labels
	expectedLabels := []string{"version", "buildtime", "gittime", "git"}
	for _, label := range expectedLabels {
		if !strings.Contains(buildInfoMetric, label) {
			t.Errorf("rrr_build_info metric missing expected label: %s", label)
		}
	}
}
