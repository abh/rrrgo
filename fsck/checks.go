package fsck

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/abh/rrrgo/recent"
	"github.com/abh/rrrgo/recentfile"
)

// checkHierarchy validates the aggregator chain.
func checkHierarchy(rec *recent.Recent, opts Options) int {
	validationErrors := rec.Validate()

	if len(validationErrors) > 0 {
		for _, err := range validationErrors {
			opts.Logger.Warn("hierarchy validation error", "error", err)
		}
		return len(validationErrors)
	}

	if opts.Verbose {
		opts.Logger.Debug("hierarchy is valid")
	}

	return 0
}

// checkFileIntegrity verifies that all recentfiles exist and are readable.
func checkFileIntegrity(rec *recent.Recent, opts Options) int {
	issues := 0

	recentfiles := rec.Recentfiles()
	for i, rf := range recentfiles {
		rfile := rf.Rfile()

		if opts.Verbose {
			opts.Logger.Debug("checking file",
				"index", fmt.Sprintf("[%d/%d]", i+1, len(recentfiles)),
				"file", filepath.Base(rfile),
			)
		}

		// Check file exists
		fi, err := os.Stat(rfile)
		if err != nil {
			if os.IsNotExist(err) {
				opts.Logger.Warn("missing file", "path", rfile)
				issues++
			} else {
				opts.Logger.Warn("cannot stat file", "path", rfile, "error", err)
				issues++
			}
			continue
		}

		// Check file size is reasonable
		if fi.Size() == 0 {
			opts.Logger.Warn("empty file", "path", rfile)
			// Not counted as error, might be intentional
		} else if fi.Size() > 100*1024*1024 { // 100MB
			opts.Logger.Warn("large file", "path", rfile, "size", fi.Size())
		}

		// Check file is readable and parseable
		if opts.SkipEvents {
			// Just check if we can open the file
			f, err := os.Open(rfile)
			if err != nil {
				opts.Logger.Warn("cannot read file", "path", rfile, "error", err)
				issues++
				continue
			}
			f.Close()

			if opts.Verbose {
				opts.Logger.Debug("file ok", "file", filepath.Base(rfile), "size", fi.Size(), "note", "events not parsed")
			}
		} else {
			// Validate the file using streaming (memory-efficient)
			if opts.Verbose && fi.Size() > 10*1024*1024 { // 10MB
				opts.Logger.Debug("parsing large file", "file", filepath.Base(rfile))
			}

			stats, err := recentfile.ValidateFile(rfile)
			if err != nil {
				opts.Logger.Warn("cannot parse file", "path", rfile, "error", err)
				issues++
				continue
			}

			if opts.Verbose {
				opts.Logger.Debug("file ok", "file", filepath.Base(rfile), "size", stats.FileSize, "events", stats.EventCount)
			}
		}
	}

	return issues
}

// checkOrphanedFiles looks for RECENT-*.yaml files that aren't in the hierarchy.
func checkOrphanedFiles(rec *recent.Recent, opts Options) int {
	issues := 0

	localRoot := rec.LocalRoot()

	// Get all expected files
	expectedFiles := make(map[string]bool)
	for _, rf := range rec.Recentfiles() {
		expectedFiles[filepath.Base(rf.Rfile())] = true
	}

	// Scan directory for RECENT-*.yaml files
	entries, err := os.ReadDir(localRoot)
	if err != nil {
		opts.Logger.Warn("cannot read directory", "path", localRoot, "error", err)
		return 1
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		// Check if it's a RECENT file
		if filepath.Ext(name) == ".yaml" && len(name) > 7 && name[:7] == "RECENT-" {
			// Skip lock and new files
			if filepath.Ext(name) == ".lock" || filepath.Ext(name) == ".new" {
				continue
			}

			// Check if it's expected
			if !expectedFiles[name] {
				opts.Logger.Warn("orphaned file", "file", name, "note", "not in hierarchy")
				issues++
			} else if opts.Verbose {
				opts.Logger.Debug("expected file", "file", name)
			}
		}
	}

	return issues
}

// verifyEventsMatchFilesystem checks that files mentioned in RECENT events exist on disk.
func verifyEventsMatchFilesystem(rec *recent.Recent, opts Options) int {
	issues := 0
	localRoot := rec.LocalRoot()

	if opts.Verbose {
		opts.Logger.Debug("streaming through events from all RECENT files")
	}

	// Track statistics across all files
	totalEvents := 0
	checked := 0
	missing := 0
	showedMissing := 0
	maxSample := 1000 // For non-verbose mode, check at most this many

	// Stream through events in batches
	batchSize := 10000
	lastReportedEvents := 0

	// Get all recentfiles
	recentfiles := rec.Recentfiles()

	// Check events from all recentfiles
	for i, rf := range recentfiles {
		rfilePath := rf.Rfile()

		if opts.Verbose {
			opts.Logger.Debug("checking events from file", "file", filepath.Base(rfilePath))
		}

		_, err := recentfile.StreamEvents(rfilePath, batchSize, func(events []recentfile.Event) bool {
			for _, event := range events {
				totalEvents++

				// In non-verbose mode, only check a sample
				if !opts.Verbose && checked >= maxSample {
					continue
				}

				// Skip delete events - those files shouldn't exist
				if event.Type == "delete" {
					continue
				}

				checked++
				fullPath := filepath.Join(localRoot, event.Path)

				// Check if file/symlink exists (Lstat doesn't follow symlinks)
				_, lstErr := os.Lstat(fullPath)
				if lstErr != nil {
					if os.IsNotExist(lstErr) {
						// File truly doesn't exist
						if opts.Verbose || showedMissing < 10 { // Only show first 10 missing files unless verbose
							opts.Logger.Warn("file in RECENT but not on disk", "path", event.Path)
							showedMissing++
						}
						missing++
						issues++
					}
					continue
				}

				// File/symlink exists, check if it's a broken symlink
				_, statErr := os.Stat(fullPath)
				if statErr != nil && os.IsNotExist(statErr) {
					// Broken symlink (symlink exists but target doesn't)
					if opts.Verbose || showedMissing < 10 {
						opts.Logger.Warn("broken symlink in RECENT", "path", event.Path)
						showedMissing++
					}
					// Don't increment issues - broken symlinks are informational only
				}
			}

			// Show progress in verbose mode after each batch
			if opts.Verbose && totalEvents-lastReportedEvents >= 10000 {
				opts.Logger.Debug("progress", "events", totalEvents, "checked", checked, "missing", missing)
				lastReportedEvents = totalEvents
			}

			// Continue streaming
			return true
		})
		if err != nil {
			opts.Logger.Warn("cannot stream file", "file", filepath.Base(rfilePath), "error", err)
			issues++
			continue
		}

		// Show progress after each file in verbose mode
		if opts.Verbose {
			opts.Logger.Debug("finished checking file", "file", filepath.Base(rfilePath))
		}

		// In non-verbose mode, stop after checking maxSample files
		if !opts.Verbose && checked >= maxSample {
			if opts.Verbose {
				opts.Logger.Debug("stopping early", "checked", checked, "files_checked", i+1, "total_files", len(recentfiles))
			}
			break
		}
	}

	// Show final progress if we haven't reported recently
	if opts.Verbose && totalEvents > lastReportedEvents {
		opts.Logger.Debug("final progress", "events", totalEvents, "checked", checked, "missing", missing)
	}

	if !opts.Verbose && totalEvents > maxSample {
		opts.Logger.Info("checked sample", "checked", checked, "total_events", totalEvents)
	}

	if missing > 0 {
		opts.Logger.Info("files in RECENT but not on disk", "missing", missing, "checked", checked)
	} else if opts.Verbose {
		opts.Logger.Debug("all files from events exist on disk", "checked", checked)
	}

	return issues
}

// verifyDiskMatchesIndex checks that files on disk exist in the index.
// Returns number of issues found (files on disk but not in index).
func verifyDiskMatchesIndex(rec *recent.Recent, opts Options) int {
	issues := 0
	localRoot := rec.LocalRoot()

	if opts.Verbose {
		opts.Logger.Debug("scanning files on disk")
	}

	// Build set of paths from RECENT files (streaming to avoid OOM)
	indexPaths := make(map[string]bool)
	recentfiles := rec.Recentfiles()

	// Get ignore pattern for RECENT files
	meta := rec.PrincipalRecentfile().Meta()
	filenameRoot := meta.Filenameroot
	serializerSuffix := meta.SerializerSuffix

	for _, rf := range recentfiles {
		rfilePath := rf.Rfile()
		_, err := recentfile.StreamEvents(rfilePath, 10000, func(events []recentfile.Event) bool {
			for _, event := range events {
				// Only track "new" events (skip deletes)
				if event.Type == "new" {
					indexPaths[event.Path] = true
				}
			}
			return true
		})
		if err != nil {
			opts.Logger.Warn("cannot read file", "file", filepath.Base(rfilePath), "error", err)
		}
	}

	if opts.Verbose {
		opts.Logger.Debug("loaded paths from index", "count", len(indexPaths))
		opts.Logger.Debug("walking directory tree")
	}

	// Walk directory tree and compare
	filesOnDisk := 0
	missingInIndex := 0
	showedMissing := 0

	err := filepath.Walk(localRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip paths we can't access
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get relative path
		relPath, err := filepath.Rel(localRoot, path)
		if err != nil {
			return nil // Skip if we can't get relative path
		}

		// Normalize path separators for cross-platform compatibility
		relPath = filepath.ToSlash(relPath)

		// Skip current and parent directory markers
		if relPath == "." || relPath == ".." {
			return nil
		}

		// Skip temporary files
		baseName := filepath.Base(path)
		if recentfile.ShouldIgnoreFile(baseName) {
			return nil
		}

		// Skip RECENT files themselves
		if len(baseName) >= len(filenameRoot) && baseName[:len(filenameRoot)] == filenameRoot {
			// Check for .recent symlink
			if baseName == filenameRoot+".recent" {
				return nil
			}
			// Check if it's a RECENT file pattern (RECENT-*)
			if len(baseName) > len(filenameRoot)+1 && baseName[len(filenameRoot)] == '-' {
				// Looks like RECENT-*
				if filepath.Ext(baseName) == serializerSuffix ||
					filepath.Ext(baseName) == ".lock" ||
					filepath.Ext(baseName) == ".new" {
					return nil // Skip RECENT files
				}
			}
		}

		filesOnDisk++

		// Check if in index
		if !indexPaths[relPath] {
			missingInIndex++
			issues++

			if opts.Verbose || showedMissing < 10 {
				opts.Logger.Warn("file on disk but not in index", "path", relPath)
				showedMissing++
			}
		}

		// Show progress in verbose mode
		if opts.Verbose && filesOnDisk%10000 == 0 {
			opts.Logger.Debug("progress", "scanned", filesOnDisk, "not_in_index", missingInIndex)
		}

		return nil
	})
	if err != nil {
		opts.Logger.Warn("error walking directory", "error", err)
		return issues
	}

	if opts.Verbose {
		opts.Logger.Debug("scanned files on disk", "count", filesOnDisk)
	}

	if missingInIndex > 0 {
		opts.Logger.Info("files on disk but not in index", "count", missingInIndex)
	} else if opts.Verbose {
		opts.Logger.Debug("all files on disk are in the index", "count", filesOnDisk)
	}

	return issues
}
