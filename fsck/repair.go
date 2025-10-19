package fsck

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/abh/rrrgo/recent"
	"github.com/abh/rrrgo/recentfile"
)

// repairIssues attempts to fix issues found during validation.
func repairIssues(rec *recent.Recent, opts Options) error {
	// Ensure all files exist
	if opts.Verbose {
		opts.Logger.Debug("ensuring all recentfiles exist")
	}

	if err := rec.EnsureFilesExist(); err != nil {
		return fmt.Errorf("ensure files exist: %w", err)
	}

	if opts.Verbose {
		opts.Logger.Debug("all files ensured")
	}

	// Repair disk→index mismatches (files on disk but not in index)
	if err := repairIndexOrphans(rec, opts); err != nil {
		return err
	}

	// Repair index→disk mismatches (files in index but not on disk)
	if err := repairIndexMismatches(rec, opts); err != nil {
		return err
	}

	return nil
}

// repairIndexOrphans adds files on disk but not in index to the principal RECENT file.
// Disk is considered authoritative.
func repairIndexOrphans(rec *recent.Recent, opts Options) error {
	localRoot := rec.LocalRoot()

	if opts.Verbose {
		opts.Logger.Debug("finding files on disk not in index")
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
				// Track both new and delete events (delete removes from set)
				if event.Type == "new" {
					indexPaths[event.Path] = true
				} else if event.Type == "delete" {
					delete(indexPaths, event.Path)
				}
			}
			return true
		})
		if err != nil {
			return fmt.Errorf("read %s: %w", filepath.Base(rfilePath), err)
		}
	}

	if opts.Verbose {
		opts.Logger.Debug("loaded paths from index", "count", len(indexPaths))
		opts.Logger.Debug("scanning disk for orphaned files")
	}

	// Collect files to add
	var batch []recentfile.BatchItem

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

		// Skip RECENT files themselves
		baseName := filepath.Base(path)
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

		// Check if in index
		if !indexPaths[relPath] {
			// File not in index - add to batch
			if opts.Verbose {
				opts.Logger.Debug("adding file to index", "path", relPath, "mtime", info.ModTime().Unix())
			}

			batch = append(batch, recentfile.BatchItem{
				Path:  relPath,
				Type:  "new",
				Epoch: recentfile.EpochFromTime(info.ModTime()),
			})
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("walk directory: %w", err)
	}

	if len(batch) == 0 {
		if opts.Verbose {
			opts.Logger.Debug("no orphaned files to add")
		}
		return nil
	}

	opts.Logger.Info("adding files to index", "count", len(batch))

	// Add to principal RECENT file
	principal := rec.PrincipalRecentfile()
	if err := principal.BatchUpdate(batch); err != nil {
		return fmt.Errorf("batch update: %w", err)
	}

	opts.Logger.Info("added files to index", "count", len(batch), "file", filepath.Base(principal.Rfile()))

	return nil
}

// repairIndexMismatches adds delete events for files in RECENT but not on disk.
// Disk is considered authoritative - if a file is in the index but not on disk,
// it means the file was deleted and we need to record that in the index.
func repairIndexMismatches(rec *recent.Recent, opts Options) error {
	localRoot := rec.LocalRoot()

	if opts.Verbose {
		opts.Logger.Debug("finding files in index but not on disk")
	}

	// Build set of paths that exist on disk
	diskPaths := make(map[string]bool)

	// Get ignore pattern for RECENT files
	meta := rec.PrincipalRecentfile().Meta()
	filenameRoot := meta.Filenameroot
	serializerSuffix := meta.SerializerSuffix

	// Walk disk to build set of existing files
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
			return nil
		}

		// Normalize path separators
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
			if baseName == filenameRoot+".recent" {
				return nil
			}
			if len(baseName) > len(filenameRoot)+1 && baseName[len(filenameRoot)] == '-' {
				if filepath.Ext(baseName) == serializerSuffix ||
					filepath.Ext(baseName) == ".lock" ||
					filepath.Ext(baseName) == ".new" {
					return nil
				}
			}
		}

		diskPaths[relPath] = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk directory: %w", err)
	}

	if opts.Verbose {
		opts.Logger.Debug("found files on disk", "count", len(diskPaths))
		opts.Logger.Debug("checking index for missing files")
	}

	// Build set of paths from RECENT files and find missing ones
	indexPaths := make(map[string]bool)
	var missingPaths []string

	recentfiles := rec.Recentfiles()
	for _, rf := range recentfiles {
		rfilePath := rf.Rfile()
		_, err := recentfile.StreamEvents(rfilePath, 10000, func(events []recentfile.Event) bool {
			for _, event := range events {
				// Track both new and delete events
				if event.Type == "new" {
					indexPaths[event.Path] = true
				} else if event.Type == "delete" {
					delete(indexPaths, event.Path)
				}
			}
			return true
		})
		if err != nil {
			return fmt.Errorf("read %s: %w", filepath.Base(rfilePath), err)
		}
	}

	// Find files in index but not on disk
	for path := range indexPaths {
		if !diskPaths[path] {
			missingPaths = append(missingPaths, path)
		}
	}

	if len(missingPaths) == 0 {
		if opts.Verbose {
			opts.Logger.Debug("no files in index missing from disk")
		}
		return nil
	}

	opts.Logger.Info("adding delete events for missing files", "count", len(missingPaths))

	// Create batch of delete events
	var batch []recentfile.BatchItem
	now := recentfile.EpochNow()
	for _, path := range missingPaths {
		if opts.Verbose {
			opts.Logger.Debug("marking file as deleted", "path", path)
		}
		batch = append(batch, recentfile.BatchItem{
			Path:  path,
			Type:  "delete",
			Epoch: now,
		})
	}

	// Add to principal RECENT file
	principal := rec.PrincipalRecentfile()
	if err := principal.BatchUpdate(batch); err != nil {
		return fmt.Errorf("batch update: %w", err)
	}

	opts.Logger.Info("added delete events", "count", len(batch), "file", filepath.Base(principal.Rfile()))

	return nil
}
