package fsck

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/abh/rrrgo/recent"
	"github.com/abh/rrrgo/recentfile"
)

// repairIssues attempts to fix issues found during validation.
// Returns epoch repair statistics: (epochsQuantized, epochsDeduplicated, error)
func repairIssues(rec *recent.Recent, opts Options) (int, int, error) {
	// Ensure all files exist
	if opts.Verbose {
		opts.Logger.Debug("ensuring all recentfiles exist")
	}

	if err := rec.EnsureFilesExist(); err != nil {
		return 0, 0, fmt.Errorf("ensure files exist: %w", err)
	}

	if opts.Verbose {
		opts.Logger.Debug("all files ensured")
	}

	// Repair disk→index mismatches (files on disk but not in index)
	if err := repairIndexOrphans(rec, opts); err != nil {
		return 0, 0, err
	}

	// Repair index→disk mismatches (files in index but not on disk)
	if err := repairIndexMismatches(rec, opts); err != nil {
		return 0, 0, err
	}

	// Repair epochs (quantize to 10µs and deduplicate)
	quantized, deduplicated, err := repairEpochs(rec, opts)
	if err != nil {
		return 0, 0, err
	}

	return quantized, deduplicated, nil
}

// repairEpochs quantizes epochs to 10µs precision and deduplicates collisions.
// Returns statistics about epochs quantized and collisions fixed.
func repairEpochs(rec *recent.Recent, opts Options) (quantized int, deduplicated int, err error) {
	if opts.Verbose {
		opts.Logger.Debug("quantizing and deduplicating epochs in all RECENT files")
	}

	recentfiles := rec.Recentfiles()
	for _, rf := range recentfiles {
		q, d, err := repairEpochsInFile(rf, opts)
		if err != nil {
			return quantized, deduplicated, fmt.Errorf("repair epochs in %s: %w", filepath.Base(rf.Rfile()), err)
		}
		quantized += q
		deduplicated += d

		if opts.Verbose && (q > 0 || d > 0) {
			opts.Logger.Debug("repaired epochs in file",
				"file", filepath.Base(rf.Rfile()),
				"quantized", q,
				"deduplicated", d,
			)
		}
	}

	if quantized > 0 || deduplicated > 0 {
		opts.Logger.Info("epoch repair complete",
			"total_quantized", quantized,
			"total_deduplicated", deduplicated,
		)
	} else if opts.Verbose {
		opts.Logger.Debug("no epochs needed repair")
	}

	return quantized, deduplicated, nil
}

// repairEpochsInFile quantizes and deduplicates epochs in a single recentfile.
func repairEpochsInFile(rf *recentfile.Recentfile, opts Options) (quantized int, deduplicated int, err error) {
	// Read the file
	if err := rf.Read(); err != nil {
		return 0, 0, err
	}

	events := rf.RecentEvents()
	if len(events) == 0 {
		return 0, 0, nil
	}

	// Quantize each epoch to 10µs precision
	for i := range events {
		oldEpoch := events[i].Epoch
		// Quantize to 10-microsecond intervals
		tenMicroUnits := int64(float64(oldEpoch) * 1e5)
		newEpoch := recentfile.Epoch(float64(tenMicroUnits) / 1e5)

		if oldEpoch != newEpoch {
			events[i].Epoch = newEpoch
			quantized++
		}
	}

	// Check for duplicates before deduplication
	seen := make(map[recentfile.Epoch]bool)
	dupCount := 0
	for _, event := range events {
		if seen[event.Epoch] {
			dupCount++
		}
		seen[event.Epoch] = true
	}

	// Deduplicate if necessary
	if dupCount > 0 {
		events = rf.DeduplicateEpochs(events)
		deduplicated = dupCount
	}

	// Only write if we made changes
	if quantized > 0 || deduplicated > 0 {
		// Update the recentfile's events
		rf.SetRecentEvents(events)

		// Write the file back
		if err := rf.Write(); err != nil {
			return quantized, deduplicated, fmt.Errorf("write file: %w", err)
		}
	}

	return quantized, deduplicated, nil
}

// repairIndexOrphans adds files on disk but not in index to the principal RECENT file.
// Disk is considered authoritative.
func repairIndexOrphans(rec *recent.Recent, opts Options) error {
	localRoot := rec.LocalRoot()

	if opts.Verbose {
		opts.Logger.Debug("finding files on disk not in index")
	}

	// Build set of paths that should exist according to index
	indexPaths, err := buildCurrentIndexState(rec)
	if err != nil {
		return fmt.Errorf("build index state: %w", err)
	}

	// Get ignore pattern for RECENT files
	meta := rec.PrincipalRecentfile().Meta()
	filenameRoot := meta.Filenameroot
	serializerSuffix := meta.SerializerSuffix

	if opts.Verbose {
		opts.Logger.Debug("loaded paths from index", "count", len(indexPaths))
		opts.Logger.Debug("scanning disk for orphaned files")
	}

	// Collect files to add
	var batch []recentfile.BatchItem

	err = filepath.Walk(localRoot, func(path string, info os.FileInfo, err error) error {
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

	// Build set of paths that should exist according to index
	indexPaths, err := buildCurrentIndexState(rec)
	if err != nil {
		return fmt.Errorf("build index state: %w", err)
	}

	var missingPaths []string

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
