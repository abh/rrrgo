package fsck

import (
	"fmt"
	"log/slog"

	"github.com/abh/rrrgo/recent"
)

// Options controls fsck behavior.
type Options struct {
	Repair     bool          // Auto-repair issues found
	SkipEvents bool          // Skip event parsing (faster, less thorough)
	Verbose    bool          // Detailed output
	Logger     *slog.Logger  // Required for all output
}

// Result contains fsck findings.
type Result struct {
	Issues      int            // Total issues found
	IssuesFound map[string]int // Issues per check type
	Repaired    bool           // Whether repair was attempted
}

// Run performs fsck on a Recent collection.
func Run(rec *recent.Recent, opts Options) (*Result, error) {
	if opts.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	opts.Logger.Info("starting fsck",
		"repair", opts.Repair,
		"skip_events", opts.SkipEvents,
		"verbose", opts.Verbose,
	)

	result := &Result{
		IssuesFound: make(map[string]int),
	}

	// Check hierarchy
	if opts.Verbose {
		opts.Logger.Debug("validating hierarchy")
	}
	result.IssuesFound["hierarchy"] = checkHierarchy(rec, opts)

	// Check file integrity
	if opts.Verbose {
		opts.Logger.Debug("checking file integrity")
	}
	result.IssuesFound["file_integrity"] = checkFileIntegrity(rec, opts)

	// Check for orphaned files
	if opts.Verbose {
		opts.Logger.Debug("checking for orphaned files")
	}
	result.IssuesFound["orphaned_files"] = checkOrphanedFiles(rec, opts)

	// Check disk→index
	if opts.Verbose {
		opts.Logger.Debug("checking for files on disk not in index")
	}
	result.IssuesFound["disk_index"] = verifyDiskMatchesIndex(rec, opts)

	// Check index→disk (unless skipped)
	if !opts.SkipEvents {
		if opts.Verbose {
			opts.Logger.Debug("verifying events match filesystem")
		}
		result.IssuesFound["index_disk"] = verifyEventsMatchFilesystem(rec, opts)
	} else if opts.Verbose {
		opts.Logger.Debug("skipping event-to-filesystem verification")
	}

	// Calculate total issues
	for _, count := range result.IssuesFound {
		result.Issues += count
	}

	opts.Logger.Info("fsck checks complete",
		"issues_found", result.Issues,
		"hierarchy", result.IssuesFound["hierarchy"],
		"file_integrity", result.IssuesFound["file_integrity"],
		"orphaned_files", result.IssuesFound["orphaned_files"],
		"disk_index", result.IssuesFound["disk_index"],
		"index_disk", result.IssuesFound["index_disk"],
	)

	// Repair if requested and issues found
	if result.Issues > 0 && opts.Repair {
		opts.Logger.Info("attempting to repair issues", "count", result.Issues)

		if err := repairIssues(rec, opts); err != nil {
			return result, fmt.Errorf("repair failed: %w", err)
		}

		result.Repaired = true
		opts.Logger.Info("repair complete")
	}

	return result, nil
}
