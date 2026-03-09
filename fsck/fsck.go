package fsck

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/abh/rrrgo/recent"
)

// Options controls fsck behavior.
type Options struct {
	Repair      bool                                              // Auto-repair issues found
	SkipEvents  bool                                              // Skip event parsing (faster, less thorough)
	Verbose     bool                                              // Detailed output
	Logger      *slog.Logger                                      // Required for all output
	EnqueueFunc func(ctx context.Context, path, typ string) error // If set, repair enqueues rather than calling BatchUpdate directly
}

// Result contains fsck findings.
type Result struct {
	Issues             int            // Total issues found
	IssuesFound        map[string]int // Issues per check type
	Repaired           bool           // Whether repair was attempted
	EpochsQuantized    int            // Number of epochs quantized during repair
	EpochsDeduplicated int            // Number of epoch collisions fixed during repair
}

// Run performs fsck on a Recent collection.
func Run(ctx context.Context, rec *recent.Recent, opts Options) (*Result, error) {
	if opts.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	fsckStart := time.Now()

	opts.Logger.Info("starting fsck",
		"repair", opts.Repair,
		"skip_events", opts.SkipEvents,
		"verbose", opts.Verbose,
	)

	result := &Result{
		IssuesFound: make(map[string]int),
	}

	type check struct {
		name string
		fn   func() int
	}

	checks := []check{
		{"hierarchy", func() int { return checkHierarchy(rec, opts) }},
		{"file_integrity", func() int { return checkFileIntegrity(rec, opts) }},
		{"orphaned_files", func() int { return checkOrphanedFiles(rec, opts) }},
		{"disk_index", func() int { return verifyDiskMatchesIndex(rec, opts) }},
	}

	if !opts.SkipEvents {
		checks = append(checks, check{"index_disk", func() int { return verifyEventsMatchFilesystem(rec, opts) }})
	}

	for _, c := range checks {
		if err := ctx.Err(); err != nil {
			opts.Logger.Info("fsck cancelled", "during", c.name, "elapsed", time.Since(fsckStart))
			return result, fmt.Errorf("fsck cancelled: %w", err)
		}

		start := time.Now()
		result.IssuesFound[c.name] = c.fn()
		opts.Logger.Info("fsck check complete", "check", c.name, "issues", result.IssuesFound[c.name], "elapsed", time.Since(start))
	}

	// Calculate total issues
	for _, count := range result.IssuesFound {
		result.Issues += count
	}

	opts.Logger.Info("fsck checks complete",
		"issues_found", result.Issues,
		"elapsed", time.Since(fsckStart),
	)

	// Repair if requested and issues found
	if result.Issues > 0 && opts.Repair {
		if err := ctx.Err(); err != nil {
			opts.Logger.Info("fsck cancelled before repair", "elapsed", time.Since(fsckStart))
			return result, fmt.Errorf("fsck cancelled: %w", err)
		}

		opts.Logger.Info("attempting to repair issues", "count", result.Issues)

		repairStart := time.Now()
		quantized, deduplicated, err := repairIssues(ctx, rec, opts)
		if err != nil {
			return result, fmt.Errorf("repair failed: %w", err)
		}

		result.Repaired = true
		result.EpochsQuantized = quantized
		result.EpochsDeduplicated = deduplicated
		opts.Logger.Info("repair complete", "elapsed", time.Since(repairStart))
	}

	opts.Logger.Info("fsck complete", "total_elapsed", time.Since(fsckStart))
	return result, nil
}
