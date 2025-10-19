package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"
	"go.ntppool.org/common/version"

	"github.com/abh/rrrgo/fsck"
	"github.com/abh/rrrgo/recent"
)

// CLI defines the command-line interface for rrr-fsck.
type CLI struct {
	PrincipalFile string `arg:"" help:"Path to principal RECENT file (e.g., RECENT-1h.yaml)." type:"path"`

	Repair     bool `short:"r" help:"Repair issues found (otherwise just report)."`
	SkipEvents bool `help:"Skip parsing events (faster, less thorough)."`
	Verbose    bool `short:"v" help:"Enable verbose logging."`

	Version kong.VersionFlag `short:"V" help:"Show version."`
}

func main() {
	var cli CLI

	ctx := kong.Parse(&cli,
		kong.Name("rrr-fsck"),
		kong.Description("Verify and repair RECENT file integrity"),
		kong.UsageOnError(),
		kong.Vars{"version": version.Version()},
	)

	if err := run(&cli); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		ctx.Exit(1)
	}
}

func run(cli *CLI) error {
	// Resolve absolute path
	principalPath, err := filepath.Abs(cli.PrincipalFile)
	if err != nil {
		return fmt.Errorf("resolve principal path: %w", err)
	}

	// Check file exists
	if _, err := os.Stat(principalPath); err != nil {
		return fmt.Errorf("principal file not found: %w", err)
	}

	// Create logger for CLI output
	logLevel := slog.LevelInfo
	if cli.Verbose {
		logLevel = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	if cli.Verbose {
		fmt.Printf("Checking RECENT collection: %s\n", principalPath)
	}

	// Load Recent collection (metadata only, not all events)
	rec, err := recent.New(principalPath)
	if err != nil {
		return fmt.Errorf("load recent: %w", err)
	}

	if cli.Verbose {
		fmt.Printf("Loaded: %s\n", rec.String())
	}

	// Run fsck
	result, err := fsck.Run(rec, fsck.Options{
		Repair:     cli.Repair,
		SkipEvents: cli.SkipEvents,
		Verbose:    cli.Verbose,
		Logger:     logger,
	})
	if err != nil {
		return fmt.Errorf("fsck failed: %w", err)
	}

	// Print summary
	fmt.Println("\n=== Summary ===")
	stats := rec.Stats()
	fmt.Printf("Intervals: %d\n", stats.Intervals)
	fmt.Printf("Total events: %d\n", stats.TotalEvents)

	fmt.Println("\nPer-interval statistics:")
	for interval, fs := range stats.Files {
		fmt.Printf("  %s: %d events, %d bytes", interval, fs.Events, fs.Size)
		if fs.Mtime > 0 {
			fmt.Printf(", modified: %d", fs.Mtime)
		}
		fmt.Println()
	}

	// Report issues
	fmt.Printf("\nIssues found: %d\n", result.Issues)

	if result.Issues > 0 {
		if cli.Repair {
			if result.Repaired {
				fmt.Println("✓ Repair complete")
				if result.EpochsQuantized > 0 || result.EpochsDeduplicated > 0 {
					fmt.Println("\nEpoch repairs:")
					if result.EpochsQuantized > 0 {
						fmt.Printf("  • Quantized %d epochs to 10µs precision\n", result.EpochsQuantized)
					}
					if result.EpochsDeduplicated > 0 {
						fmt.Printf("  • Fixed %d epoch collisions\n", result.EpochsDeduplicated)
					}
				}
			} else {
				return fmt.Errorf("repair was requested but not completed")
			}
		} else {
			fmt.Println("\nTo fix issues:")
			fmt.Println("  • Files on disk but not in index: --repair will add them to the index")
			fmt.Println("  • Files in index but not on disk:")
			fmt.Println("      - If syncing from remote: run 'rsync -av REMOTE/ LOCAL/' first")
			fmt.Println("      - If disk is authoritative: --repair will mark them as deleted")
			return fmt.Errorf("found %d issues", result.Issues)
		}
	} else {
		fmt.Println("✓ No issues found")
	}

	return nil
}
