package fsck

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/abh/rrrgo/recent"
	"github.com/abh/rrrgo/recentfile"
)

// shouldSkipManagedFile returns true if the file is a root-level RECENT file
// managed by rrr-server (RECENT-*.yaml, .lock, .new, RECENT.recent).
// Subdirectory RECENT files (e.g., modules/RECENT-*) are mirrored content
// and should NOT be skipped.
func shouldSkipManagedFile(baseName, relPath, filenameRoot, serializerSuffix string) bool {
	if !strings.HasPrefix(baseName, filenameRoot) {
		return false
	}

	inRootDir := filepath.Dir(relPath) == "."

	// Check for .recent symlink (e.g., RECENT.recent)
	if baseName == filenameRoot+".recent" && inRootDir {
		return true
	}

	// Check for RECENT-* pattern
	if len(baseName) > len(filenameRoot)+1 && baseName[len(filenameRoot)] == '-' && inRootDir {
		ext := filepath.Ext(baseName)
		if ext == serializerSuffix || ext == ".lock" || ext == ".new" {
			return true
		}
	}

	return false
}

// buildCurrentIndexState returns paths that should exist on disk according to
// the current state of all RECENT files (where most recent event type is "new").
// This correctly handles files with multiple events by keeping only the most recent.
func buildCurrentIndexState(rec *recent.Recent) (map[string]bool, error) {
	// Build state map of path -> most recent event
	stateMap := make(map[string]recentfile.Event)
	recentfiles := rec.Recentfiles()

	for _, rf := range recentfiles {
		rfilePath := rf.Rfile()
		_, err := recentfile.StreamEvents(rfilePath, 10000, func(events []recentfile.Event) bool {
			for _, event := range events {
				// Keep the event with the highest epoch for each path
				if existing, ok := stateMap[event.Path]; ok {
					if recentfile.EpochGt(event.Epoch, existing.Epoch) {
						stateMap[event.Path] = event
					}
				} else {
					stateMap[event.Path] = event
				}
			}
			return true
		})
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", filepath.Base(rfilePath), err)
		}
	}

	// Build set of paths that should exist (where most recent event is "new")
	indexPaths := make(map[string]bool)
	for path, event := range stateMap {
		if event.Type == "new" {
			indexPaths[path] = true
		}
	}

	return indexPaths, nil
}
