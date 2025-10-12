package recentfile

import "strings"

// ShouldIgnoreFile returns true if the given filename should be ignored
// during filesystem operations (watching, fsck, etc.).
// This includes temporary files created during atomic writes and symlink operations.
func ShouldIgnoreFile(basename string) bool {
	// Ignore .FRMRecent temporary files
	if strings.HasPrefix(basename, ".FRMRecent") {
		return true
	}

	// Ignore .tmp files
	if len(basename) >= 4 && basename[len(basename)-4:] == ".tmp" {
		return true
	}

	return false
}
