# rrrgo - Go Implementation of rrr-server

Go rewrite of the server-side components of File::Rsync::Mirror::Recent.

This is a re-implementation of the original Perl version created by [Andreas König](https://github.com/andk), available at https://repo.or.cz/rersyncrecent.git

## Overview

This is a Go implementation of the `rrr-server` and `rrr-fsck` tools, providing efficient rsync-based mirroring with file system watching capabilities.

## Features

- Cross-platform file system watching (fsnotify)
- YAML and JSON serialization formats
- Compatible with Perl-generated RECENT files
- Efficient batch processing
- Aggregation across multiple time intervals

## Installation

```bash
cd rrrgo
go build ./cmd/rrr-server
go build ./cmd/rrr-fsck
```

## Usage

### rrr-server

Watch a directory tree and continuously update index files:

```bash
./rrr-server /path/to/RECENT.recent
```

Options:
- `-v, --verbose`: Increase verbosity (can be repeated)
- `-h, --help`: Show help

### rrr-fsck

Check consistency between disk and index:

```bash
./rrr-fsck /path/to/RECENT.recent
```

Options:
- `-n, --dry-run`: Show what would be done without making changes
- `--remoteroot=URL`: Fetch missing files from remote
- `-v, --verbose`: Increase verbosity
- `-y, --yes`: Answer yes to all prompts

## Architecture

- `pkg/recentfile`: Core RECENT file handling, serialization, locking
- `pkg/recent`: Collection manager for multiple recentfiles
- `pkg/watcher`: File system watching with fsnotify
- `pkg/rsync`: Rsync command wrapper
- `cmd/rrr-server`: Server daemon
- `cmd/rrr-fsck`: Consistency checker

## Compatibility

The Go implementation reads and writes RECENT files compatible with the Perl version (File::Rsync::Mirror::Recent).

## Development

Run tests:

```bash
go test ./...
```

## License

Same terms as Perl itself.
