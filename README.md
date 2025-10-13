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

### Docker

Pre-built Docker images are available at https://github.com/abh/rrrgo/pkgs/container/rrrgo

```bash
# Latest development build
docker pull ghcr.io/abh/rrrgo:main

# Latest release build
docker pull ghcr.io/abh/rrrgo:latest
```

## Usage

### rrr-server

Watch a directory tree and continuously update index files:

```bash
./rrr-server <local-root>
```

Arguments:
- `<local-root>`: Local root directory to watch

Options:
- `-i, --interval`: Principal recentfile interval (default: "1h", e.g., 30m, 1h, 6h)
- `-a, --aggregator`: Aggregator intervals (e.g., 6h,1d,1W). Can be specified multiple times
- `-f, --format`: Serialization format - yaml or json (default: "yaml")
- `--batch-size`: Maximum batch size before flushing events (default: 1000)
- `--batch-delay`: Maximum delay before flushing events (default: 1s)
- `--aggregate-interval`: How often to run aggregation (default: 5m)
- `--metrics-port`: Port for metrics server (default: 9090)
- `--log-level`: Log level - debug, info, warn, error (default: "info")
- `--skip-fsck`: Skip startup integrity check
- `--fsck-repair`: Auto-repair issues found during startup fsck
- `-v, --verbose`: Enable verbose logging
- `-V, --version`: Show version
- `-h, --help`: Show help

### rrr-fsck

Check consistency between disk and index:

```bash
./rrr-fsck <principal-file>
```

Arguments:
- `<principal-file>`: Path to principal RECENT file (e.g., RECENT-1h.yaml)

Options:
- `-r, --repair`: Repair issues found (otherwise just report)
- `--skip-events`: Skip parsing events (faster, less thorough)
- `-v, --verbose`: Enable verbose logging
- `-V, --version`: Show version
- `-h, --help`: Show help

## Architecture

- `recentfile/`: Core RECENT file handling, serialization, locking
- `recent/`: Collection manager for multiple recentfiles
- `watcher/`: File system watching with fsnotify
- `fsck/`: Consistency checking functionality
- `cmd/rrr-server/`: Server daemon
- `cmd/rrr-fsck/`: Consistency checker tool

## Compatibility

The Go implementation reads and writes RECENT files compatible with the Perl version (File::Rsync::Mirror::Recent).

## Development

Run tests:

```bash
go test ./...
```

## License

Same terms as Perl itself.
