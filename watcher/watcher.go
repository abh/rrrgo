package watcher

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/abh/rrrgo/recent"
	"github.com/abh/rrrgo/recentfile"
)

// Watcher monitors a directory tree for changes and updates a Recent collection.
type Watcher struct {
	// fsnotify watcher
	fsw *fsnotify.Watcher

	// Recent collection to update
	recent *recent.Recent

	// Root directory being watched
	rootDir string

	// Pattern to ignore (RECENT files)
	ignoredRx *regexp.Regexp

	// Batch processing
	batchChan   chan batchItem
	batchSize   int           // Max batch size before flush
	batchDelay  time.Duration // Max delay before flush
	batch       []recentfile.BatchItem
	batchMu     sync.Mutex
	lastFlush   time.Time
	lastFlushMu sync.Mutex

	// Aggregation
	aggregateInterval time.Duration // How often to run aggregation (0 = disabled)

	// Context for shutdown
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	runMu   sync.RWMutex

	// Verbose logging
	verbose bool

	// Error callback
	errorHandler func(error)

	// Event callback - called after successful batch processing
	// Arguments: eventType ("new" or "delete"), count
	eventCallback func(eventType string, count int)

	// Aggregation callback - called after successful aggregation
	// Argument: duration of aggregation
	aggregationCallback func(duration time.Duration)
}

// batchItem is an internal item in the batch channel.
type batchItem struct {
	path string
	typ  string
}

// Option is a functional option for configuring the Watcher.
type Option func(*Watcher)

// WithBatchSize sets the maximum batch size before flushing.
func WithBatchSize(size int) Option {
	return func(w *Watcher) {
		w.batchSize = size
	}
}

// WithBatchDelay sets the maximum delay before flushing.
func WithBatchDelay(delay time.Duration) Option {
	return func(w *Watcher) {
		w.batchDelay = delay
	}
}

// WithVerbose enables verbose logging.
func WithVerbose(v bool) Option {
	return func(w *Watcher) {
		w.verbose = v
	}
}

// WithErrorHandler sets a callback for handling errors.
func WithErrorHandler(handler func(error)) Option {
	return func(w *Watcher) {
		w.errorHandler = handler
	}
}

// WithEventCallback sets a callback for tracking processed events.
// The callback is called after each successful batch flush with the event type and count.
func WithEventCallback(callback func(eventType string, count int)) Option {
	return func(w *Watcher) {
		w.eventCallback = callback
	}
}

// WithAggregateInterval sets the interval for periodic aggregation.
// If set to 0, aggregation is disabled.
func WithAggregateInterval(interval time.Duration) Option {
	return func(w *Watcher) {
		w.aggregateInterval = interval
	}
}

// WithAggregationCallback sets a callback for tracking aggregation runs.
// The callback is called after each successful aggregation with the duration.
func WithAggregationCallback(callback func(duration time.Duration)) Option {
	return func(w *Watcher) {
		w.aggregationCallback = callback
	}
}

// New creates a new file system watcher for the given Recent collection.
func New(rec *recent.Recent, opts ...Option) (*Watcher, error) {
	if rec == nil {
		return nil, fmt.Errorf("recent collection cannot be nil")
	}

	// Create fsnotify watcher
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create fsnotify watcher: %w", err)
	}

	// Create context
	ctx, cancel := context.WithCancel(context.Background())

	// Build ignore regex for RECENT files
	meta := rec.PrincipalRecentfile().Meta()
	pattern := fmt.Sprintf(`^%s(-[0-9]*[smhdWMQYZ]%s(\.lock(/.*)?|\.new)?|\.recent)$`,
		regexp.QuoteMeta(meta.Filenameroot),
		regexp.QuoteMeta(meta.SerializerSuffix))
	ignoredRx := regexp.MustCompile(pattern)

	w := &Watcher{
		fsw:          fsw,
		recent:       rec,
		rootDir:      rec.LocalRoot(),
		ignoredRx:    ignoredRx,
		batchChan:    make(chan batchItem, 100000),
		batchSize:    1000,
		batchDelay:   1 * time.Second,
		ctx:          ctx,
		cancel:       cancel,
		lastFlush:    time.Now(),
		errorHandler: func(err error) { fmt.Fprintf(os.Stderr, "watcher error: %v\n", err) },
	}

	// Apply options
	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

// Start begins watching the filesystem.
func (w *Watcher) Start() error {
	w.runMu.Lock()
	if w.running {
		w.runMu.Unlock()
		return fmt.Errorf("watcher already running")
	}
	w.running = true
	w.runMu.Unlock()

	// Watch the entire directory tree
	if err := w.watchTree(w.rootDir); err != nil {
		w.runMu.Lock()
		w.running = false
		w.runMu.Unlock()
		return fmt.Errorf("watch tree: %w", err)
	}

	// Start event handler
	w.wg.Add(1)
	go w.eventLoop()

	// Start batch processor
	w.wg.Add(1)
	go w.batchProcessor()

	return nil
}

// Stop stops the watcher gracefully.
func (w *Watcher) Stop() error {
	w.runMu.Lock()
	if !w.running {
		w.runMu.Unlock()
		return nil // Already stopped
	}
	w.runMu.Unlock()

	// Signal shutdown
	w.cancel()

	// Close fsnotify watcher (will cause eventLoop to exit)
	if err := w.fsw.Close(); err != nil {
		return fmt.Errorf("close fsnotify: %w", err)
	}

	// Wait for goroutines to finish
	w.wg.Wait()

	// Flush any remaining events
	w.flushBatch()

	w.runMu.Lock()
	w.running = false
	w.runMu.Unlock()

	return nil
}

// watchTree recursively watches all directories.
func (w *Watcher) watchTree(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			return nil
		}

		// Check if this is a symlink
		fi, err := os.Lstat(path)
		if err != nil {
			return err
		}

		if fi.Mode()&os.ModeSymlink != 0 {
			return filepath.SkipDir // Don't follow symlinks
		}

		// Add watch
		if err := w.fsw.Add(path); err != nil {
			if w.verbose {
				fmt.Fprintf(os.Stderr, "warn: failed to watch %s: %v\n", path, err)
			}
			return nil // Continue anyway
		}

		if w.verbose {
			fmt.Printf("Watching: %s\n", path)
		}

		return nil
	})
}

// eventLoop processes fsnotify events.
// It drains all immediately available events before processing them as a batch,
// which reduces overhead and matches the Perl implementation's behavior.
func (w *Watcher) eventLoop() {
	defer w.wg.Done()

	for {
		select {
		case event, ok := <-w.fsw.Events:
			if !ok {
				return // Channel closed, watcher stopped
			}

			// Drain all immediately available events into a slice
			events := []fsnotify.Event{event}
			draining := true
			for draining && len(events) < 100000 { // Safety limit
				select {
				case e, ok := <-w.fsw.Events:
					if !ok {
						// Process what we have and exit
						w.handleEvents(events)
						return
					}
					events = append(events, e)
				default:
					draining = false
				}
			}

			// Process all drained events together
			w.handleEvents(events)

		case err, ok := <-w.fsw.Errors:
			if !ok {
				return // Channel closed
			}
			if w.errorHandler != nil {
				w.errorHandler(fmt.Errorf("fsnotify error: %w", err))
			}

		case <-w.ctx.Done():
			return
		}
	}
}

// handleEvents processes multiple fsnotify events efficiently.
// This reduces overhead by processing bursts of events together.
func (w *Watcher) handleEvents(events []fsnotify.Event) {
	if len(events) == 0 {
		return
	}

	items := make([]batchItem, 0, len(events))

	for _, event := range events {
		basename := filepath.Base(event.Name)

		// Filter 1: Skip temporary files
		if recentfile.ShouldIgnoreFile(basename) {
			continue
		}

		// Filter 2: Ignore RECENT files
		if w.ignoredRx.MatchString(basename) {
			continue
		}

		// Determine event type
		var typ string
		switch {
		case event.Op&fsnotify.Create != 0:
			// If it's a directory, add watch but don't create an entry
			if fi, err := os.Stat(event.Name); err == nil && fi.IsDir() {
				if err := w.watchTree(event.Name); err != nil && w.errorHandler != nil {
					w.errorHandler(fmt.Errorf("watch tree %s: %w", event.Name, err))
				}
				continue
			}
			typ = "new"

		case event.Op&fsnotify.Write != 0:
			// Skip directory modifications - we don't track those
			if fi, err := os.Stat(event.Name); err == nil && fi.IsDir() {
				continue
			}
			typ = "new"

		case event.Op&fsnotify.Chmod != 0:
			// Skip directory permission changes - we don't track those
			if fi, err := os.Stat(event.Name); err == nil && fi.IsDir() {
				continue
			}
			typ = "new"

		case event.Op&fsnotify.Remove != 0:
			// For removes, we can't stat since the path is gone
			// Could be a file or directory - add entry either way
			typ = "delete"

		case event.Op&fsnotify.Rename != 0:
			typ = "delete" // Source of rename

		default:
			continue // Ignore unknown events
		}

		if w.verbose {
			fmt.Printf("Event: %s %s\n", typ, event.Name)
		}

		items = append(items, batchItem{path: event.Name, typ: typ})
	}

	// Send all items to batch channel
	for _, item := range items {
		select {
		case w.batchChan <- item:
		default:
			// Channel full, drop event
			if w.errorHandler != nil {
				w.errorHandler(fmt.Errorf("batch channel full, dropping event: %s", item.path))
			}
		}
	}
}

// handleEvent processes a single fsnotify event.
func (w *Watcher) handleEvent(event fsnotify.Event) {
	basename := filepath.Base(event.Name)

	// Filter 1: Skip temporary files
	// These are created during atomic writes and symlink operations
	if recentfile.ShouldIgnoreFile(basename) {
		return
	}

	// Filter 2: Ignore RECENT files
	if w.ignoredRx.MatchString(basename) {
		return
	}

	// Determine event type
	var typ string
	switch {
	case event.Op&fsnotify.Create != 0:
		// If it's a directory, add watch but don't create an entry
		if fi, err := os.Stat(event.Name); err == nil && fi.IsDir() {
			if err := w.watchTree(event.Name); err != nil && w.errorHandler != nil {
				w.errorHandler(fmt.Errorf("watch tree %s: %w", event.Name, err))
			}
			return
		}
		typ = "new"

	case event.Op&fsnotify.Write != 0:
		// Skip directory modifications - we don't track those
		if fi, err := os.Stat(event.Name); err == nil && fi.IsDir() {
			return
		}
		typ = "new"

	case event.Op&fsnotify.Chmod != 0:
		// Skip directory permission changes - we don't track those
		if fi, err := os.Stat(event.Name); err == nil && fi.IsDir() {
			return
		}
		typ = "new"

	case event.Op&fsnotify.Remove != 0:
		// For removes, we can't stat since the path is gone
		// Could be a file or directory - add entry either way
		typ = "delete"

	case event.Op&fsnotify.Rename != 0:
		typ = "delete" // Source of rename

	default:
		return // Ignore unknown events
	}

	if w.verbose {
		fmt.Printf("Event: %s %s\n", typ, event.Name)
	}

	// Send to batch channel
	select {
	case w.batchChan <- batchItem{path: event.Name, typ: typ}:
	default:
		// Channel full, drop event (or could flush immediately)
		if w.errorHandler != nil {
			w.errorHandler(fmt.Errorf("batch channel full, dropping event: %s", event.Name))
		}
	}
}

// batchProcessor accumulates events and flushes periodically.
func (w *Watcher) batchProcessor() {
	defer w.wg.Done()

	// Create timer for batch flushing
	flushTimer := time.NewTimer(w.batchDelay)
	defer flushTimer.Stop()

	// Create timer for aggregation (if enabled)
	var aggregateTimer *time.Timer
	var aggregateChan <-chan time.Time
	if w.aggregateInterval > 0 {
		aggregateTimer = time.NewTimer(w.aggregateInterval)
		aggregateChan = aggregateTimer.C
		defer aggregateTimer.Stop()
	}

	for {
		select {
		case item, ok := <-w.batchChan:
			if !ok {
				return // Channel closed
			}

			w.batchMu.Lock()
			w.batch = append(w.batch, recentfile.BatchItem{
				Path: item.path,
				Type: item.typ,
			})

			// Check if batch is full
			needFlush := len(w.batch) >= w.batchSize
			w.batchMu.Unlock()

			if needFlush {
				w.flushBatch()
				// Reset flush timer after flushing
				if !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}
				flushTimer.Reset(w.batchDelay)
			}

		case <-flushTimer.C:
			w.flushBatch()
			flushTimer.Reset(w.batchDelay)

		case <-aggregateChan:
			if w.verbose {
				fmt.Println("Running periodic aggregation")
			}
			start := time.Now()
			if err := w.recent.Aggregate(false); err != nil {
				if w.errorHandler != nil {
					w.errorHandler(fmt.Errorf("aggregation error: %w", err))
				}
			} else {
				duration := time.Since(start)
				if w.aggregationCallback != nil {
					w.aggregationCallback(duration)
				}
			}
			aggregateTimer.Reset(w.aggregateInterval)

		case <-w.ctx.Done():
			w.flushBatch()
			return
		}
	}
}

// flushBatch writes accumulated events to the Recent collection.
func (w *Watcher) flushBatch() {
	w.batchMu.Lock()
	if len(w.batch) == 0 {
		w.batchMu.Unlock()
		return
	}

	batch := w.batch
	w.batch = nil
	w.batchMu.Unlock()

	if w.verbose {
		fmt.Printf("Flushing batch: %d events\n", len(batch))
	}

	// Deduplicate events (keep last event for each path)
	deduped := w.deduplicateBatch(batch)

	// Update the recent collection
	if err := w.recent.BatchUpdate(deduped); err != nil {
		if w.errorHandler != nil {
			w.errorHandler(fmt.Errorf("batch update failed: %w", err))
		}
		return // Don't call event callback on error
	}

	// Call event callback if registered
	if w.eventCallback != nil {
		// Count events by type
		counts := make(map[string]int)
		for _, item := range deduped {
			counts[item.Type]++
		}

		for eventType, count := range counts {
			w.eventCallback(eventType, count)
		}
	}

	// Update last flush time
	w.lastFlushMu.Lock()
	w.lastFlush = time.Now()
	w.lastFlushMu.Unlock()
}

// deduplicateBatch removes duplicate paths, keeping the last event for each path.
func (w *Watcher) deduplicateBatch(batch []recentfile.BatchItem) []recentfile.BatchItem {
	if len(batch) <= 1 {
		return batch
	}

	// Use map to track last event for each path
	eventMap := make(map[string]recentfile.BatchItem)

	for _, item := range batch {
		eventMap[item.Path] = item // Overwrites previous event for same path
	}

	// Convert back to slice
	result := make([]recentfile.BatchItem, 0, len(eventMap))
	for _, item := range eventMap {
		result = append(result, item)
	}

	if w.verbose && len(result) < len(batch) {
		fmt.Printf("Deduplicated: %d -> %d events\n", len(batch), len(result))
	}

	return result
}

// Stats returns statistics about the watcher.
func (w *Watcher) Stats() Stats {
	w.batchMu.Lock()
	currentBatchSize := len(w.batch)
	w.batchMu.Unlock()

	w.lastFlushMu.Lock()
	timeSinceFlush := time.Since(w.lastFlush)
	w.lastFlushMu.Unlock()

	return Stats{
		QueuedEvents:   len(w.batchChan),
		BatchSize:      currentBatchSize,
		TimeSinceFlush: timeSinceFlush,
	}
}

// Stats represents watcher statistics.
type Stats struct {
	QueuedEvents   int           // Events in channel
	BatchSize      int           // Events in current batch
	TimeSinceFlush time.Duration // Time since last flush
}

// IsRunning returns true if the watcher is running.
func (w *Watcher) IsRunning() bool {
	w.runMu.RLock()
	defer w.runMu.RUnlock()
	return w.running
}
