package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/prometheus/client_golang/prometheus"

	"go.ntppool.org/common/logger"
	"go.ntppool.org/common/metricsserver"
	"go.ntppool.org/common/version"

	"github.com/abh/rrrgo/fsck"
	"github.com/abh/rrrgo/recent"
	"github.com/abh/rrrgo/recentfile"
	"github.com/abh/rrrgo/watcher"
)

// CLI defines the command-line interface for rrr-server.
type CLI struct {
	LocalRoot string `arg:"" help:"Local root directory to watch." type:"path"`

	Interval   string   `short:"i" default:"1h" help:"Principal recentfile interval (e.g., 1h, 30m)."`
	Aggregator []string `short:"a" help:"Aggregator intervals (e.g., 6h,1d,1W). Can be specified multiple times."`
	Format     string   `short:"f" default:"yaml" enum:"yaml,yml,json" help:"Serialization format (yaml or json)."`

	BatchSize  int           `default:"1000" help:"Maximum batch size before flushing events."`
	BatchDelay time.Duration `default:"1s" help:"Maximum delay before flushing events."`

	AggregateInterval time.Duration `default:"5m" help:"How often to run aggregation."`

	MetricsPort int    `default:"9090" help:"Port for metrics server."`
	LogLevel    string `default:"info" help:"Log level (debug, info, warn, error)."`

	SkipFsck   bool `help:"Skip startup integrity check."`
	FsckRepair bool `help:"Auto-repair issues found during startup fsck."`

	Verbose bool `short:"v" help:"Enable verbose logging."`

	Version kong.VersionFlag `short:"V" help:"Show version."`
}

// metrics holds Prometheus metrics collectors.
type metrics struct {
	eventsProcessed     *prometheus.CounterVec
	aggregationRuns     prometheus.Counter
	aggregationDuration prometheus.Histogram
	eventsInQueue       prometheus.Gauge
}

// server holds the application state for rrr-server.
type server struct {
	rec     *recent.Recent
	watcher *watcher.Watcher
	metrics *metrics
	log     *slog.Logger
}

func main() {
	var cli CLI

	kctx := kong.Parse(&cli,
		kong.Name("rrr-server"),
		kong.Description("File synchronization server using RECENT protocol"),
		kong.UsageOnError(),
		kong.Vars{"version": version.Version()},
	)

	// Initialize logger
	// Set log level via environment variable for logger package
	if cli.Verbose {
		os.Setenv("LOG_LEVEL", "DEBUG")
	} else if cli.LogLevel != "" {
		os.Setenv("LOG_LEVEL", cli.LogLevel)
	}

	log := logger.Setup()

	if err := run(context.Background(), &cli, log); err != nil {
		log.Error("fatal error", "error", err)
		kctx.Exit(1)
	}
}

func run(ctx context.Context, cli *CLI, log *slog.Logger) error {
	// Validate local root
	localRoot, err := filepath.Abs(cli.LocalRoot)
	if err != nil {
		return fmt.Errorf("resolve local root: %w", err)
	}

	fi, err := os.Stat(localRoot)
	if err != nil {
		return fmt.Errorf("stat local root: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("local root is not a directory: %s", localRoot)
	}

	log.Info("starting rrr-server",
		"version", version.Version(),
		"local_root", localRoot,
		"interval", cli.Interval,
		"format", cli.Format,
		"aggregator", cli.Aggregator,
		"batch_size", cli.BatchSize,
		"batch_delay", cli.BatchDelay,
		"aggregate_interval", cli.AggregateInterval,
		"metrics_port", cli.MetricsPort,
	)

	// Start metrics server
	metricsSrv := metricsserver.New()

	// Define and register Prometheus metrics with custom registry
	eventsProcessed := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rrr_events_processed_total",
			Help: "Total number of file system events processed",
		},
		[]string{"type"}, // "new" or "delete"
	)

	aggregationRuns := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "rrr_aggregation_runs_total",
			Help: "Total number of aggregation runs",
		},
	)

	aggregationDuration := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "rrr_aggregation_duration_seconds",
			Help:    "Time taken to run aggregation",
			Buckets: prometheus.DefBuckets,
		},
	)

	eventsInQueue := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "rrr_events_in_queue",
			Help: "Current number of events queued for processing",
		},
	)

	// Register all metrics with the custom registry
	metricsSrv.Registry().MustRegister(
		eventsProcessed,
		aggregationRuns,
		aggregationDuration,
		eventsInQueue,
	)

	go func() {
		log.Info("metrics server starting", "port", cli.MetricsPort)
		if err := metricsSrv.ListenAndServe(ctx, cli.MetricsPort); err != nil {
			log.Error("metrics server error", "error", err)
		}
	}()

	// Create or load Recent collection
	rec, err := createOrLoadRecent(localRoot, cli.Interval, cli.Format, cli.Aggregator, log)
	if err != nil {
		return fmt.Errorf("create/load recent: %w", err)
	}

	log.Info("recent collection loaded", "collection", rec.String())

	// Run startup fsck (unless --skip-fsck)
	if !cli.SkipFsck {
		log.Info("running startup fsck", "auto_repair", cli.FsckRepair)

		fsckOpts := fsck.Options{
			Repair:     cli.FsckRepair,
			SkipEvents: false, // Full check by default
			Verbose:    cli.Verbose,
			Logger:     log,
		}

		result, err := fsck.Run(rec, fsckOpts)
		if err != nil {
			return fmt.Errorf("startup fsck failed: %w", err)
		}

		if result.Issues > 0 {
			if cli.FsckRepair {
				log.Info("startup fsck repaired issues", "issues", result.Issues)
			} else {
				// Issues found but not repaired - fail startup
				return fmt.Errorf("startup fsck found %d issues (use --fsck-repair to auto-fix)", result.Issues)
			}
		} else {
			log.Debug("startup fsck completed with no issues")
		}
	} else {
		log.Info("skipping startup fsck")
	}

	// Create watcher
	w, err := watcher.New(rec,
		watcher.WithBatchSize(cli.BatchSize),
		watcher.WithBatchDelay(cli.BatchDelay),
		watcher.WithVerbose(cli.Verbose),
		watcher.WithErrorHandler(func(err error) {
			log.Error("watcher error", "error", err)
		}),
		watcher.WithEventCallback(func(eventType string, count int) {
			eventsProcessed.WithLabelValues(eventType).Add(float64(count))
		}),
	)
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}

	// Start watcher
	if err := w.Start(); err != nil {
		return fmt.Errorf("start watcher: %w", err)
	}

	log.Info("watcher started")

	// Create server struct
	srv := &server{
		rec:     rec,
		watcher: w,
		metrics: &metrics{
			eventsProcessed:     eventsProcessed,
			aggregationRuns:     aggregationRuns,
			aggregationDuration: aggregationDuration,
			eventsInQueue:       eventsInQueue,
		},
		log: log,
	}

	// Start periodic aggregation
	stopAgg := make(chan struct{})
	aggDone := make(chan struct{})
	go srv.periodicAggregation(cli.AggregateInterval, stopAgg, aggDone)

	log.Info("periodic aggregation started", "interval", cli.AggregateInterval)

	// Start metrics reporter
	stopMetrics := make(chan struct{})
	metricsDone := make(chan struct{})
	go srv.metricsReporter(stopMetrics, metricsDone)

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Info("received shutdown signal", "signal", sig.String())

	// Stop metrics reporter
	close(stopMetrics)
	<-metricsDone

	// Stop periodic aggregation
	close(stopAgg)
	<-aggDone

	// Stop watcher
	if err := w.Stop(); err != nil {
		return fmt.Errorf("stop watcher: %w", err)
	}

	log.Info("watcher stopped")

	// Final aggregation
	log.Info("running final aggregation")
	if err := rec.Aggregate(false); err != nil {
		return fmt.Errorf("final aggregation: %w", err)
	}

	stats := rec.Stats()
	log.Info("shutdown complete",
		"total_events", stats.TotalEvents,
		"intervals", stats.Intervals,
	)

	return nil
}

// createOrLoadRecent creates a new Recent collection or loads an existing one.
func createOrLoadRecent(localRoot, interval, format string, aggregator []string, log *slog.Logger) (*recent.Recent, error) {
	// Normalize format to file extension
	suffix := "." + format
	if format == "yml" {
		suffix = ".yaml"
	}

	// Check if principal recentfile exists
	principalPath := filepath.Join(localRoot, fmt.Sprintf("RECENT-%s%s", interval, suffix))

	if _, err := os.Stat(principalPath); os.IsNotExist(err) {
		// Create new Recent collection
		log.Info("creating new recent collection", "principal", principalPath)

		principal := recentfile.New(
			recentfile.WithLocalRoot(localRoot),
			recentfile.WithInterval(interval),
			recentfile.WithSerializerSuffix(suffix),
			recentfile.WithAggregator(aggregator),
		)

		rec, err := recent.NewWithPrincipal(principal)
		if err != nil {
			return nil, fmt.Errorf("new with principal: %w", err)
		}

		// Ensure all files exist
		if err := rec.EnsureFilesExist(); err != nil {
			return nil, fmt.Errorf("ensure files exist: %w", err)
		}

		return rec, nil
	}

	// Load existing Recent collection
	log.Info("loading existing recent collection", "principal", principalPath)

	rec, err := recent.New(principalPath)
	if err != nil {
		return nil, fmt.Errorf("load recent: %w", err)
	}

	// Load all recentfiles from disk
	if err := rec.LoadAll(); err != nil {
		return nil, fmt.Errorf("load all: %w", err)
	}

	return rec, nil
}

// periodicAggregation runs aggregation at regular intervals.
func (s *server) periodicAggregation(interval time.Duration, stop chan struct{}, done chan struct{}) {
	defer close(done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.log.Debug("running periodic aggregation")

			start := time.Now()
			if err := s.rec.Aggregate(false); err != nil {
				s.log.Error("aggregation error", "error", err)
			} else {
				duration := time.Since(start)
				stats := s.rec.Stats()

				// Update Prometheus metrics
				s.metrics.aggregationRuns.Inc()
				s.metrics.aggregationDuration.Observe(duration.Seconds())

				s.log.Info("aggregation complete",
					"duration", duration,
					"total_events", stats.TotalEvents,
				)
			}

		case <-stop:
			return
		}
	}
}

// metricsReporter periodically reports watcher stats to Prometheus.
func (s *server) metricsReporter(stop chan struct{}, done chan struct{}) {
	defer close(done)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stats := s.watcher.Stats()
			s.metrics.eventsInQueue.Set(float64(stats.QueuedEvents + stats.BatchSize))

		case <-stop:
			return
		}
	}
}
