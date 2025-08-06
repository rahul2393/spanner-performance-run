package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	// OpenTelemetry imports
	cloudtrace "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"

	"github.com/rahul2393/spanner-experiments/golang-spanner-performance/metrics"
)

var (
	VUS               = 10
	MaxCommitDelayMs  = 100
	TEST_DURATION_SEC = 0

	numChannels   int
	testType      string
	useStaleReads bool
	withJaegar    bool
	staleReadMode string
	stalenessMs   int

	instanceID             = "irahul-load-test"
	GRAPH_NAME             = "g0618"
	credentialsFile        = "sa2.json"
	projectID              = "span-cloud-testing"
	databaseID             = "graphdb"
	currentTPS      uint64 = 0
)

func init() {
	flag.StringVar(&testType, "test", "setup", "Test type to run: setup, setupindex, write-vertex, write-vertex-improved, delete-vertex-using-pdml, write-edge, relation, all")
	flag.BoolVar(&useStaleReads, "stale-reads", true, "Enable stale reads for read operations")
	flag.BoolVar(&withJaegar, "with-jaeger", false, "Enable Jaeger tracing exporter")
	flag.StringVar(&staleReadMode, "stale-mode", "max", "Stale read mode: max or exact")
	flag.IntVar(&stalenessMs, "staleness-ms", 200, "Staleness time in milliseconds")
	flag.IntVar(&numChannels, "num-channels", 4, "Number of channels to use")
	flag.Parse()

	// Initialize VUS from environment variable
	if vusStr := os.Getenv("VUS"); vusStr != "" {
		if parsedVUS, err := strconv.Atoi(vusStr); err == nil && parsedVUS > 0 {
			VUS = parsedVUS
		}
	}

	// Initialize MaxCommitDelayMs from environment variable
	if delayStr := os.Getenv("MAX_COMMIT_DELAY_MS"); delayStr != "" {
		if parsedDelay, err := strconv.Atoi(delayStr); err == nil && parsedDelay >= 0 {
			MaxCommitDelayMs = parsedDelay
		}
	}

	// Initialize instanceID from environment variable
	if instID := os.Getenv("INSTANCE_ID"); instID != "" {
		instanceID = instID
	}

	// Initialize TEST_DURATION_SEC from environment variable
	if testDurationStr := os.Getenv("TEST_DURATION_SEC"); testDurationStr != "" {
		if parsedTestDuration, err := strconv.Atoi(testDurationStr); err == nil && parsedTestDuration >= 0 {
			TEST_DURATION_SEC = parsedTestDuration
		}
	}

	// Initialize credentialsFile from environment variable
	if credFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS_FILE"); credFile != "" {
		credentialsFile = credFile
	}

	// Initialize projectID from environment variable
	if projectIDStr := os.Getenv("PROJECT_ID"); projectIDStr != "" {
		projectID = projectIDStr
	}

	// Initialize databaseID from environment variable
	if dbID := os.Getenv("DATABASE_ID"); dbID != "" {
		databaseID = dbID
	}
}

func countdownOrExit(action string, seconds int) {
	// translate in english
	log.Printf("Countdown action: %s, seconds: %d", action, seconds)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	timer := time.NewTimer(time.Duration(seconds) * time.Second)
	select {
	case <-interrupt:
		log.Println("Interrupted")
		os.Exit(1)
	case <-timer.C:
		// Continue normally
	}
}

// handleInterrupt sets up graceful shutdown on interrupt signal
func handleInterrupt(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		log.Println("\nReceived interrupt signal, stopping workers gracefully...")
		cancel()
	}()
}

func initOpenTelemetryTracer() func() {
	// Create Jaeger exporter for OpenTelemetry
	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://localhost:14268/api/traces")))
	if err != nil {
		log.Printf("Failed to create Jaeger exporter: %v", err)
		panic(err)
	}

	// Create trace provider with batch span processor
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithSampler(trace.AlwaysSample()),
	)

	// Set the global trace provider
	otel.SetTracerProvider(tp)
	log.Printf("OpenTelemetry tracer configured with Jaeger exporter at http://localhost:14268/api/traces")
	log.Printf("All Spanner client internal tracing will be automatically exported")

	return func() {
		log.Printf("Shutting down trace provider...")
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down trace provider: %v", err)
		}
	}
}

// setupTracing initializes OpenTelemetry tracing with Cloud Trace exporter
func setupTracing(ctx context.Context) (*trace.TracerProvider, error) {

	// Create resource detector for GCP
	resourceDetector := gcp.NewDetector()

	// Create resource with service information
	res, err := resource.New(ctx,
		resource.WithDetectors(resourceDetector),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("spanner-performance-test"),
			semconv.ServiceVersionKey.String("1.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create Cloud Trace exporter
	exporter, err := cloudtrace.New(cloudtrace.WithProjectID(projectID))
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Trace exporter: %w", err)
	}

	// Create tracer provider
	tp := trace.NewTracerProvider(
		trace.WithResource(res),
		trace.WithBatcher(exporter),
		trace.WithSampler(trace.AlwaysSample()),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	log.Printf("OpenTelemetry Cloud Trace exporter initialized for project: %s", projectID)
	return tp, nil
}

func spannerReadUserTest(client *spanner.Client, dbPath string) {
	log.Println("Starting improved Spanner read vertex test...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handleInterrupt(cancel)

	startTime := time.Now()

	// Initialize detailed metrics collector
	metricsCollector := metrics.NewConcurrentMetrics(VUS)
	atomic.StoreUint64(&currentTPS, 0)

	// Total queries to execute
	totalQueries := 1000000

	// Determine test mode
	isDurationBased := TEST_DURATION_SEC > 0
	if isDurationBased {
		log.Printf("Running duration-based vertex read test: %d seconds with %d workers", TEST_DURATION_SEC, VUS)
	} else {
		log.Printf("Running count-based vertex read test: %d queries with %d workers", totalQueries, VUS)
	}

	log.Printf("Starting %d vertex read workers...", VUS)
	log.Println("Press Ctrl+C at any time to stop gracefully...")
	countdownOrExit("开始读顶点", 5)

	group, grpCtx := errgroup.WithContext(ctx)

	// Launch vertex read workers
	for workerID := 0; workerID < VUS; workerID++ {
		workerID := workerID // capture loop variable
		group.Go(func() error {
			return readVertexWorkerWithMetrics(grpCtx, client, workerID, totalQueries, isDurationBased, metricsCollector)
		})
	}

	// Wait for all workers to complete
	if err := group.Wait(); err != nil {
		log.Printf("Vertex read worker error: %v", err)
	}

	totalDuration := time.Since(startTime)

	// Get combined metrics for detailed latency breakdown
	combinedMetrics := metricsCollector.CombinedStats()
	metricsSuccess := metricsCollector.GetSuccessCount()
	metricsErrors := metricsCollector.GetErrorCount()

	if ctx.Err() != nil {
		log.Println("Improved vertex read test interrupted by user:")
	} else {
		log.Println("Improved vertex read test completed:")
	}
	log.Printf("  Total duration: %v", totalDuration)
	log.Printf("  Expected queries: %d", totalQueries)
	log.Printf("  Successful queries:(metrics: %d)", metricsSuccess)
	log.Printf("  Failed queries: (metrics: %d)", metricsErrors)

	log.Printf("  Latency metrics: %s", combinedMetrics.String())
	spanner.PrintQueryTimingPercentiles()
}

// readVertexWorkerWithMetrics is a worker function for reading vertices with detailed metrics collection
func readVertexWorkerWithMetrics(ctx context.Context, client *spanner.Client, workerID int, totalQueries int, isDurationBased bool, metricsCollector *metrics.ConcurrentMetrics) error {
	log.Printf("Read Worker %d started", workerID)

	var stopTimer *time.Timer
	var done bool = false

	// Calculate queries per worker for count-based tests
	var queriesPerWorker int
	var executedQueries int

	if !isDurationBased {
		queriesPerWorker = totalQueries / VUS
		// Handle remainder by giving extra queries to the first few workers
		if workerID < totalQueries%VUS {
			queriesPerWorker++
		}
		log.Printf("Worker %d will execute %d queries", workerID, queriesPerWorker)
	}

	if isDurationBased {
		stopTimer = time.NewTimer(time.Duration(TEST_DURATION_SEC) * time.Second)
		defer func() {
			if stopTimer != nil {
				stopTimer.Stop()
			}
		}()

		go func() {
			select {
			case <-stopTimer.C:
				log.Printf("Vertex Read Worker %d completed due to timer", workerID)
				done = true
			case <-ctx.Done():
				log.Printf("Vertex Read Worker %d completed due to context cancellation", workerID)
				done = true
			}
		}()
	}
	workerSuccessCount := 0
	workerErrorCount := 0

	for !done {
		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Printf("Vertex Read Worker %d stopping due to context cancellation", workerID)
			return ctx.Err()
		default:
		}

		// Check if we've reached the query limit for count-based tests
		if !isDurationBased && executedQueries >= queriesPerWorker {
			log.Printf("Worker %d completed %d queries", workerID, executedQueries)
			done = true
			break
		}

		var ro *spanner.ReadOnlyTransaction
		sql := `Select uid from Users where uid=@attr11`
		// Build parameterized query
		stmt := spanner.Statement{
			SQL: sql,
			Params: map[string]interface{}{
				"attr11": int64(1099511627777),
			},
		}
		queryStart := time.Now()
		// Create a new single-use read-only transaction for each query
		if useStaleReads {
			// Configure stale read based on settings
			if staleReadMode == "max" {
				// Maximum staleness - read data that's at most N milliseconds old
				ro = client.Single().WithTimestampBound(
					spanner.MaxStaleness(time.Duration(stalenessMs) * time.Millisecond))
			} else if staleReadMode == "exact" {
				// Exact staleness - read data that's exactly N milliseconds old
				ro = client.Single().WithTimestampBound(
					spanner.ExactStaleness(time.Duration(stalenessMs) * time.Millisecond))
			}
		} else {
			// Use strong consistency (default)
			ro = client.Single()
		}
		iterRows := ro.Query(ctx, stmt)

		// Count rows and consume results
		rowCount := 0
		success := true
		for {
			_, err := iterRows.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Printf("Vertex Read Worker %d query failed: %v", workerID, err)
				success = false
				break
			}
			rowCount++
		}
		iterRows.Stop()
		ro.Close() // Close the transaction after each query

		queryDuration := time.Since(queryStart)

		atomic.AddUint64(&currentTPS, 1)
		metricsCollector.AddDuration(workerID, queryDuration)

		// Increment executed queries counter
		executedQueries++

		if success {
			workerSuccessCount++
			metricsCollector.AddSuccess(1)
		} else {
			workerErrorCount++
			metricsCollector.AddError(1)
		}
		// Small delay to prevent overwhelming the system
		if !isDurationBased {
			time.Sleep(1 * time.Millisecond)
		}
	}

	log.Printf("Vertex Read Worker %d completed: %d success, %d errors", workerID, workerSuccessCount, workerErrorCount)
	return nil
}

func main() {
	ctx := context.Background()

	// Initialize OpenTelemetry tracing
	if withJaegar {
		log.Println("Initializing OpenTelemetry tracer with Jaeger exporter...")
		cleanup := initOpenTelemetryTracer()
		defer cleanup()
	} else {
		tp, err := setupTracing(ctx)
		if err != nil {
			log.Fatalf("Failed to setup tracing: %v", err)
		}
		defer func() {
			if err := tp.Shutdown(ctx); err != nil {
				log.Printf("Error shutting down tracer provider: %v", err)
			}
		}()
	}

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	clientOpts := []option.ClientOption{
		option.WithGRPCConnectionPool(numChannels),

		//option.WithGRPCDialOption(grpc.WithStreamInterceptor(AddGFELatencyStreamingInterceptor)),
	}
	client, err := spanner.NewClientWithConfig(ctx, dbPath, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{MaxOpened: 1, MinOpened: 1},
	},
		clientOpts...,
	)
	if err != nil {
		log.Printf("Failed to create Spanner client: %v", err)
		log.Fatalf("Spanner client required for test type: %s", testType)
	}
	defer client.Close()
	for i := 0; i < numChannels; i++ {
		if err = client.Single().Query(ctx, spanner.NewStatement("SELECT * from Users limit 1")).Do(func(row *spanner.Row) error {
			return nil
		}); err != nil {
			log.Printf("Failed to connect to Spanner database %s: %v", dbPath, err)
			log.Fatalf("Spanner client required for test type: %s", testType)
		}
	}
	spanner.EnableQueryTimingMetrics()
	switch testType {
	case "read-user":
		log.Println("Running read user...")
		spannerReadUserTest(client, dbPath)
	}
}

const serverTimingKey = "server-timing"

var serverTimingPattern = regexp.MustCompile(`([a-zA-Z0-9_-]+);\s*dur=(\d*\.?\d+)`)

// parseT4T7Latency parse the headers and trailers for finding the gfet4t7 latency.
func parseT4T7Latency(md metadata.MD) (map[string]time.Duration, error) {
	if md == nil {
		return nil, fmt.Errorf("server-timing headers not found")
	}

	serverTiming := md.Get(serverTimingKey)

	if len(serverTiming) == 0 {
		return nil, fmt.Errorf("server-timing headers not found")
	}
	result := make(map[string]time.Duration)
	for _, timing := range serverTiming {
		matches := serverTimingPattern.FindAllStringSubmatch(timing, -1)
		for _, match := range matches {
			if len(match) == 3 { // full match + 2 capture groups
				metricName := match[1]
				duration, err := strconv.ParseFloat(match[2], 10)
				if err != nil {
					return nil, fmt.Errorf("failed to parse gfe latency: %v", err)

				}
				if metricName == "gfet4t7" {
					result["gfe"] = time.Duration(duration*1000) * time.Microsecond
				}
				if metricName == "afe" {
					result["afe"] = time.Duration(duration*1000) * time.Microsecond
				}
			}
		}
	}
	return result, nil
}

func AddGFELatencyStreamingInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return cs, err
	}

	// Get the current span from context (created by otelgrpc instrumentation)
	span := oteltrace.SpanFromContext(ctx)

	if span.IsRecording() && cs != nil {
		headers, err := cs.Header()
		if err != nil {
			return cs, nil
		}
		// Parse and add GFE latency attributes to the existing span
		if latencyMap, parseErr := parseT4T7Latency(headers); parseErr == nil {
			for latencyType, gfeLatency := range latencyMap {
				span.SetAttributes(
					attribute.String(latencyType+".latency", gfeLatency.String()),
					attribute.Int64(latencyType+".latency_us", gfeLatency.Microseconds()),
					attribute.Float64(latencyType+".latency_ms", float64(gfeLatency.Nanoseconds())/1e6),
				)
			}
		} else {
			span.SetAttributes(attribute.String("gfe_latency_error", parseErr.Error()))
		}

		// Add additional useful attributes
		span.SetAttributes(
			attribute.String("component", "spanner-client"),
			attribute.String("interceptor", "gfe-latency"),
		)
	}

	return cs, nil
}
