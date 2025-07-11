package metrics

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DurationMetrics stores statistics about operation durations
type DurationMetrics struct {
	durations []time.Duration
	mutex     sync.Mutex // Only used when finalizing statistics, not during data collection
}

// NewDurationMetrics creates a new DurationMetrics instance
func NewDurationMetrics() *DurationMetrics {
	return &DurationMetrics{
		durations: make([]time.Duration, 0, 10000), // Pre-allocate some capacity
	}
}

// Add adds a new duration to the metrics
func (m *DurationMetrics) Add(d time.Duration) {
	// This is the only function called during the benchmark
	// We use append which is not thread-safe, but we accept this risk
	// to avoid locking during the benchmark
	m.durations = append(m.durations, d)
}

// Stats calculates and returns statistics about the collected durations
// This should only be called after all data collection is complete
func (m *DurationMetrics) Stats() (avg, min, med, max time.Duration, p90, p95, p99 time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.durations) == 0 {
		return 0, 0, 0, 0, 0, 0, 0
	}

	// Sort the durations for percentile calculations
	sort.Slice(m.durations, func(i, j int) bool {
		return m.durations[i] < m.durations[j]
	})

	// Calculate statistics
	var sum time.Duration
	min = m.durations[0]
	max = m.durations[0]

	for _, d := range m.durations {
		sum += d
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}

	avg = sum / time.Duration(len(m.durations))
	med = m.durations[len(m.durations)/2]
	p90 = m.durations[int(float64(len(m.durations))*0.9)]
	p95 = m.durations[int(float64(len(m.durations))*0.95)]
	p99 = m.durations[int(float64(len(m.durations))*0.99)]

	return avg, min, med, max, p90, p95, p99
}

// String returns a formatted string with all statistics
func (m *DurationMetrics) String() string {
	avg, min, med, max, p90, p95, p99 := m.Stats()
	return fmt.Sprintf("avg=%v min=%v med=%v max=%v p(90)=%v p(95)=%v p(99)=%v",
		roundDuration(avg), roundDuration(min), roundDuration(med),
		roundDuration(max), roundDuration(p90), roundDuration(p95), roundDuration(p99))
}

// Count returns the number of samples
func (m *DurationMetrics) Count() int {
	return len(m.durations)
}

// Helper function to round duration to 2 decimal places for display
func roundDuration(d time.Duration) string {
	ms := float64(d) / float64(time.Millisecond)
	return fmt.Sprintf("%.2fms", ms)
}

// ConcurrentMetrics is a thread-safe metrics collector that can be used
// across multiple goroutines without locking during data collection
type ConcurrentMetrics struct {
	// These metrics are collected per worker and then combined at the end
	workerMetrics []*DurationMetrics
	successCount  int64
	errorCount    int64
}

// NewConcurrentMetrics creates a new ConcurrentMetrics with the specified number of workers
func NewConcurrentMetrics(workers int) *ConcurrentMetrics {
	workerMetrics := make([]*DurationMetrics, workers)
	for i := 0; i < workers; i++ {
		workerMetrics[i] = NewDurationMetrics()
	}
	return &ConcurrentMetrics{
		workerMetrics: workerMetrics,
	}
}

// AddDuration adds a duration for a specific worker
func (cm *ConcurrentMetrics) AddDuration(workerID int, d time.Duration) {
	// This is thread-safe because each worker only accesses its own metrics
	cm.workerMetrics[workerID].Add(d)
}

// AddSuccess increments the success counter atomically
func (cm *ConcurrentMetrics) AddSuccess(count int64) {
	atomic.AddInt64(&cm.successCount, count)
}

// AddError increments the error counter atomically
func (cm *ConcurrentMetrics) AddError(count int64) {
	atomic.AddInt64(&cm.errorCount, count)
}

// GetSuccessCount returns the current success count
func (cm *ConcurrentMetrics) GetSuccessCount() int64 {
	return atomic.LoadInt64(&cm.successCount)
}

// GetErrorCount returns the current error count
func (cm *ConcurrentMetrics) GetErrorCount() int64 {
	return atomic.LoadInt64(&cm.errorCount)
}

// CombinedStats returns combined statistics from all workers
func (cm *ConcurrentMetrics) CombinedStats() *DurationMetrics {
	combined := NewDurationMetrics()

	// Combine all worker metrics
	for _, wm := range cm.workerMetrics {
		combined.durations = append(combined.durations, wm.durations...)
	}

	return combined
}

// String returns a formatted string with all statistics
func (cm *ConcurrentMetrics) String() string {
	combined := cm.CombinedStats()
	successCount := cm.GetSuccessCount()
	errorCount := cm.GetErrorCount()
	totalCount := successCount + errorCount

	var successRate float64
	if totalCount > 0 {
		successRate = float64(successCount) * 100 / float64(totalCount)
	}

	return fmt.Sprintf("Total: %d, Success: %d, Error: %d, Success Rate: %.2f%%\n%s",
		totalCount, successCount, errorCount, successRate, combined.String())
}
