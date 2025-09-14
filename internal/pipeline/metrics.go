package pipeline

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	received  uint64
	processed uint64
	failed    uint64

	totalLatencyMS uint64
	startTime      time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{startTime: time.Now()}
}

func (m *Metrics) IncReceived() {
	atomic.AddUint64(&m.received, 1)
}

func (m *Metrics) IncProcessed() {
	atomic.AddUint64(&m.processed, 1)
}

func (m *Metrics) IncFailed() {
	atomic.AddUint64(&m.failed, 1)
}

func (m *Metrics) AddLatency(ms int64) {
	atomic.AddUint64(&m.totalLatencyMS, uint64(ms))
}

func (m *Metrics) GetReceived() uint64 {
	return atomic.LoadUint64(&m.received)
}

func (m *Metrics) GetProcessed() uint64 {
	return atomic.LoadUint64(&m.processed)
}

func (m *Metrics) GetFailed() uint64 {
	return atomic.LoadUint64(&m.failed)
}

func (m *Metrics) AvgLatencyMS() float64 {
	processed := atomic.LoadUint64(&m.processed)
	if processed == 0 {
		return 0
	}
	total := atomic.LoadUint64(&m.totalLatencyMS)
	return float64(total) / float64(processed)
}

func (m *Metrics) EPS() float64 {
	secs := time.Since(m.startTime).Seconds()
	if secs <= 0 {
		return 0
	}
	return float64(m.GetProcessed()) / secs
}

func (m *Metrics) StartTime() time.Time {
	return m.startTime
}
