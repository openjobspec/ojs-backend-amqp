// Package metrics provides Prometheus instrumentation for the OJS AMQP server.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_enqueued_total",
		Help:      "Total number of jobs enqueued.",
	}, []string{"queue", "type"})

	JobsFetched = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_fetched_total",
		Help:      "Total number of jobs fetched.",
	}, []string{"queue"})

	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_completed_total",
		Help:      "Total number of jobs completed.",
	}, []string{"queue", "type"})

	JobsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_failed_total",
		Help:      "Total number of jobs failed.",
	}, []string{"queue", "type"})

	JobsCancelled = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_cancelled_total",
		Help:      "Total number of jobs cancelled.",
	}, []string{"queue", "type"})

	ActiveJobs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ojs",
		Name:      "jobs_active",
		Help:      "Number of currently active jobs per queue.",
	}, []string{"queue"})

	ServerInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ojs",
		Name:      "server_info",
		Help:      "Static server metadata.",
	}, []string{"version", "backend"})

	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "http_requests_total",
		Help:      "Total number of HTTP requests.",
	}, []string{"method", "path", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ojs",
		Name:      "http_request_duration_seconds",
		Help:      "Duration of HTTP requests in seconds.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"method", "path", "status"})
)

// Init sets static server metadata on the info metric.
func Init(version, backend string) {
	ServerInfo.WithLabelValues(version, backend).Set(1)
}
