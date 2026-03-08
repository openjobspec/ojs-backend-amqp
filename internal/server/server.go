package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	commoncore "github.com/openjobspec/ojs-go-backend-common/core"
	"github.com/openjobspec/ojs-go-backend-common/httputil"
	commonmw "github.com/openjobspec/ojs-go-backend-common/middleware"
	"github.com/openjobspec/ojs-go-backend-common/registry"

	"github.com/openjobspec/ojs-backend-amqp/internal/metrics"
)

// routerConfig holds optional router configuration.
type routerConfig struct {
	eventLister EventLister
}

// RouterOption configures optional router behavior.
type RouterOption func(*routerConfig)

// WithEventLister enables the GET /ojs/v1/events endpoint.
func WithEventLister(el EventLister) RouterOption {
	return func(c *routerConfig) { c.eventLister = el }
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		duration := time.Since(start).Seconds()

		path := "unknown"
		if rctx := chi.RouteContext(r.Context()); rctx != nil && rctx.RoutePattern() != "" {
			path = rctx.RoutePattern()
		}

		metrics.HTTPRequestsTotal.WithLabelValues(r.Method, path, fmt.Sprintf("%d", ww.Status())).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(r.Method, path, fmt.Sprintf("%d", ww.Status())).Observe(duration)
	})
}

// EventLister provides queryable access to stored events.
type EventLister interface {
	ListEvents(types []string, queues []string, limit int) []map[string]any
}

// NewRouter creates the HTTP router with all OJS routes.
func NewRouter(backend commoncore.Backend, cfg Config, publisher commoncore.EventPublisher, subscriber commoncore.EventSubscriber, opts ...RouterOption) http.Handler {
	var routerCfg routerConfig
	for _, o := range opts {
		o(&routerCfg)
	}
	metrics.Init("0.2.0", "amqp")

	r := chi.NewRouter()

	// Middleware stack
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(metricsMiddleware)
	r.Use(commonmw.OJSHeaders)

	// Authentication: OIDC JWT takes precedence over API key
	skipPaths := []string{"/metrics", "/ojs/v1/health", "/healthz", "/readyz"}
	if cfg.OIDCIssuer != "" {
		r.Use(commonmw.JWTAuth(commonmw.OIDCConfig{
			IssuerURL: cfg.OIDCIssuer,
			Audience:  cfg.OIDCClientID,
			SkipPaths: skipPaths,
		}))
	} else if cfg.APIKey != "" {
		r.Use(commonmw.KeyAuth(cfg.APIKey, skipPaths...))
	}

	// Prometheus metrics endpoint
	r.Handle("/metrics", promhttp.Handler())

	// Create handlers from shared backend-common
	jobHandler := commonapi.NewJobHandler(backend)
	workerHandler := commonapi.NewWorkerHandler(backend)

	// Enable schema validation via in-memory registry
	schemaReg := commoncore.NewMemorySchemaRegistry()
	jobHandler.SetSchemaRegistry(schemaReg)

	systemHandler := commonapi.NewSystemHandler(backend, commonapi.ManifestConfig{
		ImplementationName: "ojs-backend-amqp",
		ImplementationVer:  "0.2.0",
		BackendName:        "amqp",
		ConformanceLevel:   4,
	})
	queueHandler := commonapi.NewQueueHandler(backend)
	deadLetterHandler := commonapi.NewDeadLetterHandler(backend)
	cronHandler := commonapi.NewCronHandler(backend)
	workflowHandler := commonapi.NewWorkflowHandler(backend)
	batchHandler := commonapi.NewBatchHandler(backend)
	adminHandler := commonapi.NewAdminHandler(backend)

	// Wire event publisher
	if publisher != nil {
		jobHandler.SetEventPublisher(publisher)
		workerHandler.SetEventPublisher(publisher)
	}

	// System
	r.Get("/ojs/manifest", systemHandler.Manifest)
	r.Get("/ojs/v1/health", systemHandler.Health)

	// Jobs
	r.Post("/ojs/v1/jobs", jobHandler.Create)
	r.Get("/ojs/v1/jobs/{id}", jobHandler.Get)
	r.Delete("/ojs/v1/jobs/{id}", jobHandler.Cancel)
	r.Post("/ojs/v1/jobs/batch", batchHandler.Create)

	// Workers
	r.Post("/ojs/v1/workers/fetch", workerHandler.Fetch)
	r.Post("/ojs/v1/workers/ack", workerHandler.Ack)
	r.Post("/ojs/v1/workers/nack", workerHandler.Nack)
	r.Post("/ojs/v1/workers/heartbeat", workerHandler.Heartbeat)

	// Queues
	r.Get("/ojs/v1/queues", queueHandler.List)
	r.Get("/ojs/v1/queues/{name}/stats", queueHandler.Stats)
	r.Post("/ojs/v1/queues/{name}/pause", queueHandler.Pause)
	r.Post("/ojs/v1/queues/{name}/resume", queueHandler.Resume)

	// Dead letter
	r.Get("/ojs/v1/dead-letter", deadLetterHandler.List)
	r.Post("/ojs/v1/dead-letter/{id}/retry", deadLetterHandler.Retry)
	r.Delete("/ojs/v1/dead-letter/{id}", deadLetterHandler.Delete)

	// Cron
	r.Get("/ojs/v1/cron", cronHandler.List)
	r.Post("/ojs/v1/cron", cronHandler.Register)
	r.Delete("/ojs/v1/cron/{name}", cronHandler.Delete)

	// Workflows
	r.Post("/ojs/v1/workflows", workflowHandler.Create)
	r.Get("/ojs/v1/workflows/{id}", workflowHandler.Get)
	r.Delete("/ojs/v1/workflows/{id}", workflowHandler.Cancel)

	// Admin
	r.Get("/ojs/v1/admin/stats", adminHandler.Stats)
	r.Get("/ojs/v1/admin/jobs", adminHandler.ListJobs)
	r.Get("/ojs/v1/admin/workers", adminHandler.ListWorkers)

	// Schema registry API
	schemaRegistry := registry.NewSchemaRegistry()
	schemaHandler := registry.NewSchemaHandler(schemaRegistry)
	r.Post("/ojs/v1/schemas", schemaHandler.HandleRegister)
	r.Get("/ojs/v1/schemas/{jobType}", schemaHandler.HandleGetLatest)
	r.Get("/ojs/v1/schemas/{jobType}/versions", schemaHandler.HandleListVersions)
	r.Get("/ojs/v1/schemas/{jobType}/versions/{version}", schemaHandler.HandleGetVersion)
	r.Post("/ojs/v1/schemas/{jobType}/validate", schemaHandler.HandleValidate)
	r.Put("/ojs/v1/schemas/{jobType}/compatibility", schemaHandler.HandleSetCompatibility)
	r.Delete("/ojs/v1/schemas/{jobType}", schemaHandler.HandleDelete)
	r.Delete("/ojs/v1/schemas/{jobType}/versions/{version}", schemaHandler.HandleDelete)

	// SSE events
	if subscriber != nil {
		sseHandler := commonapi.NewSSEHandler(backend, subscriber)
		r.Get("/ojs/v1/jobs/{id}/events", sseHandler.JobEvents)
		r.Get("/ojs/v1/queues/{name}/events", sseHandler.QueueEvents)
	}

	// Events list endpoint (GET /ojs/v1/events?types=...&queues=...&limit=...)
	if routerCfg.eventLister != nil {
		r.Get("/ojs/v1/events", func(w http.ResponseWriter, r *http.Request) {
			var types, queues []string
			if t := r.URL.Query().Get("types"); t != "" {
				types = strings.Split(t, ",")
			}
			if q := r.URL.Query().Get("queues"); q != "" {
				queues = strings.Split(q, ",")
			}
			limit := 100
			if l := r.URL.Query().Get("limit"); l != "" {
				if n, err := strconv.Atoi(l); err == nil && n > 0 {
					limit = n
				}
			}

			events := routerCfg.eventLister.ListEvents(types, queues, limit)
			if events == nil {
				events = []map[string]any{}
			}
			httputil.WriteJSON(w, http.StatusOK, map[string]any{"events": events})
		})
	}

	// State reset endpoint for conformance testing
	type resettable interface{ Reset() }
	type logResettable interface{ ResetLog() }
	if rb, ok := backend.(resettable); ok {
		r.Post("/ojs/v1/admin/reset", func(w http.ResponseWriter, r *http.Request) {
			rb.Reset()
			if routerCfg.eventLister != nil {
				if lr, ok := routerCfg.eventLister.(logResettable); ok {
					lr.ResetLog()
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"reset":true}`))
		})
	}

	return r
}
