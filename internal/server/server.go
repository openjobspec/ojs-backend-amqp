package server

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	commoncore "github.com/openjobspec/ojs-go-backend-common/core"
	"github.com/openjobspec/ojs-go-backend-common/registry"
)

// NewRouter creates the HTTP router with all OJS routes.
func NewRouter(backend commoncore.Backend, cfg Config, publisher commoncore.EventPublisher, subscriber commoncore.EventSubscriber) http.Handler {
	r := chi.NewRouter()

	// Middleware stack
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)

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

	return r
}
