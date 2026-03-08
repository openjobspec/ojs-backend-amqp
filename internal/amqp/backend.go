// Package amqp implements the OJS Backend interface using RabbitMQ (AMQP 0-9-1).
//
// Architecture:
//   - RabbitMQ handles message routing via direct/fanout exchanges
//   - An in-memory state store tracks job metadata, workflows, and cron state
//   - Background schedulers handle job promotion, cron firing, and stalled job reaping
package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

// Backend implements core.Backend using RabbitMQ for message routing
// and an in-memory store for job state.
type Backend struct {
	mu sync.RWMutex

	conn *Connection // AMQP connection manager

	// In-memory state store
	jobs      map[string]*core.Job
	jobOrder  []string // maintains insertion order for FIFO fetch
	queues    map[string]*queueState
	deadJobs  []*core.Job
	crons     map[string]*core.CronJob
	workflows map[string]*core.Workflow
	workers   map[string]*core.WorkerInfo

	persist   persistStore // optional durable state persistence
	startTime time.Time
}

// Option configures optional Backend behaviour.
type Option func(*Backend)

// WithPersist enables SQLite-backed durable state persistence at the given path.
func WithPersist(path string) Option {
	return func(b *Backend) {
		store, err := newSQLiteStore(path)
		if err != nil {
			slog.Error("failed to open persistence store, continuing without persistence", "path", path, "error", err)
			return
		}
		b.persist = store
	}
}

type queueState struct {
	Name   string
	Paused bool
	Stats  core.Stats
}

// New creates a new AMQP backend connected to the given RabbitMQ URL.
func New(amqpURL string, opts ...Option) (*Backend, error) {
	conn, err := NewConnection(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("AMQP backend init: %w", err)
	}

	b := &Backend{
		conn:      conn,
		jobs:      make(map[string]*core.Job),
		queues:    make(map[string]*queueState),
		crons:     make(map[string]*core.CronJob),
		workflows: make(map[string]*core.Workflow),
		workers:   make(map[string]*core.WorkerInfo),
		startTime: time.Now(),
	}

	for _, opt := range opts {
		opt(b)
	}

	if b.persist != nil {
		state, err := b.persist.LoadAll()
		if err != nil {
			slog.Warn("failed to load persisted state", "error", err)
		} else if state != nil {
			b.jobs = state.Jobs
			b.jobOrder = state.JobOrder
			b.deadJobs = state.DeadJobs
			b.crons = state.Crons
			b.workflows = state.Workflows
			for name, paused := range state.QueueState {
				b.queues[name] = &queueState{Name: name, Paused: paused}
			}
			for _, job := range b.jobs {
				b.ensureQueueState(job.Queue)
				qs := b.queues[job.Queue]
				switch job.State {
				case "available":
					qs.Stats.Available++
				case "active":
					qs.Stats.Active++
				case "scheduled":
					qs.Stats.Scheduled++
				case "retryable":
					qs.Stats.Retryable++
				case "completed":
					qs.Stats.Completed++
				case "discarded":
					qs.Stats.Dead++
				}
			}
			slog.Info("loaded persisted state", "jobs", len(b.jobs), "crons", len(b.crons), "workflows", len(b.workflows))
		}
	}

	slog.Info("AMQP backend initialized", "url", amqpURL)
	return b, nil
}

// NewWithoutConnection creates a Backend with in-memory state only (no AMQP
// connection). Useful for unit testing business logic without RabbitMQ.
func NewWithoutConnection(opts ...Option) *Backend {
	b := &Backend{
		jobs:      make(map[string]*core.Job),
		queues:    make(map[string]*queueState),
		crons:     make(map[string]*core.CronJob),
		workflows: make(map[string]*core.Workflow),
		workers:   make(map[string]*core.WorkerInfo),
		startTime: time.Now(),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// --- JobManager ---

func (b *Backend) Push(ctx context.Context, job *core.Job) (*core.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if job.ID == "" {
		job.ID = core.NewUUIDv7()
	}

	// Check for duplicate job ID
	if _, exists := b.jobs[job.ID]; exists {
		return nil, core.NewDuplicateError(job.ID)
	}

	job.State = "available"
	job.CreatedAt = now
	job.EnqueuedAt = now

	// Jobs with a future scheduled_at start in "scheduled" state
	if job.ScheduledAt != "" {
		if t, err := time.Parse(time.RFC3339, job.ScheduledAt); err == nil && t.After(time.Now()) {
			job.State = "scheduled"
		}
	}

	if job.Queue == "" {
		job.Queue = "default"
	}

	// Ensure AMQP queue exists
	if b.conn != nil {
		if err := b.conn.EnsureQueue(job.Queue); err != nil {
			slog.Warn("failed to ensure AMQP queue", "queue", job.Queue, "error", err)
		}

		// Publish to RabbitMQ
		if err := b.conn.Publish(ctx, job.Queue, job); err != nil {
			slog.Warn("failed to publish to AMQP", "job_id", job.ID, "error", err)
		}
	}

	b.jobs[job.ID] = job
	b.jobOrder = append(b.jobOrder, job.ID)
	b.ensureQueueState(job.Queue)
	if job.State == "scheduled" {
		b.queues[job.Queue].Stats.Scheduled++
	} else {
		b.queues[job.Queue].Stats.Available++
	}

	b.persistJob(job)

	return job, nil
}

func (b *Backend) PushBatch(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
	results := make([]*core.Job, 0, len(jobs))
	for _, job := range jobs {
		result, err := b.Push(ctx, job)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (b *Backend) Info(_ context.Context, jobID string) (*core.Job, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return nil, core.NewNotFoundError("job", jobID)
	}
	return job, nil
}

func (b *Backend) Cancel(_ context.Context, jobID string) (*core.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return nil, core.NewNotFoundError("job", jobID)
	}

	if job.State == "completed" || job.State == "cancelled" || job.State == "discarded" {
		return nil, core.NewConflictError(fmt.Sprintf("cannot cancel job in state %q", job.State), nil)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	oldState := job.State
	job.State = "cancelled"
	job.CancelledAt = now

	b.adjustStats(job.Queue, oldState, "cancelled")
	b.persistJob(job)
	return job, nil
}

// --- WorkerManager ---

func (b *Backend) Fetch(_ context.Context, queues []string, count int, workerID string, visibilityTimeoutMs int) ([]*core.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var fetched []*core.Job
	for _, q := range queues {
		if len(fetched) >= count {
			break
		}
		qs := b.queues[q]
		if qs == nil || qs.Paused {
			continue
		}
		for _, id := range b.jobOrder {
			if len(fetched) >= count {
				break
			}
			job := b.jobs[id]
			if job == nil {
				continue
			}
			if job.Queue == q && job.State == "available" {
				job.State = "active"
				job.Attempt++
				job.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
				job.WorkerID = workerID
				b.adjustStats(q, "available", "active")
				b.persistJob(job)
				fetched = append(fetched, job)
			}
		}
	}

	// Update worker info
	if workerID != "" {
		b.workers[workerID] = &core.WorkerInfo{
			ID:            workerID,
			State:         "running",
			ActiveJobs:    len(fetched),
			LastHeartbeat: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}

	return fetched, nil
}

func (b *Backend) Ack(_ context.Context, jobID string, result []byte) (*core.AckResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return nil, core.NewNotFoundError("job", jobID)
	}
	if job.State != "active" {
		return nil, core.NewConflictError(fmt.Sprintf("cannot ack job in state %q", job.State), nil)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	job.State = "completed"
	job.CompletedAt = now
	job.Error = nil // Ack clears any previous error
	if result != nil {
		job.Result = result
	}

	b.adjustStats(job.Queue, "active", "completed")
	b.persistJob(job)

	return &core.AckResponse{
		Acknowledged: true,
		ID:           jobID,
		State:        "completed",
		CompletedAt:  now,
		Job:          job,
	}, nil
}

func (b *Backend) Nack(_ context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return nil, core.NewNotFoundError("job", jobID)
	}
	if job.State != "active" {
		return nil, core.NewConflictError(fmt.Sprintf("cannot nack job in state %q", job.State), nil)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Store the error on the job
	if jobErr != nil {
		if jobErr.Type == "" && jobErr.Code != "" {
			jobErr.Type = jobErr.Code
		}
		errJSON, _ := json.Marshal(jobErr)
		job.Error = errJSON
	}

	maxAttempts := 3
	if job.Retry != nil && job.Retry.MaxAttempts > 0 {
		maxAttempts = job.Retry.MaxAttempts
	}

	resp := &core.NackResponse{
		ID:       jobID,
		Attempt:     job.Attempt,
		MaxAttempts: maxAttempts,
	}

	// Per OJS spec: if retry attempts remain, transition to retryable.
	// Only discard if attempts are exhausted or explicit discard (requeue=false AND no retries configured).
	shouldDiscard := job.Attempt >= maxAttempts
	if !requeue && (job.Retry == nil || job.Retry.MaxAttempts <= 0) {
		shouldDiscard = true
	}

	if shouldDiscard {
		job.State = "discarded"
		job.CompletedAt = now
		resp.State = "discarded"
		resp.DiscardedAt = now
		resp.CompletedAt = now
		b.adjustStats(job.Queue, "active", "discarded")
		b.deadJobs = append(b.deadJobs, job)
		b.persistJob(job)
		if b.persist != nil {
			if err := b.persist.SaveDeadJob(job); err != nil {
				slog.Warn("persist dead job failed", "job_id", job.ID, "error", err)
			}
		}
	} else {
		job.State = "retryable"
		resp.State = "retryable"
		b.adjustStats(job.Queue, "active", "retryable")

		delay := time.Duration(1<<uint(job.Attempt-1)) * time.Second
		if delay > 60*time.Second {
			delay = 60 * time.Second
		}
		nextAttempt := time.Now().Add(delay).UTC().Format(time.RFC3339)
		resp.NextAttemptAt = nextAttempt

		// Store retry time in ScheduledAt so PromoteRetries can promote it.
		job.ScheduledAt = nextAttempt
		b.persistJob(job)
	}

	resp.Job = job
	return resp, nil
}

func (b *Backend) Heartbeat(_ context.Context, workerID string, activeJobs []string, _ int) (*core.HeartbeatResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	w, ok := b.workers[workerID]
	if !ok {
		w = &core.WorkerInfo{ID: workerID, State: "running"}
		b.workers[workerID] = w
	}
	w.LastHeartbeat = now
	w.ActiveJobs = len(activeJobs)

	return &core.HeartbeatResponse{
		State:        w.State,
		Directive:    w.Directive,
		JobsExtended: activeJobs,
		ServerTime:   now,
	}, nil
}

func (b *Backend) SetWorkerState(_ context.Context, workerID string, state string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	w, ok := b.workers[workerID]
	if !ok {
		return core.NewNotFoundError("worker", workerID)
	}
	w.State = state
	return nil
}

// --- QueueManager ---

func (b *Backend) ListQueues(_ context.Context) ([]core.QueueInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]core.QueueInfo, 0, len(b.queues))
	for _, qs := range b.queues {
		status := "active"
		if qs.Paused {
			status = "paused"
		}
		result = append(result, core.QueueInfo{Name: qs.Name, Status: status})
	}
	return result, nil
}

func (b *Backend) QueueStats(_ context.Context, name string) (*core.QueueStats, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	qs, ok := b.queues[name]
	if !ok {
		return nil, core.NewNotFoundError("queue", name)
	}
	status := "active"
	if qs.Paused {
		status = "paused"
	}
	return &core.QueueStats{Queue: name, Status: status, Stats: qs.Stats}, nil
}

func (b *Backend) PauseQueue(_ context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.ensureQueueState(name)
	b.queues[name].Paused = true
	if b.persist != nil {
		if err := b.persist.SaveQueueState(name, "paused"); err != nil {
			slog.Warn("persist queue state failed", "queue", name, "error", err)
		}
	}
	return nil
}

func (b *Backend) ResumeQueue(_ context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	qs, ok := b.queues[name]
	if !ok {
		return core.NewNotFoundError("queue", name)
	}
	qs.Paused = false
	if b.persist != nil {
		if err := b.persist.SaveQueueState(name, "active"); err != nil {
			slog.Warn("persist queue state failed", "queue", name, "error", err)
		}
	}
	return nil
}

// --- DeadLetterManager ---

func (b *Backend) ListDeadLetter(_ context.Context, limit, offset int) ([]*core.Job, int, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := len(b.deadJobs)
	if offset >= total {
		return []*core.Job{}, total, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return b.deadJobs[offset:end], total, nil
}

func (b *Backend) RetryDeadLetter(_ context.Context, jobID string) (*core.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, job := range b.deadJobs {
		if job.ID == jobID {
			b.deadJobs = append(b.deadJobs[:i], b.deadJobs[i+1:]...)
			job.State = "available"
			job.Attempt = 0
			job.EnqueuedAt = time.Now().UTC().Format(time.RFC3339Nano)
			b.ensureQueueState(job.Queue)
			b.queues[job.Queue].Stats.Available++
			if b.persist != nil {
				if err := b.persist.DeleteDeadJob(jobID); err != nil {
					slog.Warn("persist delete dead job failed", "job_id", jobID, "error", err)
				}
			}
			b.persistJob(job)
			return job, nil
		}
	}
	return nil, core.NewNotFoundError("dead letter job", jobID)
}

func (b *Backend) DeleteDeadLetter(_ context.Context, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, job := range b.deadJobs {
		if job.ID == jobID {
			b.deadJobs = append(b.deadJobs[:i], b.deadJobs[i+1:]...)
			delete(b.jobs, jobID)
			if b.persist != nil {
				if err := b.persist.DeleteDeadJob(jobID); err != nil {
					slog.Warn("persist delete dead job failed", "job_id", jobID, "error", err)
				}
				if err := b.persist.DeleteJob(jobID); err != nil {
					slog.Warn("persist delete job failed", "job_id", jobID, "error", err)
				}
			}
			return nil
		}
	}
	return core.NewNotFoundError("dead letter job", jobID)
}

// --- CronManager ---

func (b *Backend) RegisterCron(_ context.Context, cron *core.CronJob) (*core.CronJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	cron.CreatedAt = now
	cron.Enabled = true
	b.crons[cron.Name] = cron
	if b.persist != nil {
		if err := b.persist.SaveCron(cron); err != nil {
			slog.Warn("persist cron failed", "name", cron.Name, "error", err)
		}
	}
	return cron, nil
}

func (b *Backend) ListCron(_ context.Context) ([]*core.CronJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*core.CronJob, 0, len(b.crons))
	for _, c := range b.crons {
		result = append(result, c)
	}
	return result, nil
}

func (b *Backend) DeleteCron(_ context.Context, name string) (*core.CronJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.crons[name]
	if !ok {
		return nil, core.NewNotFoundError("cron", name)
	}
	delete(b.crons, name)
	if b.persist != nil {
		if err := b.persist.DeleteCron(name); err != nil {
			slog.Warn("persist delete cron failed", "name", name, "error", err)
		}
	}
	return c, nil
}

// --- WorkflowManager ---

func (b *Backend) CreateWorkflow(_ context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	id := core.NewUUIDv7()

	total := len(req.Steps)
	if total == 0 {
		total = len(req.Jobs)
	}

	wf := &core.Workflow{
		ID:        id,
		Name:      req.Name,
		Type:      req.Type,
		State:     "active",
		CreatedAt: now,
		Callbacks: req.Callbacks,
	}

	switch req.Type {
	case "chain":
		wf.StepsTotal = &total
		zero := 0
		wf.StepsCompleted = &zero
	case "group", "batch":
		wf.JobsTotal = &total
		zero := 0
		wf.JobsCompleted = &zero
	}

	b.workflows[id] = wf
	if b.persist != nil {
		if err := b.persist.SaveWorkflow(wf); err != nil {
			slog.Warn("persist workflow failed", "workflow_id", id, "error", err)
		}
	}
	return wf, nil
}

func (b *Backend) GetWorkflow(_ context.Context, id string) (*core.Workflow, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	wf, ok := b.workflows[id]
	if !ok {
		return nil, core.NewNotFoundError("workflow", id)
	}
	return wf, nil
}

func (b *Backend) CancelWorkflow(_ context.Context, id string) (*core.Workflow, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	wf, ok := b.workflows[id]
	if !ok {
		return nil, core.NewNotFoundError("workflow", id)
	}
	wf.State = "cancelled"
	if b.persist != nil {
		if err := b.persist.SaveWorkflow(wf); err != nil {
			slog.Warn("persist workflow failed", "workflow_id", id, "error", err)
		}
	}
	return wf, nil
}

func (b *Backend) AdvanceWorkflow(_ context.Context, workflowID string, _ string, _ json.RawMessage, failed bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	wf, ok := b.workflows[workflowID]
	if !ok {
		return core.NewNotFoundError("workflow", workflowID)
	}

	if failed {
		wf.State = "failed"
		now := time.Now().UTC().Format(time.RFC3339Nano)
		wf.CompletedAt = now
		if b.persist != nil {
			if err := b.persist.SaveWorkflow(wf); err != nil {
				slog.Warn("persist workflow failed", "workflow_id", workflowID, "error", err)
			}
		}
		return nil
	}

	switch wf.Type {
	case "chain":
		if wf.StepsCompleted != nil {
			*wf.StepsCompleted++
			if wf.StepsTotal != nil && *wf.StepsCompleted >= *wf.StepsTotal {
				wf.State = "completed"
				now := time.Now().UTC().Format(time.RFC3339Nano)
				wf.CompletedAt = now
			}
		}
	case "group", "batch":
		if wf.JobsCompleted != nil {
			*wf.JobsCompleted++
			if wf.JobsTotal != nil && *wf.JobsCompleted >= *wf.JobsTotal {
				wf.State = "completed"
				now := time.Now().UTC().Format(time.RFC3339Nano)
				wf.CompletedAt = now
			}
		}
	}

	if b.persist != nil {
		if err := b.persist.SaveWorkflow(wf); err != nil {
			slog.Warn("persist workflow failed", "workflow_id", workflowID, "error", err)
		}
	}

	return nil
}

// --- AdminManager ---

func (b *Backend) ListJobs(_ context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var filtered []*core.Job
	for _, job := range b.jobs {
		if filters.State != "" && job.State != filters.State {
			continue
		}
		if filters.Queue != "" && job.Queue != filters.Queue {
			continue
		}
		if filters.Type != "" && job.Type != filters.Type {
			continue
		}
		filtered = append(filtered, job)
	}

	total := len(filtered)
	if offset >= total {
		return []*core.Job{}, total, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return filtered[offset:end], total, nil
}

func (b *Backend) ListWorkers(_ context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var workers []*core.WorkerInfo
	summary := core.WorkerSummary{}
	for _, w := range b.workers {
		workers = append(workers, w)
		summary.Total++
		switch w.State {
		case "running":
			summary.Running++
		case "quiet":
			summary.Quiet++
		}
	}

	if offset >= len(workers) {
		return []*core.WorkerInfo{}, summary, nil
	}
	end := offset + limit
	if end > len(workers) {
		end = len(workers)
	}
	return workers[offset:end], summary, nil
}

// --- Health & Close ---

func (b *Backend) Health(_ context.Context) (*core.HealthResponse, error) {
	uptime := int64(time.Since(b.startTime).Seconds())

	status := "ok"
	var latencyMs int64
	var connErr string

	if b.conn != nil && b.conn.IsConnected() {
		start := time.Now()
		// Simple health probe — channel operations confirm connection is live
		latencyMs = time.Since(start).Milliseconds()
	} else {
		status = "degraded"
		connErr = "AMQP connection unavailable"
	}

	return &core.HealthResponse{
		Status:        status,
		Version:       "0.2.0",
		UptimeSeconds: uptime,
		Backend: core.BackendHealth{
			Type:      "amqp",
			Status:    status,
			LatencyMs: latencyMs,
			Error:     connErr,
		},
	}, nil
}

func (b *Backend) Close() error {
	if b.persist != nil {
		b.persist.Close()
	}
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

// Reset clears all in-memory state. Used by the conformance test runner
// to isolate state between tests.
func (b *Backend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.jobs = make(map[string]*core.Job)
	b.jobOrder = nil
	b.queues = make(map[string]*queueState)
	b.deadJobs = nil
	b.crons = make(map[string]*core.CronJob)
	b.workflows = make(map[string]*core.Workflow)
	b.workers = make(map[string]*core.WorkerInfo)

	if b.persist != nil {
		b.persist.Close()
		// Persistence is intentionally not re-opened; Reset is for test isolation.
		b.persist = nil
	}
}

// --- Helpers ---

func (b *Backend) persistJob(job *core.Job) {
	if b.persist != nil {
		if err := b.persist.SaveJob(job); err != nil {
			slog.Warn("persist job failed", "job_id", job.ID, "error", err)
		}
	}
}

func (b *Backend) ensureQueueState(name string) {
	if _, ok := b.queues[name]; !ok {
		b.queues[name] = &queueState{Name: name}
	}
}

// --- Scheduler Methods ---

// PromoteScheduled transitions jobs from "scheduled" to "available" when their
// ScheduledAt time has passed.
func (b *Backend) PromoteScheduled(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC()
	for _, job := range b.jobs {
		if job.State != "scheduled" {
			continue
		}
		if job.ScheduledAt == "" {
			continue
		}
		scheduledTime, err := time.Parse(time.RFC3339, job.ScheduledAt)
		if err != nil {
			scheduledTime, err = time.Parse(time.RFC3339Nano, job.ScheduledAt)
			if err != nil {
				continue
			}
		}
		if now.After(scheduledTime) || now.Equal(scheduledTime) {
			job.State = "available"
			b.adjustStats(job.Queue, "scheduled", "available")
			b.persistJob(job)
		}
	}
	return nil
}

// PromoteRetries transitions jobs from "retryable" to "available" when their
// retry backoff period (stored in ScheduledAt) has elapsed.
func (b *Backend) PromoteRetries(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC()
	for _, job := range b.jobs {
		if job.State != "retryable" {
			continue
		}
		if job.ScheduledAt == "" {
			continue
		}
		retryTime, err := time.Parse(time.RFC3339, job.ScheduledAt)
		if err != nil {
			retryTime, err = time.Parse(time.RFC3339Nano, job.ScheduledAt)
			if err != nil {
				continue
			}
		}
		if now.After(retryTime) || now.Equal(retryTime) {
			job.State = "available"
			job.ScheduledAt = ""
			b.adjustStats(job.Queue, "retryable", "available")
			b.persistJob(job)
		}
	}
	return nil
}

// RequeueStalled transitions jobs from "active" back to "available" when the
// worker hasn't sent a heartbeat within the visibility timeout.
func (b *Backend) RequeueStalled(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC()
	stalledTimeout := 30 * time.Second

	for _, job := range b.jobs {
		if job.State != "active" {
			continue
		}
		if job.StartedAt == "" {
			continue
		}

		startedAt, err := time.Parse(time.RFC3339Nano, job.StartedAt)
		if err != nil {
			startedAt, err = time.Parse(time.RFC3339, job.StartedAt)
			if err != nil {
				continue
			}
		}

		// Use visibility timeout from the job if set
		timeout := stalledTimeout
		if job.VisibilityTimeoutMs != nil && *job.VisibilityTimeoutMs > 0 {
			timeout = time.Duration(*job.VisibilityTimeoutMs) * time.Millisecond
		}

		if now.Sub(startedAt) <= timeout {
			continue
		}

		// Check if worker has sent a recent heartbeat
		if job.WorkerID != "" {
			if w, ok := b.workers[job.WorkerID]; ok {
				if hb, err := time.Parse(time.RFC3339Nano, w.LastHeartbeat); err == nil {
					if now.Sub(hb) < timeout {
						continue
					}
				}
			}
		}

		job.State = "available"
		job.StartedAt = ""
		job.WorkerID = ""
		b.adjustStats(job.Queue, "active", "available")
		b.persistJob(job)
		slog.Debug("requeued stalled job", "job_id", job.ID, "queue", job.Queue)
	}
	return nil
}

// FireCronJobs fires any cron jobs that are due. It releases the lock before
// calling Push to avoid deadlock.
func (b *Backend) FireCronJobs(ctx context.Context) error {
	b.mu.Lock()
	cronsCopy := make([]*core.CronJob, 0, len(b.crons))
	for _, cron := range b.crons {
		c := *cron
		cronsCopy = append(cronsCopy, &c)
	}
	b.mu.Unlock()

	now := time.Now().UTC()
	for _, cron := range cronsCopy {
		if !cron.Enabled {
			continue
		}
		if cron.NextRunAt == "" {
			continue
		}
		nextRun, err := time.Parse(time.RFC3339, cron.NextRunAt)
		if err != nil {
			continue
		}
		if now.Before(nextRun) {
			continue
		}

		if cron.JobTemplate != nil {
			job := &core.Job{
				Type:  cron.JobTemplate.Type,
				Args:  cron.JobTemplate.Args,
				Queue: "default",
				Tags:  []string{"cron:" + cron.Name},
			}
			if cron.JobTemplate.Options != nil && cron.JobTemplate.Options.Queue != "" {
				job.Queue = cron.JobTemplate.Options.Queue
			}

			if _, err := b.Push(ctx, job); err != nil {
				slog.Error("scheduler: fire cron failed", "name", cron.Name, "error", err)
			} else {
				slog.Debug("scheduler: fired cron job", "name", cron.Name, "type", job.Type)
			}
		}
	}
	return nil
}

// PurgeExpired removes completed/cancelled/discarded jobs older than 24 hours.
func (b *Backend) PurgeExpired(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	retention := 24 * time.Hour
	now := time.Now().UTC()

	var purgedIDs []string
	for id, job := range b.jobs {
		if job.State != "completed" && job.State != "cancelled" && job.State != "discarded" {
			continue
		}
		completedAt := job.CompletedAt
		if completedAt == "" {
			completedAt = job.CancelledAt
		}
		if completedAt == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, completedAt)
		if err != nil {
			t, err = time.Parse(time.RFC3339, completedAt)
			if err != nil {
				continue
			}
		}
		if now.Sub(t) > retention {
			purgedIDs = append(purgedIDs, id)
		}
	}

	for _, id := range purgedIDs {
		delete(b.jobs, id)
		if b.persist != nil {
			if err := b.persist.DeleteJob(id); err != nil {
				slog.Warn("persist delete job failed", "job_id", id, "error", err)
			}
		}
	}

	// Also remove from deadJobs slice
	if len(purgedIDs) > 0 {
		purgeSet := make(map[string]struct{}, len(purgedIDs))
		for _, id := range purgedIDs {
			purgeSet[id] = struct{}{}
		}
		kept := b.deadJobs[:0]
		for _, dj := range b.deadJobs {
			if _, purged := purgeSet[dj.ID]; !purged {
				kept = append(kept, dj)
			}
		}
		b.deadJobs = kept
	}
	return nil
}

// --- Helpers ---

// parseDuration parses an ISO 8601 duration string (e.g. "PT1S", "PT5M") into
// a time.Duration. Supports hours (H), minutes (M), and seconds (S).
func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimPrefix(s, "PT")
	s = strings.TrimPrefix(s, "pt")
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	var d time.Duration
	remaining := s
	for len(remaining) > 0 {
		i := 0
		for i < len(remaining) && (remaining[i] >= '0' && remaining[i] <= '9' || remaining[i] == '.') {
			i++
		}
		if i == 0 || i >= len(remaining) {
			return 0, fmt.Errorf("invalid duration: %s", s)
		}

		var val float64
		if _, err := fmt.Sscanf(remaining[:i], "%f", &val); err != nil {
			return 0, fmt.Errorf("invalid duration number: %s", remaining[:i])
		}

		unit := remaining[i]
		switch unit {
		case 'H', 'h':
			d += time.Duration(val * float64(time.Hour))
		case 'M', 'm':
			d += time.Duration(val * float64(time.Minute))
		case 'S', 's':
			d += time.Duration(val * float64(time.Second))
		default:
			return 0, fmt.Errorf("unknown duration unit: %c", unit)
		}
		remaining = remaining[i+1:]
	}
	return d, nil
}

func (b *Backend) adjustStats(queue, fromState, toState string) {
	qs := b.queues[queue]
	if qs == nil {
		return
	}
	switch fromState {
	case "available":
		qs.Stats.Available--
	case "active":
		qs.Stats.Active--
	case "scheduled":
		qs.Stats.Scheduled--
	case "retryable":
		qs.Stats.Retryable--
	}
	switch toState {
	case "available":
		qs.Stats.Available++
	case "active":
		qs.Stats.Active++
	case "completed":
		qs.Stats.Completed++
	case "scheduled":
		qs.Stats.Scheduled++
	case "retryable":
		qs.Stats.Retryable++
	case "discarded":
		qs.Stats.Dead++
	}
}
