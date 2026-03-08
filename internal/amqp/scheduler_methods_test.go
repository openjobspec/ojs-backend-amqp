package amqp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

func TestBackend_PromoteScheduled(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Push a job with past ScheduledAt — it will start as "scheduled" only if
	// ScheduledAt is in the future at Push time. So we manipulate directly.
	job := &core.Job{
		Type:  "scheduled.test",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
	}
	pushed, err := b.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	// Manually set to scheduled with a past time
	b.mu.Lock()
	pushed.State = "scheduled"
	pushed.ScheduledAt = time.Now().Add(-1 * time.Minute).UTC().Format(time.RFC3339)
	b.adjustStats("default", "available", "scheduled")
	b.mu.Unlock()

	// Promote
	if err := b.PromoteScheduled(ctx); err != nil {
		t.Fatalf("PromoteScheduled failed: %v", err)
	}

	info, _ := b.Info(ctx, pushed.ID)
	if info.State != "available" {
		t.Errorf("expected state 'available' after promotion, got %q", info.State)
	}
}

func TestBackend_PromoteScheduled_FutureJobs(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Push a job with future ScheduledAt
	futureTime := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	job := &core.Job{
		Type:        "scheduled.future",
		Queue:       "default",
		Args:        json.RawMessage(`{}`),
		ScheduledAt: futureTime,
	}
	pushed, err := b.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	if pushed.State != "scheduled" {
		t.Fatalf("expected initial state 'scheduled', got %q", pushed.State)
	}

	// Promote should not affect future jobs
	if err := b.PromoteScheduled(ctx); err != nil {
		t.Fatalf("PromoteScheduled failed: %v", err)
	}

	info, _ := b.Info(ctx, pushed.ID)
	if info.State != "scheduled" {
		t.Errorf("expected state 'scheduled' (future job should not be promoted), got %q", info.State)
	}
}

func TestBackend_PromoteRetries(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Push, fetch, nack to make it retryable
	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "retry.promote",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
		Retry: &core.RetryPolicy{MaxAttempts: 3},
	})

	b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
	b.Nack(ctx, pushed.ID, nil, true)

	info, _ := b.Info(ctx, pushed.ID)
	if info.State != "retryable" {
		t.Fatalf("expected state 'retryable', got %q", info.State)
	}

	// Set ScheduledAt to the past so it can be promoted
	b.mu.Lock()
	info.ScheduledAt = time.Now().Add(-1 * time.Second).UTC().Format(time.RFC3339)
	b.mu.Unlock()

	if err := b.PromoteRetries(ctx); err != nil {
		t.Fatalf("PromoteRetries failed: %v", err)
	}

	info, _ = b.Info(ctx, pushed.ID)
	if info.State != "available" {
		t.Errorf("expected state 'available' after retry promotion, got %q", info.State)
	}
	if info.ScheduledAt != "" {
		t.Errorf("expected ScheduledAt to be cleared, got %q", info.ScheduledAt)
	}
}

func TestBackend_PromoteRetries_FutureBackoff(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Push, fetch, nack
	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "retry.future",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
		Retry: &core.RetryPolicy{MaxAttempts: 3},
	})

	b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
	b.Nack(ctx, pushed.ID, nil, true)

	// The nack sets ScheduledAt to a future time (backoff), so PromoteRetries should NOT promote
	if err := b.PromoteRetries(ctx); err != nil {
		t.Fatalf("PromoteRetries failed: %v", err)
	}

	info, _ := b.Info(ctx, pushed.ID)
	if info.State != "retryable" {
		t.Errorf("expected state 'retryable' (backoff not elapsed), got %q", info.State)
	}
}

func TestBackend_RequeueStalled(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "stall.test",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
	})

	b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)

	info, _ := b.Info(ctx, pushed.ID)
	if info.State != "active" {
		t.Fatalf("expected state 'active', got %q", info.State)
	}

	// Set StartedAt to long ago to simulate stall
	b.mu.Lock()
	info.StartedAt = time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	// Remove worker so heartbeat check doesn't save it
	delete(b.workers, "worker-1")
	b.mu.Unlock()

	if err := b.RequeueStalled(ctx); err != nil {
		t.Fatalf("RequeueStalled failed: %v", err)
	}

	info, _ = b.Info(ctx, pushed.ID)
	if info.State != "available" {
		t.Errorf("expected state 'available' after requeue, got %q", info.State)
	}
	if info.StartedAt != "" {
		t.Errorf("expected StartedAt to be cleared, got %q", info.StartedAt)
	}
	if info.WorkerID != "" {
		t.Errorf("expected WorkerID to be cleared, got %q", info.WorkerID)
	}
}

func TestBackend_RequeueStalled_ActiveWorker(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "stall.active_worker",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
	})

	b.Fetch(ctx, []string{"default"}, 1, "worker-alive", 30000)

	// Set StartedAt to long ago, but worker has a recent heartbeat
	b.mu.Lock()
	info := b.jobs[pushed.ID]
	info.StartedAt = time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339Nano)
	b.workers["worker-alive"].LastHeartbeat = time.Now().UTC().Format(time.RFC3339Nano)
	b.mu.Unlock()

	if err := b.RequeueStalled(ctx); err != nil {
		t.Fatalf("RequeueStalled failed: %v", err)
	}

	result, _ := b.Info(ctx, pushed.ID)
	if result.State != "active" {
		t.Errorf("expected state 'active' (worker is alive), got %q", result.State)
	}
}

func TestBackend_FireCronJobs(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	pastTime := time.Now().Add(-1 * time.Minute).UTC().Format(time.RFC3339)

	cron := &core.CronJob{
		Name:       "fire-test-cron",
		Expression: "* * * * *",
		Enabled:    true,
		NextRunAt:  pastTime,
		JobTemplate: &core.CronJobTemplate{
			Type: "cron.fired",
			Args: json.RawMessage(`{"from":"cron"}`),
		},
	}

	if _, err := b.RegisterCron(ctx, cron); err != nil {
		t.Fatalf("RegisterCron failed: %v", err)
	}

	if err := b.FireCronJobs(ctx); err != nil {
		t.Fatalf("FireCronJobs failed: %v", err)
	}

	// Verify a job was created
	jobs, total, err := b.ListJobs(ctx, core.JobListFilters{Type: "cron.fired"}, 100, 0)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if total < 1 {
		t.Errorf("expected at least 1 fired cron job, got %d", total)
	}
	if len(jobs) < 1 {
		t.Fatal("expected at least 1 job in results")
	}
	if jobs[0].Type != "cron.fired" {
		t.Errorf("expected job type 'cron.fired', got %q", jobs[0].Type)
	}
	// Verify cron tag is present
	found := false
	for _, tag := range jobs[0].Tags {
		if tag == "cron:fire-test-cron" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected cron tag 'cron:fire-test-cron', got tags %v", jobs[0].Tags)
	}
}

func TestBackend_FireCronJobs_Disabled(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	pastTime := time.Now().Add(-1 * time.Minute).UTC().Format(time.RFC3339)

	cron := &core.CronJob{
		Name:       "disabled-cron",
		Expression: "* * * * *",
		Enabled:    false,
		NextRunAt:  pastTime,
		JobTemplate: &core.CronJobTemplate{
			Type: "cron.disabled",
			Args: json.RawMessage(`{}`),
		},
	}

	// RegisterCron sets Enabled=true, so override after
	b.RegisterCron(ctx, cron)
	b.mu.Lock()
	b.crons["disabled-cron"].Enabled = false
	b.mu.Unlock()

	b.FireCronJobs(ctx)

	jobs, _, _ := b.ListJobs(ctx, core.JobListFilters{Type: "cron.disabled"}, 100, 0)
	if len(jobs) != 0 {
		t.Errorf("disabled cron should not fire, got %d jobs", len(jobs))
	}
}

func TestBackend_PurgeExpired(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Push and complete a job
	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "purge.test",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
	})

	b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
	b.Ack(ctx, pushed.ID, nil)

	// Set CompletedAt to 48 hours ago
	b.mu.Lock()
	b.jobs[pushed.ID].CompletedAt = time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
	b.mu.Unlock()

	if err := b.PurgeExpired(ctx); err != nil {
		t.Fatalf("PurgeExpired failed: %v", err)
	}

	_, err := b.Info(ctx, pushed.ID)
	if err == nil {
		t.Error("expected job to be purged (not found)")
	}
}

func TestBackend_PurgeExpired_RecentJobs(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Push and complete a job (completed just now)
	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "purge.recent",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
	})

	b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
	b.Ack(ctx, pushed.ID, nil)

	if err := b.PurgeExpired(ctx); err != nil {
		t.Fatalf("PurgeExpired failed: %v", err)
	}

	info, err := b.Info(ctx, pushed.ID)
	if err != nil {
		t.Fatalf("recent completed job should NOT be purged: %v", err)
	}
	if info.State != "completed" {
		t.Errorf("expected state 'completed', got %q", info.State)
	}
}

func TestBackend_PurgeExpired_CancelledJobs(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "purge.cancelled",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
	})

	b.Cancel(ctx, pushed.ID)

	// Set CancelledAt to 48 hours ago
	b.mu.Lock()
	b.jobs[pushed.ID].CancelledAt = time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
	b.mu.Unlock()

	b.PurgeExpired(ctx)

	_, err := b.Info(ctx, pushed.ID)
	if err == nil {
		t.Error("expected cancelled job to be purged")
	}
}

func TestBackend_PromoteScheduled_Multiple(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Create multiple scheduled jobs with past times
	var ids []string
	for i := 0; i < 5; i++ {
		pushed, _ := b.Push(ctx, &core.Job{
			Type:  "scheduled.multi",
			Queue: "default",
			Args:  json.RawMessage(`{}`),
		})
		ids = append(ids, pushed.ID)
	}

	// Set all to scheduled with past time
	b.mu.Lock()
	for _, id := range ids {
		b.jobs[id].State = "scheduled"
		b.jobs[id].ScheduledAt = time.Now().Add(-1 * time.Minute).UTC().Format(time.RFC3339)
		b.adjustStats("default", "available", "scheduled")
	}
	b.mu.Unlock()

	b.PromoteScheduled(ctx)

	for _, id := range ids {
		info, _ := b.Info(ctx, id)
		if info.State != "available" {
			t.Errorf("job %s: expected 'available', got %q", id, info.State)
		}
	}
}
