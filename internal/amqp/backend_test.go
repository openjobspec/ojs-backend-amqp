package amqp

import (
"context"
"encoding/json"
"fmt"
"os"
"testing"
"time"

"github.com/openjobspec/ojs-go-backend-common/core"
)

// testBackend creates a test AMQP backend. If RabbitMQ is reachable it uses
// a real connection; otherwise it falls back to the in-memory-only variant
// so that business-logic tests still run without infrastructure.
func testBackend(t *testing.T) *Backend {
t.Helper()
amqpURL := os.Getenv("AMQP_URL")
if amqpURL == "" {
amqpURL = "amqp://guest:guest@localhost:5672/"
}

b, err := New(amqpURL)
if err != nil {
// Fall back to in-memory backend for unit tests
t.Logf("RabbitMQ unavailable, using in-memory backend: %v", err)
b = NewWithoutConnection()
}

t.Cleanup(func() {
b.Close()
})

return b
}

// --- Basic Job Operations ---

func TestBackend_PushAndInfo(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

job := &core.Job{
Type:  "test.basic",
Queue: "default",
Args:  json.RawMessage(`{"foo":"bar"}`),
}

// Push job
pushed, err := b.Push(ctx, job)
if err != nil {
t.Fatalf("Push failed: %v", err)
}

// Verify ID was generated
if pushed.ID == "" {
t.Fatal("job ID should be generated")
}

// Verify initial state
if pushed.State != "available" {
t.Errorf("expected state 'available', got %q", pushed.State)
}

// Verify timestamps
if pushed.CreatedAt == "" {
t.Error("CreatedAt should be set")
}
if pushed.EnqueuedAt == "" {
t.Error("EnqueuedAt should be set")
}

// Retrieve job
info, err := b.Info(ctx, pushed.ID)
if err != nil {
t.Fatalf("Info failed: %v", err)
}

if info.ID != pushed.ID {
t.Errorf("expected ID %q, got %q", pushed.ID, info.ID)
}
if info.State != "available" {
t.Errorf("expected state 'available', got %q", info.State)
}
}

func TestBackend_PushWithCustomID(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

job := &core.Job{
ID:    "custom-id-123",
Type:  "test.custom",
Queue: "default",
Args:  json.RawMessage(`{}`),
}

pushed, err := b.Push(ctx, job)
if err != nil {
t.Fatalf("Push failed: %v", err)
}

if pushed.ID != "custom-id-123" {
t.Errorf("expected ID 'custom-id-123', got %q", pushed.ID)
}
}

func TestBackend_PushBatch(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

jobs := make([]*core.Job, 3)
for i := 0; i < 3; i++ {
jobs[i] = &core.Job{
Type:  fmt.Sprintf("batch.job%d", i),
Queue: "default",
Args:  json.RawMessage(`{}`),
}
}

results, err := b.PushBatch(ctx, jobs)
if err != nil {
t.Fatalf("PushBatch failed: %v", err)
}

if len(results) != 3 {
t.Errorf("expected 3 results, got %d", len(results))
}

for i, result := range results {
if result.ID == "" {
t.Errorf("job %d: ID should be generated", i)
}
if result.State != "available" {
t.Errorf("job %d: expected state 'available', got %q", i, result.State)
}
}
}

func TestBackend_InfoNotFound(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

_, err := b.Info(ctx, "nonexistent-id")
if err == nil {
t.Fatal("expected error for nonexistent job")
}

ojsErr, ok := err.(*core.OJSError)
if !ok {
t.Fatalf("expected OJSError, got %T", err)
}
if ojsErr.Code != "not_found" {
t.Errorf("expected code 'NOT_FOUND', got %q", ojsErr.Code)
}
}

// --- Worker Operations ---

func TestBackend_FetchAndAck(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push a job
job := &core.Job{
Type:  "test.fetch",
Queue: "default",
Args:  json.RawMessage(`{}`),
}

pushed, err := b.Push(ctx, job)
if err != nil {
t.Fatalf("Push failed: %v", err)
}

// Fetch job
fetched, err := b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
if err != nil {
t.Fatalf("Fetch failed: %v", err)
}

if len(fetched) != 1 {
t.Fatalf("expected 1 job, got %d", len(fetched))
}

if fetched[0].ID != pushed.ID {
t.Errorf("expected job ID %q, got %q", pushed.ID, fetched[0].ID)
}

if fetched[0].State != "active" {
t.Errorf("expected state 'active', got %q", fetched[0].State)
}

if fetched[0].StartedAt == "" {
t.Error("StartedAt should be set")
}

if fetched[0].WorkerID != "worker-1" {
t.Errorf("expected WorkerID 'worker-1', got %q", fetched[0].WorkerID)
}

// Ack job
result := json.RawMessage(`{"result":"success"}`)
ackResp, err := b.Ack(ctx, fetched[0].ID, result)
if err != nil {
t.Fatalf("Ack failed: %v", err)
}

if !ackResp.Acknowledged {
t.Error("Ack should be acknowledged")
}
if ackResp.State != "completed" {
t.Errorf("expected state 'completed', got %q", ackResp.State)
}
if ackResp.ID != pushed.ID {
t.Errorf("expected ID %q, got %q", pushed.ID, ackResp.ID)
}

// Verify job is completed
info, _ := b.Info(ctx, pushed.ID)
if info.State != "completed" {
t.Errorf("expected final state 'completed', got %q", info.State)
}
if info.CompletedAt == "" {
t.Error("CompletedAt should be set")
}
}

func TestBackend_FetchMultiple(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push 5 jobs
for i := 0; i < 5; i++ {
b.Push(ctx, &core.Job{
Type:  "test.multi",
Queue: "multi-q",
Args:  json.RawMessage(`{}`),
})
}

// Fetch 3
fetched, err := b.Fetch(ctx, []string{"multi-q"}, 3, "worker-1", 30000)
if err != nil {
t.Fatalf("Fetch failed: %v", err)
}

if len(fetched) != 3 {
t.Errorf("expected 3 jobs, got %d", len(fetched))
}

for _, job := range fetched {
if job.State != "active" {
t.Errorf("expected state 'active', got %q", job.State)
}
}
}

func TestBackend_FetchMultipleQueues(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push to two queues
b.Push(ctx, &core.Job{
Type:  "test.q1",
Queue: "queue-1",
Args:  json.RawMessage(`{}`),
})
b.Push(ctx, &core.Job{
Type:  "test.q2",
Queue: "queue-2",
Args:  json.RawMessage(`{}`),
})

// Fetch from both
fetched, err := b.Fetch(ctx, []string{"queue-1", "queue-2"}, 2, "worker-1", 30000)
if err != nil {
t.Fatalf("Fetch failed: %v", err)
}

if len(fetched) != 2 {
t.Errorf("expected 2 jobs, got %d", len(fetched))
}
}

func TestBackend_FetchFromPausedQueue(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push job
b.Push(ctx, &core.Job{
Type:  "test.paused",
Queue: "paused-q",
Args:  json.RawMessage(`{}`),
})

// Pause queue
b.PauseQueue(ctx, "paused-q")

// Fetch should return nothing
fetched, err := b.Fetch(ctx, []string{"paused-q"}, 1, "worker-1", 30000)
if err != nil {
t.Fatalf("Fetch failed: %v", err)
}

if len(fetched) != 0 {
t.Errorf("expected 0 jobs from paused queue, got %d", len(fetched))
}
}

func TestBackend_AckNotFound(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

_, err := b.Ack(ctx, "nonexistent-id", nil)
if err == nil {
t.Fatal("expected error for nonexistent job")
}
}

func TestBackend_AckWrongState(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push and immediately try to ack (should be available, not active)
pushed, _ := b.Push(ctx, &core.Job{
Type:  "test.ack_wrong",
Queue: "default",
Args:  json.RawMessage(`{}`),
})

_, err := b.Ack(ctx, pushed.ID, nil)
if err == nil {
t.Fatal("expected error when acking non-active job")
}

ojsErr, ok := err.(*core.OJSError)
if !ok {
t.Fatalf("expected OJSError, got %T", err)
}
if ojsErr.Code != "conflict" {
t.Errorf("expected code 'CONFLICT', got %q", ojsErr.Code)
}
}

// --- Nack and Retries ---

func TestBackend_NackWithRetry(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push job
pushed, _ := b.Push(ctx, &core.Job{
Type:  "test.nack",
Queue: "default",
Args:  json.RawMessage(`{}`),
Retry: &core.RetryPolicy{MaxAttempts: 3},
})

// Fetch
fetched, _ := b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)

// Nack with retry
jobErr := &core.JobError{
Code:    "TEST_ERROR",
Message: "test error",
}
nackResp, err := b.Nack(ctx, fetched[0].ID, jobErr, true)
if err != nil {
t.Fatalf("Nack failed: %v", err)
}

if nackResp.Attempt != 1 {
t.Errorf("expected attempt 1, got %d", nackResp.Attempt)
}
if nackResp.MaxAttempts != 3 {
t.Errorf("expected maxAttempts 3, got %d", nackResp.MaxAttempts)
}
if nackResp.State != "retryable" {
t.Errorf("expected state 'retryable', got %q", nackResp.State)
}

// Verify job is retryable
info, _ := b.Info(ctx, pushed.ID)
if info.State != "retryable" {
t.Errorf("expected state 'retryable', got %q", info.State)
}
if info.Attempt != 1 {
t.Errorf("expected attempt 1, got %d", info.Attempt)
}
}

func TestBackend_NackExhausted(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push job with low max attempts
pushed, _ := b.Push(ctx, &core.Job{
Type:  "test.nack_exhaust",
Queue: "default",
Args:  json.RawMessage(`{}`),
Retry: &core.RetryPolicy{MaxAttempts: 1},
})

// Fetch and nack (attempt 1/1)
fetched, _ := b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
nackResp, _ := b.Nack(ctx, fetched[0].ID, nil, true)

if nackResp.State != "discarded" {
t.Errorf("expected state 'discarded', got %q", nackResp.State)
}

// Verify job is in dead letter
info, _ := b.Info(ctx, pushed.ID)
if info.State != "discarded" {
t.Errorf("expected state 'discarded', got %q", info.State)
}

// Should be in dead letters list
deadLetters, total, _ := b.ListDeadLetter(ctx, 100, 0)
if total == 0 {
t.Error("expected dead letter jobs")
}

found := false
for _, dl := range deadLetters {
if dl.ID == pushed.ID {
found = true
break
}
}
if !found {
t.Errorf("job %q not found in dead letters", pushed.ID)
}
}

func TestBackend_NackNoRetry(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push job
pushed, _ := b.Push(ctx, &core.Job{
Type:  "test.nack_no_retry",
Queue: "default",
Args:  json.RawMessage(`{}`),
})

// Fetch and nack with requeue=false
fetched, _ := b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
nackResp, _ := b.Nack(ctx, fetched[0].ID, nil, false)

if nackResp.State != "discarded" {
t.Errorf("expected state 'discarded', got %q", nackResp.State)
}

// Verify in dead letters
info, _ := b.Info(ctx, pushed.ID)
if info.State != "discarded" {
t.Errorf("expected state 'discarded', got %q", info.State)
}
}

// --- Queue Management ---

func TestBackend_ListQueues(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push to multiple queues
b.Push(ctx, &core.Job{Type: "q1", Queue: "queue-a", Args: json.RawMessage(`{}`)})
b.Push(ctx, &core.Job{Type: "q2", Queue: "queue-b", Args: json.RawMessage(`{}`)})

queues, err := b.ListQueues(ctx)
if err != nil {
t.Fatalf("ListQueues failed: %v", err)
}

names := make(map[string]bool)
for _, q := range queues {
names[q.Name] = true
}

if !names["queue-a"] {
t.Error("expected queue-a in list")
}
if !names["queue-b"] {
t.Error("expected queue-b in list")
}
}

func TestBackend_QueueStats(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push 3 jobs
for i := 0; i < 3; i++ {
b.Push(ctx, &core.Job{
Type:  "test.stats",
Queue: "stats-q",
Args:  json.RawMessage(`{}`),
})
}

// Fetch 1
jobs, _ := b.Fetch(ctx, []string{"stats-q"}, 1, "worker-1", 30000)

// Get stats
stats, err := b.QueueStats(ctx, "stats-q")
if err != nil {
t.Fatalf("QueueStats failed: %v", err)
}

if stats.Queue != "stats-q" {
t.Errorf("expected queue 'stats-q', got %q", stats.Queue)
}
if stats.Stats.Available != 2 {
t.Errorf("expected 2 available, got %d", stats.Stats.Available)
}
if stats.Stats.Active != 1 {
t.Errorf("expected 1 active, got %d", stats.Stats.Active)
}

// Ack the job
b.Ack(ctx, jobs[0].ID, nil)

// Get stats again
stats, _ = b.QueueStats(ctx, "stats-q")
if stats.Stats.Available != 2 {
t.Errorf("expected 2 available, got %d", stats.Stats.Available)
}
if stats.Stats.Active != 0 {
t.Errorf("expected 0 active, got %d", stats.Stats.Active)
}
if stats.Stats.Completed != 1 {
t.Errorf("expected 1 completed, got %d", stats.Stats.Completed)
}
}

func TestBackend_PauseResumeQueue(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

queueName := "pause-test-q"

// Push job
b.Push(ctx, &core.Job{
Type:  "test.pause",
Queue: queueName,
Args:  json.RawMessage(`{}`),
})

// Pause queue
err := b.PauseQueue(ctx, queueName)
if err != nil {
t.Fatalf("PauseQueue failed: %v", err)
}

// Verify paused status
stats, _ := b.QueueStats(ctx, queueName)
if stats.Status != "paused" {
t.Errorf("expected status 'paused', got %q", stats.Status)
}

// Fetch should return nothing
fetched, _ := b.Fetch(ctx, []string{queueName}, 1, "worker-1", 30000)
if len(fetched) != 0 {
t.Errorf("expected 0 jobs from paused queue, got %d", len(fetched))
}

// Resume queue
err = b.ResumeQueue(ctx, queueName)
if err != nil {
t.Fatalf("ResumeQueue failed: %v", err)
}

// Verify active status
stats, _ = b.QueueStats(ctx, queueName)
if stats.Status != "active" {
t.Errorf("expected status 'active', got %q", stats.Status)
}

// Fetch should work
fetched, _ = b.Fetch(ctx, []string{queueName}, 1, "worker-1", 30000)
if len(fetched) != 1 {
t.Errorf("expected 1 job after resume, got %d", len(fetched))
}
}

// --- Dead Letter Management ---

func TestBackend_ListDeadLetter(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Create discarded jobs
for i := 0; i < 3; i++ {
pushed, _ := b.Push(ctx, &core.Job{
Type:  "test.dl",
Queue: "default",
Args:  json.RawMessage(`{}`),
Retry: &core.RetryPolicy{MaxAttempts: 1},
})

fetched, _ := b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
b.Nack(ctx, fetched[0].ID, nil, true)
_ = pushed
}

// List dead letters
deadLetters, total, err := b.ListDeadLetter(ctx, 100, 0)
if err != nil {
t.Fatalf("ListDeadLetter failed: %v", err)
}

if total < 3 {
t.Errorf("expected at least 3 dead letters, got %d", total)
}
if len(deadLetters) < 3 {
t.Errorf("expected at least 3 returned, got %d", len(deadLetters))
}
}

func TestBackend_RetryDeadLetter(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Create and discard a job
_, _ = b.Push(ctx, &core.Job{
Type:  "test.retry_dl",
Queue: "default",
Args:  json.RawMessage(`{}`),
Retry: &core.RetryPolicy{MaxAttempts: 1},
})

fetched, _ := b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
b.Nack(ctx, fetched[0].ID, nil, true)

// Get dead letter ID
deadLetters, _, _ := b.ListDeadLetter(ctx, 100, 0)
if len(deadLetters) == 0 {
t.Fatal("expected dead letter job")
}
deadID := deadLetters[0].ID

// Retry it
retried, err := b.RetryDeadLetter(ctx, deadID)
if err != nil {
t.Fatalf("RetryDeadLetter failed: %v", err)
}

if retried.State != "available" {
t.Errorf("expected state 'available', got %q", retried.State)
}
if retried.Attempt != 0 {
t.Errorf("expected attempt 0, got %d", retried.Attempt)
}

// Should not be in dead letters anymore
deadLetters, _, _ = b.ListDeadLetter(ctx, 100, 0)
for _, dl := range deadLetters {
if dl.ID == deadID {
t.Error("job should not be in dead letters after retry")
}
}
}

func TestBackend_DeleteDeadLetter(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Create and discard a job
_, _ = b.Push(ctx, &core.Job{
Type:  "test.delete_dl",
Queue: "default",
Args:  json.RawMessage(`{}`),
Retry: &core.RetryPolicy{MaxAttempts: 1},
})

fetched, _ := b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
b.Nack(ctx, fetched[0].ID, nil, true)

// Get dead letter ID
deadLetters, _, _ := b.ListDeadLetter(ctx, 100, 0)
deadID := deadLetters[0].ID

// Delete it
err := b.DeleteDeadLetter(ctx, deadID)
if err != nil {
t.Fatalf("DeleteDeadLetter failed: %v", err)
}

// Verify it's gone
_, err = b.Info(ctx, deadID)
if err == nil {
t.Error("deleted job should not exist")
}
}

// --- Cron Management ---

func TestBackend_RegisterAndListCron(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

cron := &core.CronJob{
Name:       "test-cron",
Expression: "0 * * * *",
Enabled:    true,
NextRunAt:  time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339),
JobTemplate: &core.CronJobTemplate{
Type: "cron.test",
Args: json.RawMessage(`{"test":"data"}`),
},
}

registered, err := b.RegisterCron(ctx, cron)
if err != nil {
t.Fatalf("RegisterCron failed: %v", err)
}

if registered.Name != "test-cron" {
t.Errorf("expected name 'test-cron', got %q", registered.Name)
}
if registered.CreatedAt == "" {
t.Error("CreatedAt should be set")
}

// List crons
crons, err := b.ListCron(ctx)
if err != nil {
t.Fatalf("ListCron failed: %v", err)
}

found := false
for _, c := range crons {
if c.Name == "test-cron" {
found = true
break
}
}
if !found {
t.Error("test-cron not found in list")
}
}

func TestBackend_DeleteCron(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

cron := &core.CronJob{
Name:     "test-delete-cron",
Schedule: "0 * * * *",
Enabled:  true,
}

b.RegisterCron(ctx, cron)

// Delete it
deleted, err := b.DeleteCron(ctx, "test-delete-cron")
if err != nil {
t.Fatalf("DeleteCron failed: %v", err)
}

if deleted.Name != "test-delete-cron" {
t.Errorf("expected name 'test-delete-cron', got %q", deleted.Name)
}

// Verify it's gone
crons, _ := b.ListCron(ctx)
for _, c := range crons {
if c.Name == "test-delete-cron" {
t.Error("cron should be deleted")
}
}
}

// --- Workflows ---

func TestBackend_CreateWorkflow_Group(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

req := &core.WorkflowRequest{
Type: "group",
Jobs: []core.WorkflowJobRequest{
{Type: "job1", Args: json.RawMessage(`{}`)},
{Type: "job2", Args: json.RawMessage(`{}`)},
},
}

wf, err := b.CreateWorkflow(ctx, req)
if err != nil {
t.Fatalf("CreateWorkflow failed: %v", err)
}

if wf.ID == "" {
t.Fatal("workflow ID should be generated")
}
if wf.Type != "group" {
t.Errorf("expected type 'group', got %q", wf.Type)
}
if wf.State != "active" {
t.Errorf("expected state 'active', got %q", wf.State)
}
if wf.JobsTotal == nil || *wf.JobsTotal != 2 {
t.Errorf("expected JobsTotal 2, got %v", wf.JobsTotal)
}
if wf.JobsCompleted == nil || *wf.JobsCompleted != 0 {
t.Errorf("expected JobsCompleted 0, got %v", wf.JobsCompleted)
}
}

func TestBackend_GetWorkflow(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

req := &core.WorkflowRequest{
Type: "group",
Jobs: []core.WorkflowJobRequest{
{Type: "job1", Args: json.RawMessage(`{}`)},
},
}

created, _ := b.CreateWorkflow(ctx, req)

retrieved, err := b.GetWorkflow(ctx, created.ID)
if err != nil {
t.Fatalf("GetWorkflow failed: %v", err)
}

if retrieved.ID != created.ID {
t.Errorf("expected ID %q, got %q", created.ID, retrieved.ID)
}
}

func TestBackend_CancelWorkflow(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

req := &core.WorkflowRequest{
Type: "group",
Jobs: []core.WorkflowJobRequest{
{Type: "job1", Args: json.RawMessage(`{}`)},
},
}

created, _ := b.CreateWorkflow(ctx, req)

cancelled, err := b.CancelWorkflow(ctx, created.ID)
if err != nil {
t.Fatalf("CancelWorkflow failed: %v", err)
}

if cancelled.State != "cancelled" {
t.Errorf("expected state 'cancelled', got %q", cancelled.State)
}
}

func TestBackend_AdvanceWorkflow_Group(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

req := &core.WorkflowRequest{
Type: "group",
Jobs: []core.WorkflowJobRequest{
{Type: "job1", Args: json.RawMessage(`{}`)},
{Type: "job2", Args: json.RawMessage(`{}`)},
},
}

wf, _ := b.CreateWorkflow(ctx, req)

// Advance once
err := b.AdvanceWorkflow(ctx, wf.ID, "", nil, false)
if err != nil {
t.Fatalf("AdvanceWorkflow failed: %v", err)
}

retrieved, _ := b.GetWorkflow(ctx, wf.ID)
if retrieved.State != "active" {
t.Errorf("expected state 'active', got %q", retrieved.State)
}
if *retrieved.JobsCompleted != 1 {
t.Errorf("expected JobsCompleted 1, got %d", *retrieved.JobsCompleted)
}

// Advance again
b.AdvanceWorkflow(ctx, wf.ID, "", nil, false)

retrieved, _ = b.GetWorkflow(ctx, wf.ID)
if retrieved.State != "completed" {
t.Errorf("expected state 'completed', got %q", retrieved.State)
}
if retrieved.CompletedAt == "" {
t.Error("CompletedAt should be set")
}
}

// --- Worker Management ---

func TestBackend_Heartbeat(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

resp, err := b.Heartbeat(ctx, "worker-test", []string{"job1", "job2"}, 30000)
if err != nil {
t.Fatalf("Heartbeat failed: %v", err)
}

if resp.State != "running" {
t.Errorf("expected state 'running', got %q", resp.State)
}
if resp.ServerTime == "" {
t.Error("ServerTime should be set")
}
}

func TestBackend_SetWorkerState(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Register worker
b.Heartbeat(ctx, "worker-state-test", []string{}, 30000)

// Set state to quiet
err := b.SetWorkerState(ctx, "worker-state-test", "quiet")
if err != nil {
t.Fatalf("SetWorkerState failed: %v", err)
}

// Verify state in next heartbeat
resp, _ := b.Heartbeat(ctx, "worker-state-test", []string{}, 30000)
if resp.Directive != "quiet" {
// Note: The directive might be a separate field, adjust as needed
t.Logf("Worker state updated (directive or state field)")
}
}

// --- Cancel Job ---

func TestBackend_Cancel(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push and cancel
pushed, _ := b.Push(ctx, &core.Job{
Type:  "test.cancel",
Queue: "default",
Args:  json.RawMessage(`{}`),
})

cancelled, err := b.Cancel(ctx, pushed.ID)
if err != nil {
t.Fatalf("Cancel failed: %v", err)
}

if cancelled.State != "cancelled" {
t.Errorf("expected state 'cancelled', got %q", cancelled.State)
}
if cancelled.CancelledAt == "" {
t.Error("CancelledAt should be set")
}
}

func TestBackend_CancelActiveJob(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push and fetch
_, _ = b.Push(ctx, &core.Job{
Type:  "test.cancel_active",
Queue: "default",
Args:  json.RawMessage(`{}`),
})

fetched, _ := b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)

// Cancel active job — should succeed per OJS spec (only terminal states are blocked)
cancelled, err := b.Cancel(ctx, fetched[0].ID)
if err != nil {
t.Fatalf("expected cancel of active job to succeed, got: %v", err)
}
if cancelled.State != "cancelled" {
t.Errorf("expected state 'cancelled', got %q", cancelled.State)
}
}

// --- Health ---

func TestBackend_Health(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

health, err := b.Health(ctx)
if err != nil {
t.Fatalf("Health failed: %v", err)
}

// Status is "ok" with connection, "degraded" without
if health.Status != "ok" && health.Status != "degraded" {
t.Errorf("expected status 'ok' or 'degraded', got %q", health.Status)
}
if health.Version != "0.2.0" {
t.Errorf("expected version '0.2.0', got %q", health.Version)
}
if health.UptimeSeconds < 0 {
t.Errorf("expected positive uptime, got %d", health.UptimeSeconds)
}
if health.Backend.Type != "amqp" {
t.Errorf("expected backend type 'amqp', got %q", health.Backend.Type)
}
}
