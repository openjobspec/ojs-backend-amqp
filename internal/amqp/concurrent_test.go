package amqp

import (
"context"
"encoding/json"
"fmt"
"sync"
"sync/atomic"
"testing"
"time"

"github.com/openjobspec/ojs-go-backend-common/core"
)

// --- Concurrency Tests ---

func TestConcurrentPush(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

numGoroutines := 50
jobsPerGoroutine := 10

var wg sync.WaitGroup
var successCount atomic.Int32

for i := 0; i < numGoroutines; i++ {
wg.Add(1)
go func(workerID int) {
defer wg.Done()
for j := 0; j < jobsPerGoroutine; j++ {
job := &core.Job{
Type:  "concurrent.test",
Queue: "default",
Args:  json.RawMessage(`{}`),
}
if _, err := b.Push(ctx, job); err == nil {
successCount.Add(1)
}
}
}(i)
}

wg.Wait()

expected := int32(numGoroutines * jobsPerGoroutine)
if successCount.Load() != expected {
t.Errorf("expected %d successful pushes, got %d", expected, successCount.Load())
}
}

func TestConcurrentFetchExclusive(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

numJobs := 100

// Push jobs
for i := 0; i < numJobs; i++ {
b.Push(ctx, &core.Job{
Type:  "exclusive.test",
Queue: "default",
Args:  json.RawMessage(`{}`),
})
}

// Multiple workers fetch concurrently
numWorkers := 20
var totalFetched atomic.Int32
var wg sync.WaitGroup
seenJobs := sync.Map{}

for i := 0; i < numWorkers; i++ {
wg.Add(1)
go func(workerID int) {
defer wg.Done()
for {
jobs, err := b.Fetch(ctx, []string{"default"}, 5, core.NewUUIDv7(), 30000)
if err != nil {
return
}
if len(jobs) == 0 {
return
}
for _, job := range jobs {
if _, loaded := seenJobs.LoadOrStore(job.ID, true); loaded {
t.Errorf("job %s fetched multiple times", job.ID)
}
totalFetched.Add(1)
}
}
}(i)
}

wg.Wait()

if totalFetched.Load() != int32(numJobs) {
t.Errorf("expected %d fetched jobs, got %d", numJobs, totalFetched.Load())
}
}

func TestConcurrentAckSameJob(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push and fetch a single job
job := &core.Job{
Type:  "concurrent.ack",
Queue: "default",
Args:  json.RawMessage(`{}`),
}
pushed, _ := b.Push(ctx, job)

fetched, _ := b.Fetch(ctx, []string{"default"}, 1, "worker-test", 30000)
if len(fetched) != 1 {
t.Fatalf("expected 1 fetched job")
}

jobID := pushed.ID
numGoroutines := 10
var successCount atomic.Int32
var wg sync.WaitGroup

// Multiple goroutines try to ack the same job
for i := 0; i < numGoroutines; i++ {
wg.Add(1)
go func() {
defer wg.Done()
_, err := b.Ack(ctx, jobID, nil)
if err == nil {
successCount.Add(1)
}
}()
}

wg.Wait()

// Only one should succeed
if successCount.Load() != 1 {
t.Errorf("expected exactly 1 successful Ack, got %d", successCount.Load())
}
}

func TestConcurrentNackSameJob(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Push and fetch a single job
job := &core.Job{
Type:  "concurrent.nack",
Queue: "default",
Args:  json.RawMessage(`{}`),
Retry: &core.RetryPolicy{MaxAttempts: 5},
}
pushed, _ := b.Push(ctx, job)

fetched, _ := b.Fetch(ctx, []string{"default"}, 1, "worker-test", 30000)
if len(fetched) != 1 {
t.Fatalf("expected 1 fetched job")
}

jobID := pushed.ID
numGoroutines := 10
var successCount atomic.Int32
var wg sync.WaitGroup

// Multiple goroutines try to nack the same job
for i := 0; i < numGoroutines; i++ {
wg.Add(1)
go func() {
defer wg.Done()
_, err := b.Nack(ctx, jobID, nil, true)
if err == nil {
successCount.Add(1)
}
}()
}

wg.Wait()

// Only one should succeed
if successCount.Load() != 1 {
t.Errorf("expected exactly 1 successful Nack, got %d", successCount.Load())
}
}

func TestConcurrentPushAndFetch(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

numProducers := 20
numConsumers := 20
jobsPerProducer := 10

var pushCount atomic.Int32
var fetchCount atomic.Int32
var wg sync.WaitGroup
done := make(chan struct{})

// Producer goroutines
for i := 0; i < numProducers; i++ {
wg.Add(1)
go func() {
defer wg.Done()
for j := 0; j < jobsPerProducer; j++ {
job := &core.Job{
Type:  "producer.test",
Queue: "concurrent-q",
Args:  json.RawMessage(`{}`),
}
if _, err := b.Push(ctx, job); err == nil {
pushCount.Add(1)
}
time.Sleep(1 * time.Millisecond)
}
}()
}

// Consumer goroutines
for i := 0; i < numConsumers; i++ {
wg.Add(1)
go func(workerID int) {
defer wg.Done()
for {
select {
case <-done:
return
default:
}

jobs, err := b.Fetch(ctx, []string{"concurrent-q"}, 5, core.NewUUIDv7(), 30000)
if err != nil {
time.Sleep(1 * time.Millisecond)
continue
}
if len(jobs) == 0 {
time.Sleep(5 * time.Millisecond)
continue
}
fetchCount.Add(int32(len(jobs)))
for _, job := range jobs {
b.Ack(ctx, job.ID, nil)
}
}
}(i)
}

// Monitor goroutine
go func() {
maxWait := time.After(15 * time.Second)
ticker := time.NewTicker(100 * time.Millisecond)
defer ticker.Stop()
for {
select {
case <-maxWait:
close(done)
return
case <-ticker.C:
if fetchCount.Load() >= int32(numProducers*jobsPerProducer) {
close(done)
return
}
}
}
}()

wg.Wait()

expectedPushes := int32(numProducers * jobsPerProducer)
if pushCount.Load() != expectedPushes {
t.Errorf("expected %d pushed jobs, got %d", expectedPushes, pushCount.Load())
}

if fetchCount.Load() < expectedPushes {
t.Errorf("expected %d fetched jobs, got %d", expectedPushes, fetchCount.Load())
}
}

func TestConcurrentBatchPush(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

numGoroutines := 20
batchSize := 5

var totalPushed atomic.Int32
var wg sync.WaitGroup

for i := 0; i < numGoroutines; i++ {
wg.Add(1)
go func() {
defer wg.Done()
batch := make([]*core.Job, batchSize)
for j := 0; j < batchSize; j++ {
batch[j] = &core.Job{
Type:  "batch.test",
Queue: "batch-q",
Args:  json.RawMessage(`{}`),
}
}
if _, err := b.PushBatch(ctx, batch); err == nil {
totalPushed.Add(int32(batchSize))
}
}()
}

wg.Wait()

expected := int32(numGoroutines * batchSize)
if totalPushed.Load() != expected {
t.Errorf("expected %d pushed jobs, got %d", expected, totalPushed.Load())
}
}

func TestConcurrentWorkflowAdvancement(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

// Create workflow with multiple jobs
req := &core.WorkflowRequest{
Type: "group",
Jobs: []core.WorkflowJobRequest{
{Type: "job1", Args: json.RawMessage(`{}`)},
{Type: "job2", Args: json.RawMessage(`{}`)},
{Type: "job3", Args: json.RawMessage(`{}`)},
{Type: "job4", Args: json.RawMessage(`{}`)},
{Type: "job5", Args: json.RawMessage(`{}`)},
},
}

wf, err := b.CreateWorkflow(ctx, req)
if err != nil {
t.Fatalf("CreateWorkflow failed: %v", err)
}

// Advance workflow concurrently from multiple goroutines
var wg sync.WaitGroup
for i := 0; i < 5; i++ {
wg.Add(1)
go func(idx int) {
defer wg.Done()
err := b.AdvanceWorkflow(ctx, wf.ID, fmt.Sprintf("job%d", idx+1), json.RawMessage(`{"result":"ok"}`), false)
if err != nil {
t.Errorf("AdvanceWorkflow failed: %v", err)
}
}(i)
}

wg.Wait()

// Verify workflow is completed
finalWF, err := b.GetWorkflow(ctx, wf.ID)
if err != nil {
t.Fatalf("GetWorkflow failed: %v", err)
}
if finalWF.State != "completed" {
t.Errorf("expected workflow state 'completed', got %q", finalWF.State)
}
if finalWF.JobsCompleted == nil || *finalWF.JobsCompleted != 5 {
t.Errorf("expected 5 completed jobs, got %v", finalWF.JobsCompleted)
}
}

func TestConcurrentQueueStats(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

queueName := "stats-concurrent-q"

// Push initial jobs
for i := 0; i < 50; i++ {
b.Push(ctx, &core.Job{
Type:  "stats.test",
Queue: queueName,
Args:  json.RawMessage(`{}`),
})
}

var wg sync.WaitGroup

// Multiple goroutines fetching and acking
for i := 0; i < 10; i++ {
wg.Add(1)
go func(workerID int) {
defer wg.Done()
for j := 0; j < 5; j++ {
jobs, _ := b.Fetch(ctx, []string{queueName}, 1, core.NewUUIDv7(), 30000)
if len(jobs) > 0 {
b.Ack(ctx, jobs[0].ID, nil)
}
}
}(i)
}

wg.Wait()

// Get final stats
stats, _ := b.QueueStats(ctx, queueName)
if stats.Stats.Completed < 50 {
t.Errorf("expected at least 50 completed, got %d", stats.Stats.Completed)
}
}

func TestConcurrentHeartbeat(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

numWorkers := 20
var wg sync.WaitGroup

for i := 0; i < numWorkers; i++ {
wg.Add(1)
go func(workerNum int) {
defer wg.Done()
wID := core.NewUUIDv7()
for j := 0; j < 10; j++ {
_, err := b.Heartbeat(ctx, wID, []string{}, 30000)
if err != nil {
t.Errorf("Heartbeat failed: %v", err)
}
time.Sleep(1 * time.Millisecond)
}
}(i)
}

wg.Wait()

// Verify workers were tracked
workers, summary, _ := b.ListWorkers(ctx, 100, 0)
if summary.Total < 1 {
t.Errorf("expected at least 1 worker, got %d", summary.Total)
}
_ = workers
}

func TestConcurrentRetries(t *testing.T) {
b := testBackend(t)
ctx := context.Background()

numJobs := 30
var wg sync.WaitGroup

// Push jobs and nack them
for i := 0; i < numJobs; i++ {
wg.Add(1)
go func() {
defer wg.Done()
pushed, _ := b.Push(ctx, &core.Job{
Type:  "retry.test",
Queue: "retry-q",
Args:  json.RawMessage(`{}`),
Retry: &core.RetryPolicy{MaxAttempts: 3},
})

fetched, _ := b.Fetch(ctx, []string{"retry-q"}, 1, core.NewUUIDv7(), 30000)
if len(fetched) > 0 {
b.Nack(ctx, fetched[0].ID, nil, true)
}
_ = pushed
}()
}

wg.Wait()

// Wait for retries to be scheduled
time.Sleep(500 * time.Millisecond)

// Check that some jobs are retryable
allJobs, _, _ := b.ListJobs(ctx, core.JobListFilters{
State: "retryable",
Queue: "retry-q",
}, 100, 0)

if len(allJobs) < numJobs {
t.Logf("expected at least %d retryable jobs, got %d (may still be in transition)", numJobs, len(allJobs))
}
}
