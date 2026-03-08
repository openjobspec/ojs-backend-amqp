package amqp

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

func newTestJob(id, jobType, queue, state string) *core.Job {
	return &core.Job{
		ID:         id,
		Type:       jobType,
		Queue:      queue,
		State:      state,
		Args:       json.RawMessage(`{"key":"value"}`),
		CreatedAt:  "2024-01-01T00:00:00Z",
		EnqueuedAt: "2024-01-01T00:00:00Z",
		Attempt:    1,
	}
}

func TestSQLiteStore_SaveAndLoadJob(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	job := newTestJob("job-1", "email.send", "default", "available")
	job.Tags = []string{"priority:high"}

	if err := store.SaveJob(job); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	state, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	loaded, ok := state.Jobs["job-1"]
	if !ok {
		t.Fatal("expected job-1 to be loaded")
	}
	if loaded.ID != "job-1" {
		t.Errorf("expected ID 'job-1', got %q", loaded.ID)
	}
	if loaded.Type != "email.send" {
		t.Errorf("expected Type 'email.send', got %q", loaded.Type)
	}
	if loaded.Queue != "default" {
		t.Errorf("expected Queue 'default', got %q", loaded.Queue)
	}
	if loaded.State != "available" {
		t.Errorf("expected State 'available', got %q", loaded.State)
	}
	if loaded.Attempt != 1 {
		t.Errorf("expected Attempt 1, got %d", loaded.Attempt)
	}
	if len(loaded.Tags) != 1 || loaded.Tags[0] != "priority:high" {
		t.Errorf("expected tags [priority:high], got %v", loaded.Tags)
	}
}

func TestSQLiteStore_SaveAndLoadDeadJob(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	job := newTestJob("dead-1", "failing.job", "default", "discarded")
	job.CompletedAt = "2024-01-01T01:00:00Z"
	errJSON := json.RawMessage(`{"code":"TIMEOUT","message":"timed out"}`)
	job.Error = errJSON

	if err := store.SaveDeadJob(job); err != nil {
		t.Fatalf("SaveDeadJob failed: %v", err)
	}

	state, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	if len(state.DeadJobs) != 1 {
		t.Fatalf("expected 1 dead job, got %d", len(state.DeadJobs))
	}
	loaded := state.DeadJobs[0]
	if loaded.ID != "dead-1" {
		t.Errorf("expected ID 'dead-1', got %q", loaded.ID)
	}
	if loaded.State != "discarded" {
		t.Errorf("expected State 'discarded', got %q", loaded.State)
	}
	if loaded.Error == nil {
		t.Error("expected Error to be set")
	}
}

func TestSQLiteStore_SaveAndLoadCron(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	cron := &core.CronJob{
		Name:       "hourly-report",
		Expression: "0 * * * *",
		Enabled:    true,
		NextRunAt:  "2024-01-01T02:00:00Z",
		JobTemplate: &core.CronJobTemplate{
			Type: "report.generate",
			Args: json.RawMessage(`{"format":"pdf"}`),
		},
	}

	if err := store.SaveCron(cron); err != nil {
		t.Fatalf("SaveCron failed: %v", err)
	}

	state, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	loaded, ok := state.Crons["hourly-report"]
	if !ok {
		t.Fatal("expected hourly-report cron to be loaded")
	}
	if loaded.Expression != "0 * * * *" {
		t.Errorf("expected Expression '0 * * * *', got %q", loaded.Expression)
	}
	if !loaded.Enabled {
		t.Error("expected Enabled to be true")
	}
	if loaded.NextRunAt != "2024-01-01T02:00:00Z" {
		t.Errorf("expected NextRunAt '2024-01-01T02:00:00Z', got %q", loaded.NextRunAt)
	}
	if loaded.JobTemplate == nil || loaded.JobTemplate.Type != "report.generate" {
		t.Errorf("expected JobTemplate.Type 'report.generate', got %v", loaded.JobTemplate)
	}
}

func TestSQLiteStore_SaveAndLoadWorkflow(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	total := 3
	completed := 1
	wf := &core.Workflow{
		ID:            "wf-1",
		Name:          "test-workflow",
		Type:          "chain",
		State:         "active",
		StepsTotal:    &total,
		StepsCompleted: &completed,
		CreatedAt:     "2024-01-01T00:00:00Z",
	}

	if err := store.SaveWorkflow(wf); err != nil {
		t.Fatalf("SaveWorkflow failed: %v", err)
	}

	state, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	loaded, ok := state.Workflows["wf-1"]
	if !ok {
		t.Fatal("expected wf-1 workflow to be loaded")
	}
	if loaded.Name != "test-workflow" {
		t.Errorf("expected Name 'test-workflow', got %q", loaded.Name)
	}
	if loaded.Type != "chain" {
		t.Errorf("expected Type 'chain', got %q", loaded.Type)
	}
	if loaded.State != "active" {
		t.Errorf("expected State 'active', got %q", loaded.State)
	}
	if loaded.StepsTotal == nil || *loaded.StepsTotal != 3 {
		t.Errorf("expected StepsTotal 3, got %v", loaded.StepsTotal)
	}
	if loaded.StepsCompleted == nil || *loaded.StepsCompleted != 1 {
		t.Errorf("expected StepsCompleted 1, got %v", loaded.StepsCompleted)
	}
}

func TestSQLiteStore_SaveQueueState(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	if err := store.SaveQueueState("queue-a", "paused"); err != nil {
		t.Fatalf("SaveQueueState paused failed: %v", err)
	}
	if err := store.SaveQueueState("queue-b", "active"); err != nil {
		t.Fatalf("SaveQueueState active failed: %v", err)
	}

	state, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	if !state.QueueState["queue-a"] {
		t.Error("expected queue-a to be paused (true)")
	}
	if state.QueueState["queue-b"] {
		t.Error("expected queue-b to be active (false)")
	}
}

func TestSQLiteStore_DeleteJob(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	job := newTestJob("del-1", "test.type", "default", "available")
	if err := store.SaveJob(job); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	// Verify it exists
	state, _ := store.LoadAll()
	if _, ok := state.Jobs["del-1"]; !ok {
		t.Fatal("expected job to exist before delete")
	}

	if err := store.DeleteJob("del-1"); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}

	state, _ = store.LoadAll()
	if _, ok := state.Jobs["del-1"]; ok {
		t.Error("expected job to be deleted")
	}
	// Job order should also be cleaned up
	for _, id := range state.JobOrder {
		if id == "del-1" {
			t.Error("expected job to be removed from job order")
		}
	}
}

func TestSQLiteStore_LoadAll(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	// Save multiple entities
	for i := 0; i < 5; i++ {
		job := newTestJob("job-"+string(rune('a'+i)), "test.type", "default", "available")
		store.SaveJob(job)
	}
	store.SaveDeadJob(newTestJob("dead-a", "test.type", "default", "discarded"))
	store.SaveDeadJob(newTestJob("dead-b", "test.type", "default", "discarded"))
	store.SaveCron(&core.CronJob{Name: "cron-a", Expression: "* * * * *", Enabled: true})
	total := 2
	zero := 0
	store.SaveWorkflow(&core.Workflow{ID: "wf-a", Type: "group", State: "active", JobsTotal: &total, JobsCompleted: &zero, CreatedAt: "2024-01-01T00:00:00Z"})
	store.SaveQueueState("q1", "paused")

	state, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	if len(state.Jobs) != 5 {
		t.Errorf("expected 5 jobs, got %d", len(state.Jobs))
	}
	if len(state.DeadJobs) != 2 {
		t.Errorf("expected 2 dead jobs, got %d", len(state.DeadJobs))
	}
	if len(state.Crons) != 1 {
		t.Errorf("expected 1 cron, got %d", len(state.Crons))
	}
	if len(state.Workflows) != 1 {
		t.Errorf("expected 1 workflow, got %d", len(state.Workflows))
	}
	if len(state.QueueState) != 1 {
		t.Errorf("expected 1 queue state, got %d", len(state.QueueState))
	}
}

func TestSQLiteStore_JobOrder(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	ids := []string{"first", "second", "third", "fourth"}
	for _, id := range ids {
		job := newTestJob(id, "test.type", "default", "available")
		if err := store.SaveJob(job); err != nil {
			t.Fatalf("SaveJob(%s) failed: %v", id, err)
		}
	}

	state, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	if len(state.JobOrder) != 4 {
		t.Fatalf("expected 4 jobs in order, got %d", len(state.JobOrder))
	}
	for i, expected := range ids {
		if state.JobOrder[i] != expected {
			t.Errorf("order[%d]: expected %q, got %q", i, expected, state.JobOrder[i])
		}
	}
}

func TestSQLiteStore_Persistence_AcrossRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "restart.db")

	// First instance: save data
	store1, err := newSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("newSQLiteStore (1st) failed: %v", err)
	}

	job := newTestJob("persist-1", "email.send", "important", "available")
	store1.SaveJob(job)
	store1.SaveCron(&core.CronJob{Name: "daily", Expression: "0 0 * * *", Enabled: true})
	total := 2
	zero := 0
	store1.SaveWorkflow(&core.Workflow{ID: "wf-persist", Type: "group", State: "active", JobsTotal: &total, JobsCompleted: &zero, CreatedAt: "2024-01-01T00:00:00Z"})
	store1.SaveQueueState("important", "paused")
	store1.SaveDeadJob(newTestJob("dead-persist", "fail.job", "default", "discarded"))

	store1.Close()

	// Second instance: load data
	store2, err := newSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("newSQLiteStore (2nd) failed: %v", err)
	}
	defer store2.Close()

	state, err := store2.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll (2nd) failed: %v", err)
	}

	if _, ok := state.Jobs["persist-1"]; !ok {
		t.Error("expected job 'persist-1' to survive restart")
	}
	if _, ok := state.Crons["daily"]; !ok {
		t.Error("expected cron 'daily' to survive restart")
	}
	if _, ok := state.Workflows["wf-persist"]; !ok {
		t.Error("expected workflow 'wf-persist' to survive restart")
	}
	if !state.QueueState["important"] {
		t.Error("expected queue 'important' to be paused after restart")
	}
	if len(state.DeadJobs) != 1 || state.DeadJobs[0].ID != "dead-persist" {
		t.Error("expected dead job 'dead-persist' to survive restart")
	}
}

func TestSQLiteStore_EmptyLoad(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "empty.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	state, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	if len(state.Jobs) != 0 {
		t.Errorf("expected 0 jobs, got %d", len(state.Jobs))
	}
	if len(state.JobOrder) != 0 {
		t.Errorf("expected 0 job order, got %d", len(state.JobOrder))
	}
	if len(state.DeadJobs) != 0 {
		t.Errorf("expected 0 dead jobs, got %d", len(state.DeadJobs))
	}
	if len(state.Crons) != 0 {
		t.Errorf("expected 0 crons, got %d", len(state.Crons))
	}
	if len(state.Workflows) != 0 {
		t.Errorf("expected 0 workflows, got %d", len(state.Workflows))
	}
	if len(state.QueueState) != 0 {
		t.Errorf("expected 0 queue states, got %d", len(state.QueueState))
	}
}

func TestSQLiteStore_SaveJob_Upsert(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	job := newTestJob("upsert-1", "test.type", "default", "available")
	store.SaveJob(job)

	// Update the job
	job.State = "active"
	job.Attempt = 2
	store.SaveJob(job)

	state, _ := store.LoadAll()
	loaded := state.Jobs["upsert-1"]
	if loaded.State != "active" {
		t.Errorf("expected updated State 'active', got %q", loaded.State)
	}
	if loaded.Attempt != 2 {
		t.Errorf("expected updated Attempt 2, got %d", loaded.Attempt)
	}
}

func TestSQLiteStore_DeleteDeadJob(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	store.SaveDeadJob(newTestJob("dead-del-1", "test.type", "default", "discarded"))
	store.SaveDeadJob(newTestJob("dead-del-2", "test.type", "default", "discarded"))

	if err := store.DeleteDeadJob("dead-del-1"); err != nil {
		t.Fatalf("DeleteDeadJob failed: %v", err)
	}

	state, _ := store.LoadAll()
	if len(state.DeadJobs) != 1 {
		t.Errorf("expected 1 dead job remaining, got %d", len(state.DeadJobs))
	}
	if state.DeadJobs[0].ID != "dead-del-2" {
		t.Errorf("expected remaining dead job 'dead-del-2', got %q", state.DeadJobs[0].ID)
	}
}

func TestSQLiteStore_DeleteCron(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	store.SaveCron(&core.CronJob{Name: "cron-del", Expression: "* * * * *", Enabled: true})

	if err := store.DeleteCron("cron-del"); err != nil {
		t.Fatalf("DeleteCron failed: %v", err)
	}

	state, _ := store.LoadAll()
	if _, ok := state.Crons["cron-del"]; ok {
		t.Error("expected cron to be deleted")
	}
}

func TestSQLiteStore_DeleteWorkflow(t *testing.T) {
	dir := t.TempDir()
	store, err := newSQLiteStore(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore failed: %v", err)
	}
	defer store.Close()

	total := 1
	zero := 0
	store.SaveWorkflow(&core.Workflow{ID: "wf-del", Type: "group", State: "active", JobsTotal: &total, JobsCompleted: &zero, CreatedAt: "2024-01-01T00:00:00Z"})

	if err := store.DeleteWorkflow("wf-del"); err != nil {
		t.Fatalf("DeleteWorkflow failed: %v", err)
	}

	state, _ := store.LoadAll()
	if _, ok := state.Workflows["wf-del"]; ok {
		t.Error("expected workflow to be deleted")
	}
}
