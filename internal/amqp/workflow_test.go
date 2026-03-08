package amqp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

func TestBackend_CreateWorkflow_Chain(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	req := &core.WorkflowRequest{
		Type: "chain",
		Name: "data-pipeline",
		Steps: []core.WorkflowJobRequest{
			{Type: "extract", Args: json.RawMessage(`{"source":"db"}`)},
			{Type: "transform", Args: json.RawMessage(`{}`)},
			{Type: "load", Args: json.RawMessage(`{"dest":"warehouse"}`)},
		},
	}

	wf, err := b.CreateWorkflow(ctx, req)
	if err != nil {
		t.Fatalf("CreateWorkflow failed: %v", err)
	}

	if wf.ID == "" {
		t.Fatal("workflow ID should be generated")
	}
	if wf.Name != "data-pipeline" {
		t.Errorf("expected name 'data-pipeline', got %q", wf.Name)
	}
	if wf.Type != "chain" {
		t.Errorf("expected type 'chain', got %q", wf.Type)
	}
	if wf.State != "active" {
		t.Errorf("expected state 'active', got %q", wf.State)
	}
	if wf.CreatedAt == "" {
		t.Error("CreatedAt should be set")
	}
	if wf.StepsTotal == nil || *wf.StepsTotal != 3 {
		t.Errorf("expected StepsTotal 3, got %v", wf.StepsTotal)
	}
	if wf.StepsCompleted == nil || *wf.StepsCompleted != 0 {
		t.Errorf("expected StepsCompleted 0, got %v", wf.StepsCompleted)
	}
	// Chain workflows should not have JobsTotal/JobsCompleted
	if wf.JobsTotal != nil {
		t.Errorf("chain workflow should not have JobsTotal, got %v", wf.JobsTotal)
	}
}

func TestBackend_CreateWorkflow_Group_Extended(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	req := &core.WorkflowRequest{
		Type: "group",
		Name: "parallel-processing",
		Jobs: []core.WorkflowJobRequest{
			{Type: "process-a", Args: json.RawMessage(`{}`)},
			{Type: "process-b", Args: json.RawMessage(`{}`)},
			{Type: "process-c", Args: json.RawMessage(`{}`)},
			{Type: "process-d", Args: json.RawMessage(`{}`)},
		},
	}

	wf, err := b.CreateWorkflow(ctx, req)
	if err != nil {
		t.Fatalf("CreateWorkflow failed: %v", err)
	}

	if wf.Type != "group" {
		t.Errorf("expected type 'group', got %q", wf.Type)
	}
	if wf.JobsTotal == nil || *wf.JobsTotal != 4 {
		t.Errorf("expected JobsTotal 4, got %v", wf.JobsTotal)
	}
	if wf.JobsCompleted == nil || *wf.JobsCompleted != 0 {
		t.Errorf("expected JobsCompleted 0, got %v", wf.JobsCompleted)
	}
	if wf.StepsTotal != nil {
		t.Errorf("group workflow should not have StepsTotal, got %v", wf.StepsTotal)
	}
}

func TestBackend_CreateWorkflow_Batch(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	req := &core.WorkflowRequest{
		Type: "batch",
		Name: "batch-import",
		Jobs: []core.WorkflowJobRequest{
			{Type: "import.chunk", Args: json.RawMessage(`{"chunk":1}`)},
			{Type: "import.chunk", Args: json.RawMessage(`{"chunk":2}`)},
		},
	}

	wf, err := b.CreateWorkflow(ctx, req)
	if err != nil {
		t.Fatalf("CreateWorkflow failed: %v", err)
	}

	if wf.Type != "batch" {
		t.Errorf("expected type 'batch', got %q", wf.Type)
	}
	if wf.State != "active" {
		t.Errorf("expected state 'active', got %q", wf.State)
	}
	if wf.JobsTotal == nil || *wf.JobsTotal != 2 {
		t.Errorf("expected JobsTotal 2, got %v", wf.JobsTotal)
	}
}

func TestBackend_GetWorkflow_Extended(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	req := &core.WorkflowRequest{
		Type: "chain",
		Name: "my-chain",
		Steps: []core.WorkflowJobRequest{
			{Type: "step1", Args: json.RawMessage(`{}`)},
			{Type: "step2", Args: json.RawMessage(`{}`)},
		},
	}

	created, err := b.CreateWorkflow(ctx, req)
	if err != nil {
		t.Fatalf("CreateWorkflow failed: %v", err)
	}

	retrieved, err := b.GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflow failed: %v", err)
	}

	if retrieved.ID != created.ID {
		t.Errorf("expected ID %q, got %q", created.ID, retrieved.ID)
	}
	if retrieved.Name != "my-chain" {
		t.Errorf("expected name 'my-chain', got %q", retrieved.Name)
	}
	if retrieved.Type != "chain" {
		t.Errorf("expected type 'chain', got %q", retrieved.Type)
	}
	if retrieved.State != "active" {
		t.Errorf("expected state 'active', got %q", retrieved.State)
	}
}

func TestBackend_GetWorkflow_NotFound(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	_, err := b.GetWorkflow(ctx, "nonexistent-wf-id")
	if err == nil {
		t.Fatal("expected error for non-existent workflow")
	}

	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != "not_found" {
		t.Errorf("expected code 'not_found', got %q", ojsErr.Code)
	}
}

func TestBackend_CancelWorkflow_Extended(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	req := &core.WorkflowRequest{
		Type: "group",
		Jobs: []core.WorkflowJobRequest{
			{Type: "job1", Args: json.RawMessage(`{}`)},
			{Type: "job2", Args: json.RawMessage(`{}`)},
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

	// Verify via GetWorkflow
	retrieved, _ := b.GetWorkflow(ctx, created.ID)
	if retrieved.State != "cancelled" {
		t.Errorf("GetWorkflow: expected state 'cancelled', got %q", retrieved.State)
	}
}

func TestBackend_AdvanceWorkflow_Chain(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	req := &core.WorkflowRequest{
		Type: "chain",
		Steps: []core.WorkflowJobRequest{
			{Type: "step1", Args: json.RawMessage(`{}`)},
			{Type: "step2", Args: json.RawMessage(`{}`)},
			{Type: "step3", Args: json.RawMessage(`{}`)},
		},
	}

	wf, _ := b.CreateWorkflow(ctx, req)

	// Advance step 1
	if err := b.AdvanceWorkflow(ctx, wf.ID, "step1", json.RawMessage(`{"r":1}`), false); err != nil {
		t.Fatalf("AdvanceWorkflow step1 failed: %v", err)
	}
	retrieved, _ := b.GetWorkflow(ctx, wf.ID)
	if retrieved.State != "active" {
		t.Errorf("after step1: expected state 'active', got %q", retrieved.State)
	}
	if *retrieved.StepsCompleted != 1 {
		t.Errorf("after step1: expected StepsCompleted 1, got %d", *retrieved.StepsCompleted)
	}

	// Advance step 2
	if err := b.AdvanceWorkflow(ctx, wf.ID, "step2", json.RawMessage(`{"r":2}`), false); err != nil {
		t.Fatalf("AdvanceWorkflow step2 failed: %v", err)
	}
	retrieved, _ = b.GetWorkflow(ctx, wf.ID)
	if retrieved.State != "active" {
		t.Errorf("after step2: expected state 'active', got %q", retrieved.State)
	}
	if *retrieved.StepsCompleted != 2 {
		t.Errorf("after step2: expected StepsCompleted 2, got %d", *retrieved.StepsCompleted)
	}

	// Advance step 3 — should complete workflow
	if err := b.AdvanceWorkflow(ctx, wf.ID, "step3", json.RawMessage(`{"r":3}`), false); err != nil {
		t.Fatalf("AdvanceWorkflow step3 failed: %v", err)
	}
	retrieved, _ = b.GetWorkflow(ctx, wf.ID)
	if retrieved.State != "completed" {
		t.Errorf("after step3: expected state 'completed', got %q", retrieved.State)
	}
	if *retrieved.StepsCompleted != 3 {
		t.Errorf("after step3: expected StepsCompleted 3, got %d", *retrieved.StepsCompleted)
	}
	if retrieved.CompletedAt == "" {
		t.Error("CompletedAt should be set on completion")
	}
}

func TestBackend_AdvanceWorkflow_Group_Extended(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	req := &core.WorkflowRequest{
		Type: "group",
		Jobs: []core.WorkflowJobRequest{
			{Type: "job-a", Args: json.RawMessage(`{}`)},
			{Type: "job-b", Args: json.RawMessage(`{}`)},
			{Type: "job-c", Args: json.RawMessage(`{}`)},
		},
	}

	wf, _ := b.CreateWorkflow(ctx, req)

	// Advance first two jobs
	b.AdvanceWorkflow(ctx, wf.ID, "job-a", nil, false)
	b.AdvanceWorkflow(ctx, wf.ID, "job-b", nil, false)

	retrieved, _ := b.GetWorkflow(ctx, wf.ID)
	if retrieved.State != "active" {
		t.Errorf("after 2/3: expected state 'active', got %q", retrieved.State)
	}
	if *retrieved.JobsCompleted != 2 {
		t.Errorf("after 2/3: expected JobsCompleted 2, got %d", *retrieved.JobsCompleted)
	}

	// Advance last job — should complete
	b.AdvanceWorkflow(ctx, wf.ID, "job-c", nil, false)

	retrieved, _ = b.GetWorkflow(ctx, wf.ID)
	if retrieved.State != "completed" {
		t.Errorf("after 3/3: expected state 'completed', got %q", retrieved.State)
	}
	if retrieved.CompletedAt == "" {
		t.Error("CompletedAt should be set on completion")
	}
}

func TestBackend_AdvanceWorkflow_Failed(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	req := &core.WorkflowRequest{
		Type: "chain",
		Steps: []core.WorkflowJobRequest{
			{Type: "step1", Args: json.RawMessage(`{}`)},
			{Type: "step2", Args: json.RawMessage(`{}`)},
		},
	}

	wf, _ := b.CreateWorkflow(ctx, req)

	// Advance with failure
	if err := b.AdvanceWorkflow(ctx, wf.ID, "step1", nil, true); err != nil {
		t.Fatalf("AdvanceWorkflow with failure failed: %v", err)
	}

	retrieved, _ := b.GetWorkflow(ctx, wf.ID)
	if retrieved.State != "failed" {
		t.Errorf("expected state 'failed', got %q", retrieved.State)
	}
	if retrieved.CompletedAt == "" {
		t.Error("CompletedAt should be set on failure")
	}
}

func TestBackend_AdvanceWorkflow_NotFound(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	err := b.AdvanceWorkflow(ctx, "nonexistent-wf", "step1", nil, false)
	if err == nil {
		t.Fatal("expected error for non-existent workflow")
	}

	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != "not_found" {
		t.Errorf("expected code 'not_found', got %q", ojsErr.Code)
	}
}

func TestBackend_CancelWorkflow_NotFound(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	_, err := b.CancelWorkflow(ctx, "nonexistent-wf")
	if err == nil {
		t.Fatal("expected error for non-existent workflow")
	}

	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != "not_found" {
		t.Errorf("expected code 'not_found', got %q", ojsErr.Code)
	}
}

func TestBackend_AdvanceWorkflow_Batch_Completion(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	req := &core.WorkflowRequest{
		Type: "batch",
		Jobs: []core.WorkflowJobRequest{
			{Type: "batch-1", Args: json.RawMessage(`{}`)},
			{Type: "batch-2", Args: json.RawMessage(`{}`)},
		},
	}

	wf, _ := b.CreateWorkflow(ctx, req)

	b.AdvanceWorkflow(ctx, wf.ID, "batch-1", nil, false)
	retrieved, _ := b.GetWorkflow(ctx, wf.ID)
	if retrieved.State != "active" {
		t.Errorf("after 1/2: expected 'active', got %q", retrieved.State)
	}

	b.AdvanceWorkflow(ctx, wf.ID, "batch-2", nil, false)
	retrieved, _ = b.GetWorkflow(ctx, wf.ID)
	if retrieved.State != "completed" {
		t.Errorf("after 2/2: expected 'completed', got %q", retrieved.State)
	}
}
