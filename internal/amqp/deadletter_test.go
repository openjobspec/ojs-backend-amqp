package amqp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

func TestBackend_ListDeadLetter_Empty(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	deadLetters, total, err := b.ListDeadLetter(ctx, 100, 0)
	if err != nil {
		t.Fatalf("ListDeadLetter failed: %v", err)
	}
	if total != 0 {
		t.Errorf("expected total 0, got %d", total)
	}
	if len(deadLetters) != 0 {
		t.Errorf("expected empty list, got %d items", len(deadLetters))
	}
}

func TestBackend_ListDeadLetter_WithJobs(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Create dead jobs via nack exhaustion
	for i := 0; i < 3; i++ {
		pushed, _ := b.Push(ctx, &core.Job{
			Type:  "dead.letter.test",
			Queue: "default",
			Args:  json.RawMessage(`{}`),
			Retry: &core.RetryPolicy{MaxAttempts: 1},
		})

		b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
		b.Nack(ctx, pushed.ID, &core.JobError{
			Code:    "FAILED",
			Message: "test failure",
		}, true)
	}

	deadLetters, total, err := b.ListDeadLetter(ctx, 100, 0)
	if err != nil {
		t.Fatalf("ListDeadLetter failed: %v", err)
	}
	if total != 3 {
		t.Errorf("expected total 3, got %d", total)
	}
	if len(deadLetters) != 3 {
		t.Errorf("expected 3 dead letters, got %d", len(deadLetters))
	}

	for _, dl := range deadLetters {
		if dl.State != "discarded" {
			t.Errorf("expected state 'discarded', got %q", dl.State)
		}
	}
}

func TestBackend_ListDeadLetter_Pagination(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Create 5 dead jobs
	for i := 0; i < 5; i++ {
		pushed, _ := b.Push(ctx, &core.Job{
			Type:  "dead.paginate",
			Queue: "default",
			Args:  json.RawMessage(`{}`),
			Retry: &core.RetryPolicy{MaxAttempts: 1},
		})
		b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
		b.Nack(ctx, pushed.ID, nil, true)
	}

	// Page 1: limit 2, offset 0
	page1, total, _ := b.ListDeadLetter(ctx, 2, 0)
	if total != 5 {
		t.Errorf("expected total 5, got %d", total)
	}
	if len(page1) != 2 {
		t.Errorf("expected 2 results, got %d", len(page1))
	}

	// Page 2: limit 2, offset 2
	page2, _, _ := b.ListDeadLetter(ctx, 2, 2)
	if len(page2) != 2 {
		t.Errorf("expected 2 results, got %d", len(page2))
	}

	// Page 3: limit 2, offset 4
	page3, _, _ := b.ListDeadLetter(ctx, 2, 4)
	if len(page3) != 1 {
		t.Errorf("expected 1 result, got %d", len(page3))
	}

	// Beyond: offset >= total
	beyond, _, _ := b.ListDeadLetter(ctx, 2, 10)
	if len(beyond) != 0 {
		t.Errorf("expected 0 results beyond total, got %d", len(beyond))
	}
}

func TestBackend_RetryDeadLetter_Extended(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Create and exhaust a job
	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "retry.dead",
		Queue: "retry-q",
		Args:  json.RawMessage(`{"data":"important"}`),
		Retry: &core.RetryPolicy{MaxAttempts: 1},
	})

	b.Fetch(ctx, []string{"retry-q"}, 1, "worker-1", 30000)
	b.Nack(ctx, pushed.ID, nil, true)

	// Verify it's in dead letters
	deadLetters, _, _ := b.ListDeadLetter(ctx, 100, 0)
	if len(deadLetters) == 0 {
		t.Fatal("expected dead letter job")
	}

	// Retry it
	retried, err := b.RetryDeadLetter(ctx, pushed.ID)
	if err != nil {
		t.Fatalf("RetryDeadLetter failed: %v", err)
	}

	if retried.State != "available" {
		t.Errorf("expected state 'available', got %q", retried.State)
	}
	if retried.Attempt != 0 {
		t.Errorf("expected attempt 0 (reset), got %d", retried.Attempt)
	}
	if retried.EnqueuedAt == "" {
		t.Error("expected EnqueuedAt to be set")
	}

	// Should not be in dead letters anymore
	deadLetters, total, _ := b.ListDeadLetter(ctx, 100, 0)
	if total != 0 {
		t.Errorf("expected 0 dead letters after retry, got %d", total)
	}
	_ = deadLetters

	// Should be fetchable again
	fetched, _ := b.Fetch(ctx, []string{"retry-q"}, 1, "worker-2", 30000)
	if len(fetched) != 1 {
		t.Fatalf("expected 1 fetchable job after retry, got %d", len(fetched))
	}
	if fetched[0].ID != pushed.ID {
		t.Errorf("expected fetched job ID %q, got %q", pushed.ID, fetched[0].ID)
	}
}

func TestBackend_RetryDeadLetter_NotFound(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	_, err := b.RetryDeadLetter(ctx, "nonexistent-dead-job")
	if err == nil {
		t.Fatal("expected error for non-existent dead job")
	}

	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != "not_found" {
		t.Errorf("expected code 'not_found', got %q", ojsErr.Code)
	}
}

func TestBackend_DeleteDeadLetter_Extended(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	// Create dead job
	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "delete.dead",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
		Retry: &core.RetryPolicy{MaxAttempts: 1},
	})
	b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
	b.Nack(ctx, pushed.ID, nil, true)

	// Delete it
	err := b.DeleteDeadLetter(ctx, pushed.ID)
	if err != nil {
		t.Fatalf("DeleteDeadLetter failed: %v", err)
	}

	// Not in dead letters
	deadLetters, total, _ := b.ListDeadLetter(ctx, 100, 0)
	if total != 0 {
		t.Errorf("expected 0 dead letters, got %d", total)
	}
	_ = deadLetters

	// Not in jobs either
	_, err = b.Info(ctx, pushed.ID)
	if err == nil {
		t.Error("expected job to be completely removed after DeleteDeadLetter")
	}
}

func TestBackend_DeleteDeadLetter_NotFound(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	err := b.DeleteDeadLetter(ctx, "nonexistent-dead-job")
	if err == nil {
		t.Fatal("expected error for non-existent dead job")
	}

	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != "not_found" {
		t.Errorf("expected code 'not_found', got %q", ojsErr.Code)
	}
}

func TestBackend_Nack_ExhaustsRetries(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "exhaust.retries",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
		Retry: &core.RetryPolicy{MaxAttempts: 2},
	})

	// Attempt 1: fetch and nack — should become retryable
	b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
	resp1, _ := b.Nack(ctx, pushed.ID, &core.JobError{
		Code:    "ERR1",
		Message: "first failure",
	}, true)

	if resp1.State != "retryable" {
		t.Errorf("attempt 1: expected 'retryable', got %q", resp1.State)
	}
	if resp1.Attempt != 1 {
		t.Errorf("attempt 1: expected attempt 1, got %d", resp1.Attempt)
	}

	// Make job available again for second attempt
	b.mu.Lock()
	b.jobs[pushed.ID].State = "available"
	b.jobs[pushed.ID].ScheduledAt = ""
	b.adjustStats("default", "retryable", "available")
	b.mu.Unlock()

	// Attempt 2: fetch and nack — should be discarded (MaxAttempts=2, attempt=2)
	b.Fetch(ctx, []string{"default"}, 1, "worker-2", 30000)
	resp2, _ := b.Nack(ctx, pushed.ID, &core.JobError{
		Code:    "ERR2",
		Message: "second failure",
	}, true)

	if resp2.State != "discarded" {
		t.Errorf("attempt 2: expected 'discarded', got %q", resp2.State)
	}

	// Should be in dead letter queue
	deadLetters, total, _ := b.ListDeadLetter(ctx, 100, 0)
	if total != 1 {
		t.Errorf("expected 1 dead letter, got %d", total)
	}

	found := false
	for _, dl := range deadLetters {
		if dl.ID == pushed.ID {
			found = true
			if dl.Error == nil {
				t.Error("expected error to be set on dead letter job")
			}
			break
		}
	}
	if !found {
		t.Error("expected job to be in dead letter queue")
	}
}

func TestBackend_Nack_NoRequeue_NoRetryPolicy(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	pushed, _ := b.Push(ctx, &core.Job{
		Type:  "nack.no_requeue",
		Queue: "default",
		Args:  json.RawMessage(`{}`),
		// No retry policy
	})

	b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)

	resp, _ := b.Nack(ctx, pushed.ID, nil, false)
	if resp.State != "discarded" {
		t.Errorf("expected 'discarded' (no requeue, no retry), got %q", resp.State)
	}

	deadLetters, total, _ := b.ListDeadLetter(ctx, 100, 0)
	if total != 1 {
		t.Errorf("expected 1 dead letter, got %d", total)
	}
	_ = deadLetters
}

func TestBackend_DeadLetter_MultipleJobs_Order(t *testing.T) {
	b := NewWithoutConnection()
	defer b.Close()
	ctx := context.Background()

	var pushedIDs []string
	for i := 0; i < 4; i++ {
		pushed, _ := b.Push(ctx, &core.Job{
			Type:  "dead.order",
			Queue: "default",
			Args:  json.RawMessage(`{}`),
			Retry: &core.RetryPolicy{MaxAttempts: 1},
		})
		pushedIDs = append(pushedIDs, pushed.ID)
		b.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
		b.Nack(ctx, pushed.ID, nil, true)
	}

	deadLetters, total, _ := b.ListDeadLetter(ctx, 100, 0)
	if total != 4 {
		t.Errorf("expected 4 dead letters, got %d", total)
	}

	// Verify order: dead letters should be in insertion order
	for i, dl := range deadLetters {
		if dl.ID != pushedIDs[i] {
			t.Errorf("dead letter[%d]: expected ID %q, got %q", i, pushedIDs[i], dl.ID)
		}
	}
}
