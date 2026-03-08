package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"
)

// --- Mock Backend for Testing ---

type mockBackend struct {
	mu              sync.Mutex
	promotedCount   int
	retriesCount    int
	stalledCount    int
	cronCount       int
	purgedCount     int
}

func (m *mockBackend) PromoteScheduled(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.promotedCount++
	return nil
}

func (m *mockBackend) PromoteRetries(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retriesCount++
	return nil
}

func (m *mockBackend) RequeueStalled(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stalledCount++
	return nil
}

func (m *mockBackend) FireCronJobs(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cronCount++
	return nil
}

func (m *mockBackend) PurgeExpired(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purgedCount++
	return nil
}

// --- Scheduler Tests ---

func TestScheduler_New(t *testing.T) {
	mock := &mockBackend{}
	sched := New(mock)

	if sched == nil {
		t.Fatal("expected scheduler to be created")
	}
	if sched.backend != mock {
		t.Error("backend not set correctly")
	}
}

func TestScheduler_StartStop(t *testing.T) {
	mock := &mockBackend{}
	sched := New(mock)

	sched.Start()
	time.Sleep(600 * time.Millisecond)
	sched.Stop()

	mock.mu.Lock()
	defer mock.mu.Unlock()

	// Retry promoter runs every 200ms, so ~3 invocations in 600ms
	if mock.retriesCount < 1 {
		t.Error("expected PromoteRetries to be called at least once")
	}

	// Stalled reaper runs every 500ms, so ~1 invocation
	if mock.stalledCount < 1 {
		t.Error("expected RequeueStalled to be called at least once")
	}
}

func TestScheduler_DoubleStop(t *testing.T) {
	mock := &mockBackend{}
	sched := New(mock)

	sched.Start()
	time.Sleep(100 * time.Millisecond)

	// Should not panic
	sched.Stop()
	sched.Stop()
}

func TestScheduler_AllLoopsRun(t *testing.T) {
	mock := &mockBackend{}
	sched := New(mock)

	sched.Start()
	// Wait long enough for all loops to fire at least once.
	// Slowest loop is expired-purger at 60s, but we only need
	// the faster ones for a quick smoke test.
	time.Sleep(1200 * time.Millisecond)
	sched.Stop()

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if mock.promotedCount < 1 {
		t.Error("expected PromoteScheduled to be called at least once")
	}
	if mock.retriesCount < 1 {
		t.Error("expected PromoteRetries to be called at least once")
	}
	if mock.stalledCount < 1 {
		t.Error("expected RequeueStalled to be called at least once")
	}
}
