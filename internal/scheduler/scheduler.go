// Package scheduler provides background goroutines for the AMQP backend
// to handle cron firing, retryable job promotion, and stalled job reaping.
package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// SchedulerBackend defines the interface for background scheduling operations.
type SchedulerBackend interface {
	PromoteScheduled(ctx context.Context) error
	PromoteRetries(ctx context.Context) error
	RequeueStalled(ctx context.Context) error
	FireCronJobs(ctx context.Context) error
	PurgeExpired(ctx context.Context) error
}

// Scheduler runs background maintenance tasks.
type Scheduler struct {
	backend  SchedulerBackend
	stop     chan struct{}
	stopOnce sync.Once
}

// New creates a new scheduler.
func New(backend SchedulerBackend) *Scheduler {
	return &Scheduler{
		backend: backend,
		stop:    make(chan struct{}),
	}
}

// Start launches the background goroutines.
func (s *Scheduler) Start() {
	go s.runLoop("scheduled-promoter", 1*time.Second, s.backend.PromoteScheduled)
	go s.runLoop("retry-promoter", 200*time.Millisecond, s.backend.PromoteRetries)
	go s.runLoop("stalled-reaper", 500*time.Millisecond, s.backend.RequeueStalled)
	go s.runLoop("cron-scheduler", 10*time.Second, s.backend.FireCronJobs)
	go s.runLoop("expired-purger", 60*time.Second, s.backend.PurgeExpired)
	slog.Info("AMQP scheduler started")
}

// Stop gracefully shuts down all background goroutines. Safe to call multiple times.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
}

func (s *Scheduler) runLoop(name string, interval time.Duration, fn func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := fn(ctx); err != nil {
				slog.Error("scheduler loop error", "loop", name, "error", err)
			}
			cancel()
		}
	}
}
