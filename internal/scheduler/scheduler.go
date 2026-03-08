// Package scheduler provides background goroutines for the AMQP backend
// to handle cron firing, retryable job promotion, and stalled job reaping.
package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

// Backend is the subset of core.Backend needed by the scheduler.
type Backend interface {
	ListCron(ctx context.Context) ([]*core.CronJob, error)
	Push(ctx context.Context, job *core.Job) (*core.Job, error)
}

// Scheduler runs background maintenance tasks.
type Scheduler struct {
	backend Backend
	done    chan struct{}
	wg      sync.WaitGroup
}

// New creates a new scheduler.
func New(backend Backend) *Scheduler {
	return &Scheduler{
		backend: backend,
		done:    make(chan struct{}),
	}
}

// Start launches the background goroutines.
func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.cronLoop()

	slog.Info("AMQP scheduler started")
}

// Stop gracefully shuts down all background goroutines.
func (s *Scheduler) Stop() {
	close(s.done)
	s.wg.Wait()
	slog.Info("AMQP scheduler stopped")
}

// cronLoop checks registered cron jobs every minute and fires any that are due.
func (s *Scheduler) cronLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.fireCrons()
		}
	}
}

func (s *Scheduler) fireCrons() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	crons, err := s.backend.ListCron(ctx)
	if err != nil {
		slog.Error("scheduler: list crons failed", "error", err)
		return
	}

	now := time.Now().UTC()
	for _, cron := range crons {
		if !cron.Enabled {
			continue
		}

		// Check if this cron should fire based on NextRunAt
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

		// Fire the cron job
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

			if _, err := s.backend.Push(ctx, job); err != nil {
				slog.Error("scheduler: fire cron failed", "name", cron.Name, "error", err)
			} else {
				slog.Debug("scheduler: fired cron job", "name", cron.Name, "type", job.Type)
			}
		}
	}
}
