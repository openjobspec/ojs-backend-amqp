package amqp

import "github.com/openjobspec/ojs-go-backend-common/core"

// persistStore defines the interface for optional state persistence.
type persistStore interface {
	SaveJob(job *core.Job) error
	DeleteJob(id string) error
	SaveDeadJob(job *core.Job) error
	DeleteDeadJob(id string) error
	SaveCron(cron *core.CronJob) error
	DeleteCron(name string) error
	SaveWorkflow(wf *core.Workflow) error
	DeleteWorkflow(id string) error
	SaveQueueState(name, status string) error
	LoadAll() (*persistedState, error)
	Close() error
}

type persistedState struct {
	Jobs       map[string]*core.Job
	JobOrder   []string
	DeadJobs   []*core.Job
	Crons      map[string]*core.CronJob
	Workflows  map[string]*core.Workflow
	QueueState map[string]bool // queue name -> paused
}
