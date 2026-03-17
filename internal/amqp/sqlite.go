package amqp

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/openjobspec/ojs-go-backend-common/core"

	_ "modernc.org/sqlite"
)

type sqliteStore struct {
	mu sync.Mutex
	db *sql.DB
}

func newSQLiteStore(path string) (*sqliteStore, error) {
	db, err := sql.Open("sqlite", path+"?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, data BLOB NOT NULL);
	CREATE TABLE IF NOT EXISTS job_order (seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL UNIQUE);
	CREATE TABLE IF NOT EXISTS dead_jobs (id TEXT PRIMARY KEY, data BLOB NOT NULL);
	CREATE TABLE IF NOT EXISTS crons (name TEXT PRIMARY KEY, data BLOB NOT NULL);
	CREATE TABLE IF NOT EXISTS workflows (id TEXT PRIMARY KEY, data BLOB NOT NULL);
	CREATE TABLE IF NOT EXISTS queue_state (name TEXT PRIMARY KEY, paused INTEGER NOT NULL DEFAULT 0);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}

	return &sqliteStore{db: db}, nil
}

func (s *sqliteStore) SaveJob(job *core.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT OR REPLACE INTO jobs (id, data) VALUES (?, ?)", job.ID, data)
	if err != nil {
		return err
	}
	if _, err := s.db.Exec("INSERT OR IGNORE INTO job_order (job_id) VALUES (?)", job.ID); err != nil {
		return fmt.Errorf("insert job order: %w", err)
	}
	return nil
}

func (s *sqliteStore) DeleteJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("DELETE FROM jobs WHERE id = ?", id)
	if err != nil {
		return err
	}
	if _, err := s.db.Exec("DELETE FROM job_order WHERE job_id = ?", id); err != nil {
		return fmt.Errorf("delete job order: %w", err)
	}
	return nil
}

func (s *sqliteStore) SaveDeadJob(job *core.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT OR REPLACE INTO dead_jobs (id, data) VALUES (?, ?)", job.ID, data)
	return err
}

func (s *sqliteStore) DeleteDeadJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("DELETE FROM dead_jobs WHERE id = ?", id)
	return err
}

func (s *sqliteStore) SaveCron(cron *core.CronJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(cron)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT OR REPLACE INTO crons (name, data) VALUES (?, ?)", cron.Name, data)
	return err
}

func (s *sqliteStore) DeleteCron(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("DELETE FROM crons WHERE name = ?", name)
	return err
}

func (s *sqliteStore) SaveWorkflow(wf *core.Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(wf)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT OR REPLACE INTO workflows (id, data) VALUES (?, ?)", wf.ID, data)
	return err
}

func (s *sqliteStore) DeleteWorkflow(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("DELETE FROM workflows WHERE id = ?", id)
	return err
}

func (s *sqliteStore) SaveQueueState(name, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	paused := 0
	if status == "paused" {
		paused = 1
	}
	_, err := s.db.Exec("INSERT OR REPLACE INTO queue_state (name, paused) VALUES (?, ?)", name, paused)
	return err
}

func (s *sqliteStore) LoadAll() (*persistedState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := &persistedState{
		Jobs:       make(map[string]*core.Job),
		DeadJobs:   make([]*core.Job, 0),
		Crons:      make(map[string]*core.CronJob),
		Workflows:  make(map[string]*core.Workflow),
		QueueState: make(map[string]bool),
	}

	// Load jobs
	rows, err := s.db.Query("SELECT data FROM jobs")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			continue
		}
		var job core.Job
		if err := json.Unmarshal(data, &job); err != nil {
			continue
		}
		state.Jobs[job.ID] = &job
	}

	// Load job order
	orderRows, err := s.db.Query("SELECT job_id FROM job_order ORDER BY seq ASC")
	if err == nil {
		defer orderRows.Close()
		for orderRows.Next() {
			var jobID string
			if err := orderRows.Scan(&jobID); err != nil {
				continue
			}
			if _, exists := state.Jobs[jobID]; exists {
				state.JobOrder = append(state.JobOrder, jobID)
			}
		}
	}

	// Load dead jobs
	deadRows, err := s.db.Query("SELECT data FROM dead_jobs ORDER BY rowid ASC")
	if err == nil {
		defer deadRows.Close()
		for deadRows.Next() {
			var data []byte
			if err := deadRows.Scan(&data); err != nil {
				continue
			}
			var job core.Job
			if err := json.Unmarshal(data, &job); err != nil {
				continue
			}
			state.DeadJobs = append(state.DeadJobs, &job)
		}
	}

	// Load crons
	cronRows, err := s.db.Query("SELECT data FROM crons")
	if err == nil {
		defer cronRows.Close()
		for cronRows.Next() {
			var data []byte
			if err := cronRows.Scan(&data); err != nil {
				continue
			}
			var cron core.CronJob
			if err := json.Unmarshal(data, &cron); err != nil {
				continue
			}
			state.Crons[cron.Name] = &cron
		}
	}

	// Load workflows
	wfRows, err := s.db.Query("SELECT data FROM workflows")
	if err == nil {
		defer wfRows.Close()
		for wfRows.Next() {
			var data []byte
			if err := wfRows.Scan(&data); err != nil {
				continue
			}
			var wf core.Workflow
			if err := json.Unmarshal(data, &wf); err != nil {
				continue
			}
			state.Workflows[wf.ID] = &wf
		}
	}

	// Load queue state
	qRows, err := s.db.Query("SELECT name, paused FROM queue_state")
	if err == nil {
		defer qRows.Close()
		for qRows.Next() {
			var name string
			var paused int
			if err := qRows.Scan(&name, &paused); err != nil {
				continue
			}
			state.QueueState[name] = paused == 1
		}
	}

	return state, nil
}

func (s *sqliteStore) Close() error {
	return s.db.Close()
}
