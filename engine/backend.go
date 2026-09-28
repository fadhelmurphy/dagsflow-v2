package engine

import (
	"errors"
	"io"
	"time"
)

// Errors shared by the metadata backends.
var (
	// ErrConflict: the run changed (or is owned by another process) since it
	// was loaded. Reload and retry.
	ErrConflict = errors.New("dag run was changed by another process (or is being executed); reload and retry")
	// ErrNeedsDatabase: the feature needs the Postgres metadata database.
	ErrNeedsDatabase = errors.New("this feature needs the Postgres metadata database (set DAGSFLOW_DATABASE_URL)")
)

// LogKey identifies one task-instance attempt log.
type LogKey struct {
	DagID, RunID, TaskID string
	MapIndex, Try        int
}

// Component is a running dagsflow process (scheduler, webserver, worker, cli)
// registered in the metadata database (~ Airflow's job table).
type Component struct {
	ID        string         `json:"id"`
	Kind      string         `json:"kind"`
	Host      string         `json:"host"`
	PID       int            `json:"pid"`
	Started   time.Time      `json:"started_at"`
	Heartbeat time.Time      `json:"latest_heartbeat"`
	Info      map[string]any `json:"info,omitempty"`
}

// Alive reports whether the component heartbeat is recent.
func (c Component) Alive() bool { return time.Since(c.Heartbeat) < componentTimeout }

// vars so tests can shorten them
var (
	componentTimeout  = 30 * time.Second // a component without heartbeat for this long is dead
	runLease          = 30 * time.Second // ownership lease of a running DagRun
	heartbeatInterval = 5 * time.Second  // component heartbeat + lease renewal
)

// backend is the storage behind Store: JSON files (single node, default) or
// Postgres (multi-node: HA schedulers, remote workers, several webservers).
type backend interface {
	kind() string
	close()

	// dag runs. saveRun honours ownership (owned runs) and optimistic
	// concurrency (runs loaded from the store); see Store.SaveRun.
	saveRun(r *DagRun, me string) error
	loadRun(dagID, runID string) (*DagRun, error)
	runExists(dagID, runID string) bool
	deleteRun(dagID, runID string) error
	listRuns(dagID string, states []string) ([]*DagRun, error)
	claimRun(dagID, runID, me string, maxActive int) (*DagRun, error)
	takeOwnership(r *DagRun, me string) error
	requeueOrphans() ([]string, error)
	// renewLeases extends the leases of the given runs (keys "dag\x1frun")
	// owned by me and returns the keys it renewed.
	renewLeases(me string, keys []string) (map[string]bool, error)
	deleteDagData(dagID string) error
	dagIDs() ([]string, error)

	getDagMeta(dagID string) *DagMeta
	saveDagMeta(m *DagMeta) error

	readTable(name string, v any) error
	updateTable(name string, v any, fn func() error) error
	tableStamp(name string) time.Time

	enqueueCommand(dagID, runID string, m MarkRequest) error
	takeCommands(dagID, runID string) []MarkRequest
	dropCommands(dagID, runID string)

	audit(ev AuditEvent)
	auditEvents(dagID, runID, taskID string, limit int) ([]AuditEvent, error)

	openLog(k LogKey) (io.WriteCloser, string, error)
	readLog(k LogKey) ([]byte, error)
	deleteLogs(dagID, runID string) error

	heartbeat(c Component) error
	components() ([]Component, error)
	removeComponent(id string)
	tryLease(name, me string, ttl time.Duration) (bool, error)

	acquirePoolSlot(pool string, slots, limit int, holder, me string) (bool, error)
	releasePoolSlot(holder string)
	poolUsage() (map[string]int, error)

	secretKey() ([]byte, error)

	enqueueJob(j *TaskJob) (int64, error)
	claimJob(queues []string, worker string, id int64) (*TaskJob, error)
	getJob(id int64) (*TaskJob, error)
	jobHeartbeat(id int64, worker string) (cancel bool, err error)
	finishJob(id int64, worker string, res JobResult) error
	cancelJob(id int64) error
	// findActiveJob: a queued/running job of one attempt (failover without a
	// recorded job id), nil if none.
	findActiveJob(dagID, runID, taskID string, mapIndex, try int) (*TaskJob, error)
	jobStats() (map[string]int, error)

	cleanup(before time.Time) (CleanupStats, error)
}

// CleanupStats is what `db clean` removed.
type CleanupStats struct {
	Runs   int `json:"dag_runs"`
	Audit  int `json:"event_logs"`
	Jobs   int `json:"task_jobs"`
	Logs   int `json:"task_logs"`
	Events int `json:"dataset_events"`
}
