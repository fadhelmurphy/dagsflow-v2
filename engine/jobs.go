package engine

import "time"

// Job states of the task queue (remote executors).
const (
	JobQueued    = "queued"
	JobRunning   = "running"
	JobSuccess   = "success"
	JobFailed    = "failed"
	JobCancelled = "cancelled"
)

// TaskJob is one task-instance attempt handed to a remote executor (worker
// processes or Kubernetes pods) through the metadata database, like a Celery
// message. The scheduler keeps supervising the DagRun; the job carries
// everything the task needs and the result carries what it produced.
type TaskJob struct {
	ID        int64      `json:"id"`
	Queue     string     `json:"queue"`
	State     string     `json:"state"`
	Worker    string     `json:"worker,omitempty"`
	Cancel    bool       `json:"cancel,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	Heartbeat *time.Time `json:"heartbeat_at,omitempty"`
	Payload   JobPayload `json:"payload"`
	Result    *JobResult `json:"result,omitempty"`
}

// JobPayload: the task instance to run and a snapshot of the run it needs.
// Upstream tasks are finished before a task starts, so the XCom snapshot is
// exactly what XComPull would see locally.
type JobPayload struct {
	DagID             string                    `json:"dag_id"`
	RunID             string                    `json:"run_id"`
	RunType           string                    `json:"run_type"`
	LogicalDate       time.Time                 `json:"logical_date"`
	DataIntervalStart time.Time                 `json:"data_interval_start"`
	DataIntervalEnd   time.Time                 `json:"data_interval_end"`
	Conf              map[string]any            `json:"conf,omitempty"`
	TaskID            string                    `json:"task_id"`
	MapIndex          int                       `json:"map_index"`
	Try               int                       `json:"try_number"`
	Item              any                       `json:"item,omitempty"`
	XCom              map[string]map[string]any `json:"xcom,omitempty"`
	MappedLen         map[string]int            `json:"mapped_len,omitempty"`
	// Preview: a builder "Run node" request instead of a task instance.
	Preview *PreviewRequest `json:"preview,omitempty"`
}

// JobResult is reported by the executor when the attempt ends.
type JobResult struct {
	Error         string                    `json:"error,omitempty"`
	SensorTimeout bool                      `json:"sensor_timeout,omitempty"`
	Selected      []string                  `json:"branch_selected,omitempty"`
	XCom          map[string]any            `json:"xcom,omitempty"`
	Rendered      map[string]string         `json:"rendered,omitempty"`
	OutletExtra   map[string]map[string]any `json:"outlet_extra,omitempty"`
	Preview       *PreviewResult            `json:"preview,omitempty"`
}
