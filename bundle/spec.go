// Package bundle compiles Go DAG files from the DAG folder on the fly and runs
// them as subprocesses ("DAG bundles"), so new Go DAGs need no rebuild of the
// scheduler.
//
// Host (scheduler/webserver)            Bundle binary (compiled DAG files)
//
//	go build ───────────────────────▶   main() = bundle.Main()
//	`bin describe` ◀─────── JSON ─────  DAG/task specs
//	proxy DAG in the registry
//	task runs: `bin run` + stdin JSON ▶ runs the real Go task
//	RPC server (xcom, variables, ◀────  RPC client (127.0.0.1 + token)
//	connections, trigger, report)
package bundle

import (
	"errors"
	"time"

	"dagsflow-go/dag"
)

// DagSpec / TaskSpec: what the host needs to schedule a DAG it can't execute itself.
type DagSpec struct {
	ID            string         `json:"dag_id"`
	Owner         string         `json:"owner"`
	Schedule      string         `json:"schedule"`
	StartDate     time.Time      `json:"start_date"`
	Catchup       bool           `json:"catchup"`
	MaxActiveRuns int            `json:"max_active_runs"`
	Params        map[string]any `json:"params"`
	Description   string         `json:"description"`
	Tags          []string       `json:"tags"`
	Datasets      []string       `json:"datasets"`
	OnSuccess     int            `json:"on_success"`
	OnFailure     int            `json:"on_failure"`
	OnSLAMiss     int            `json:"on_sla_miss"`
	Tasks         []TaskSpec     `json:"tasks"`
}

type TaskSpec struct {
	ID          string            `json:"task_id"`
	Upstream    []string          `json:"upstream"`
	TriggerRule string            `json:"trigger_rule"`
	Retries     int               `json:"retries"`
	RetryDelay  time.Duration     `json:"retry_delay"`
	Timeout     time.Duration     `json:"timeout"`
	Pool        string            `json:"pool"`
	PoolSlots   int               `json:"pool_slots"`
	Priority    int               `json:"priority_weight"`
	SLA         time.Duration     `json:"sla"`
	Outlets     []string          `json:"outlets"`
	Branch      bool              `json:"branch"`
	Sensor      *dag.SensorConfig `json:"sensor,omitempty"`
	Map         *dag.MapSource    `json:"map,omitempty"`
	OnSuccess   int               `json:"on_success"`
	OnFailure   int               `json:"on_failure"`
	OnRetry     int               `json:"on_retry"`
}

type Describe struct {
	Dags         []DagSpec         `json:"dags"`
	ImportErrors map[string]string `json:"import_errors"`
}

func describe() Describe {
	out := Describe{ImportErrors: map[string]string{}}
	for id, err := range dag.ImportErrors() {
		out.ImportErrors[id] = err.Error()
	}
	for _, d := range dag.List() {
		ds := DagSpec{ID: d.ID, Owner: d.DefaultArgs.Owner, Schedule: d.Schedule, StartDate: d.StartDate, Catchup: d.Catchup,
			MaxActiveRuns: d.MaxActiveRuns, Params: d.Params, Description: d.Description, Tags: d.Tags,
			Datasets: d.Datasets, OnSuccess: len(d.OnSuccess), OnFailure: len(d.OnFailure), OnSLAMiss: len(d.OnSLAMiss)}
		for _, t := range d.Tasks() {
			ts := TaskSpec{ID: t.ID, TriggerRule: string(t.TriggerRule()), Retries: t.Retries(),
				RetryDelay: t.RetryDelay(), Timeout: t.Timeout(), Pool: t.Pool(), PoolSlots: t.PoolSlots(),
				Priority: t.PriorityWeight(), SLA: t.SLA(), Outlets: t.Outlets(), Branch: t.IsBranch(),
				Map: t.MapSource(), OnSuccess: len(t.OnSuccessCallbacks()),
				OnFailure: len(t.OnFailureCallbacks()), OnRetry: len(t.OnRetryCallbacks())}
			if sn := t.Sensor(); sn != nil {
				cfg := sn.SensorConfig
				ts.Sensor = &cfg
			}
			for _, u := range t.Upstream() {
				ts.Upstream = append(ts.Upstream, u.ID)
			}
			ds.Tasks = append(ds.Tasks, ts)
		}
		out.Dags = append(out.Dags, ds)
	}
	return out
}

// Request is sent to `bin run` on stdin.
type Request struct {
	Action   string        `json:"action"` // task | poke | callback
	DagID    string        `json:"dag_id"`
	TaskID   string        `json:"task_id,omitempty"`
	Info     dag.RunInfo   `json:"run_info"`
	Callback string        `json:"callback,omitempty"` // task_on_failure, dag_on_success, ...
	CC       *WireCallback `json:"callback_context,omitempty"`
}

// WireCallback is dag.CallbackContext with the error as a string.
type WireCallback struct {
	Event       string    `json:"event"`
	DagID       string    `json:"dag_id"`
	RunID       string    `json:"run_id"`
	TaskID      string    `json:"task_id"`
	MapIndex    int       `json:"map_index"`
	State       dag.State `json:"state"`
	TryNumber   int       `json:"try_number"`
	LogicalDate time.Time `json:"logical_date"`
	Err         string    `json:"error"`
	LogPath     string    `json:"log_path"`
}

func toWire(c dag.CallbackContext) *WireCallback {
	w := &WireCallback{c.Event, c.DagID, c.RunID, c.TaskID, c.MapIndex, c.State, c.TryNumber, c.LogicalDate, "", c.LogPath}
	if c.Err != nil {
		w.Err = c.Err.Error()
	}
	return w
}

func (w *WireCallback) context() dag.CallbackContext {
	c := dag.CallbackContext{Event: w.Event, DagID: w.DagID, RunID: w.RunID, TaskID: w.TaskID, MapIndex: w.MapIndex,
		State: w.State, TryNumber: w.TryNumber, LogicalDate: w.LogicalDate, LogPath: w.LogPath}
	if w.Err != "" {
		c.Err = errors.New(w.Err)
	}
	return c
}

// Result is reported by the bundle process before it exits.
type Result struct {
	Done     bool     `json:"done"`
	Err      string   `json:"error,omitempty"`
	Selected []string `json:"selected,omitempty"`
	PokeOK   bool     `json:"poke_ok,omitempty"`
}

// RPC argument/reply types.
type Args struct {
	Token    string
	TaskID   string
	Key      string
	Value    any
	DagID    string
	Conf     map[string]any
	Wait     bool
	Logical  time.Time
	Name     string
	Result   Result
	MapIndex int
}

type Reply struct {
	Value any
	Str   string
	Found bool
	Conn  *dag.Connection
}
