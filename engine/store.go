package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"dagsflow-go/dag"
)

// Run states (Airflow DagRunState)
const (
	RunQueued  = "queued"
	RunRunning = "running"
	RunSuccess = "success"
	RunFailed  = "failed"
)

// Run types
const (
	RunTypeScheduled = "scheduled"
	RunTypeManual    = "manual"
	RunTypeBackfill  = "backfill"
	RunTypeDataset   = "dataset_triggered"
)

type TaskInstance struct {
	TaskID    string     `json:"task_id"`
	MapIndex  int        `json:"map_index"`
	Pool      string     `json:"pool,omitempty"`
	State     dag.State  `json:"state"`
	TryNumber int        `json:"try_number"`
	StartDate *time.Time `json:"start_date,omitempty"`
	EndDate   *time.Time `json:"end_date,omitempty"`
	Error     string     `json:"error,omitempty"`
	Note      string     `json:"note,omitempty"`
	// MaxTries is the last try number allowed for the current attempt series.
	// 0 = retries+1. Clearing extends it (Airflow: max_tries = try_number + retries),
	// so the try number keeps growing across clears instead of restarting at 1.
	MaxTries int `json:"max_tries,omitempty"`
}

// MaxTriesFor returns the effective max tries of a task instance.
func (ti *TaskInstance) MaxTriesFor(retries int) int {
	if ti.MaxTries > 0 {
		return ti.MaxTries
	}
	return retries + 1
}

// clearTI resets a task instance so it runs again, keeping its try number.
func clearTI(r *DagRun, ti *TaskInstance, retries int) {
	ti.State, ti.StartDate, ti.EndDate, ti.Error = dag.StateNone, nil, nil, ""
	if ti.TryNumber > 0 {
		ti.MaxTries = ti.TryNumber + retries + 1
	}
	slot := ti.TaskID
	if ti.MapIndex >= 0 {
		slot = mappedKey(ti.TaskID, ti.MapIndex)
	}
	delete(r.XCom, slot)
	delete(r.Rendered, slot)
}

// clearTask clears a task; for a mapped task either every instance or only
// mapIndex. The expansion is kept so instances keep their try numbers.
// Returns false when nothing matched (onlyFailed).
func clearTask(r *DagRun, t *dag.Task, mapIndex int, onlyFailed bool) bool {
	ti := r.Tasks[t.ID]
	if ti == nil {
		ti = &TaskInstance{TaskID: t.ID, MapIndex: -1}
		r.Tasks[t.ID] = ti
	}
	failed := func(s dag.State) bool { return s == dag.StateFailed || s == dag.StateUpstreamFailed }
	kids := r.Mapped[t.ID]
	if len(kids) == 0 {
		if onlyFailed && !failed(ti.State) {
			return false
		}
		clearTI(r, ti, t.Retries())
		return true
	}
	any := false
	for i, k := range kids {
		if (mapIndex >= 0 && i != mapIndex) || (onlyFailed && !failed(k.State)) {
			continue
		}
		clearTI(r, k, t.Retries())
		any = true
	}
	if !any && !(onlyFailed && failed(ti.State)) {
		return false
	}
	// the aggregate reruns: the executor re-expands (keeping instances) and
	// launches only the instances that are no longer finished
	ti.State, ti.StartDate, ti.EndDate, ti.Error = dag.StateNone, nil, nil, ""
	return true
}

type DagRun struct {
	DagID             string                    `json:"dag_id"`
	RunID             string                    `json:"run_id"`
	RunType           string                    `json:"run_type"`
	State             string                    `json:"state"`
	LogicalDate       time.Time                 `json:"logical_date"`
	DataIntervalStart time.Time                 `json:"data_interval_start"`
	DataIntervalEnd   time.Time                 `json:"data_interval_end"`
	Conf              map[string]any            `json:"conf,omitempty"`
	QueuedAt          time.Time                 `json:"queued_at"`
	StartDate         *time.Time                `json:"start_date,omitempty"`
	EndDate           *time.Time                `json:"end_date,omitempty"`
	Tasks             map[string]*TaskInstance  `json:"task_instances"`
	XCom              map[string]map[string]any `json:"xcom,omitempty"`
	// Mapped holds the expanded instances of mapped tasks; Tasks[id] is their aggregate.
	Mapped    map[string][]*TaskInstance `json:"mapped_task_instances,omitempty"`
	SLAMisses []SLAMiss                  `json:"sla_misses,omitempty"`
	// Rendered template fields per task instance slot (task or task#mapIndex).
	Rendered map[string]map[string]string `json:"rendered_fields,omitempty"`
	Note     string                       `json:"note,omitempty"`
}

type SLAMiss struct {
	TaskID   string    `json:"task_id"`
	Deadline time.Time `json:"deadline"`
	Detected time.Time `json:"detected"`
}

// DagMeta ~ Airflow's `dag` table row.
type DagMeta struct {
	DagID                 string     `json:"dag_id"`
	IsPaused              bool       `json:"is_paused"`
	NextDataIntervalStart *time.Time `json:"next_data_interval_start,omitempty"`
	// DatasetConsumed: per dataset URI, timestamp of the latest event already
	// used to trigger this DAG (data-aware scheduling).
	DatasetConsumed map[string]time.Time `json:"dataset_consumed,omitempty"`
}

// Store persists metadata as JSON files under $DAGSFLOW_HOME/state.
// It plays the role of Airflow's metadata database.
type Store struct{ Home string }

func NewStore(home string) *Store { return &Store{Home: home} }

func (s *Store) runsDir(dagID string) string { return filepath.Join(s.Home, "state", "runs", dagID) }
func (s *Store) metaPath(dagID string) string {
	return filepath.Join(s.Home, "state", "dags", dagID+".json")
}
func (s *Store) LogsDir() string { return filepath.Join(s.Home, "logs") }

func mappedKey(taskID string, idx int) string { return fmt.Sprintf("%s#%d", taskID, idx) }

func safeName(runID string) string {
	return strings.NewReplacer(":", "_", "/", "_", "\\", "_").Replace(runID)
}

func (s *Store) runPath(dagID, runID string) string {
	return filepath.Join(s.runsDir(dagID), safeName(runID)+".json")
}

func writeJSONAtomic(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp%d", path, time.Now().UnixNano())
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return renameReplace(tmp, path)
}

// renameReplace atomically replaces path. On Windows the replace fails while
// another process (e.g. the webserver) has the file open, so it is retried
// for up to ~2s instead of losing the write.
func renameReplace(tmp, path string) error {
	var err error
	for i := 0; i < 30; i++ {
		if err = os.Rename(tmp, path); err == nil {
			return nil
		}
		if runtime.GOOS != "windows" {
			break
		}
		time.Sleep(time.Duration(10+i*5) * time.Millisecond)
	}
	_ = os.Remove(tmp)
	return err
}

func readJSON(path string, v any) error {
	var b []byte
	var err error
	for i := 0; i < 10; i++ { // Windows: the file may be mid-replace (sharing violation)
		if b, err = os.ReadFile(path); err == nil || os.IsNotExist(err) || runtime.GOOS != "windows" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func (s *Store) SaveRun(r *DagRun) error { return writeJSONAtomic(s.runPath(r.DagID, r.RunID), r) }

func (s *Store) DeleteRun(dagID, runID string) error {
	if err := os.Remove(s.runPath(dagID, runID)); err != nil {
		return err
	}
	_ = os.RemoveAll(filepath.Join(s.LogsDir(), "dag_id="+dagID, "run_id="+safeName(runID)))
	return nil
}

// DeleteDagData removes every run, the DAG's metadata and its logs (Airflow's
// "Delete DAG"): the definition itself stays and shows up again empty.
func (s *Store) DeleteDagData(dagID string) error {
	for _, p := range []string{s.runsDir(dagID), s.metaPath(dagID), filepath.Join(s.LogsDir(), "dag_id="+dagID)} {
		if err := os.RemoveAll(p); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) RunExists(dagID, runID string) bool {
	_, err := os.Stat(s.runPath(dagID, runID))
	return err == nil
}

func (s *Store) LoadRun(dagID, runID string) (*DagRun, error) {
	var r DagRun
	if err := readJSON(s.runPath(dagID, runID), &r); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("dag run %s/%s not found", dagID, runID)
		}
		return nil, err
	}
	return &r, nil
}

// ListRuns returns runs of a DAG ordered by logical date (oldest first).
func (s *Store) ListRuns(dagID string) ([]*DagRun, error) {
	entries, err := os.ReadDir(s.runsDir(dagID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []*DagRun
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		var r DagRun
		if err := readJSON(filepath.Join(s.runsDir(dagID), e.Name()), &r); err == nil {
			out = append(out, &r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LogicalDate.Equal(out[j].LogicalDate) {
			return out[i].QueuedAt.Before(out[j].QueuedAt)
		}
		return out[i].LogicalDate.Before(out[j].LogicalDate)
	})
	return out, nil
}

func (s *Store) GetDagMeta(dagID string) *DagMeta {
	m := &DagMeta{DagID: dagID}
	_ = readJSON(s.metaPath(dagID), m)
	return m
}

func (s *Store) SaveDagMeta(m *DagMeta) error { return writeJSONAtomic(s.metaPath(m.DagID), m) }

// NewDagRun builds a run with one TaskInstance per task in state none.
func NewDagRun(d *dag.DAG, runType, runID string, logical, start, end time.Time, conf map[string]any) *DagRun {
	if runID == "" {
		runID = fmt.Sprintf("%s__%s", runType, logical.UTC().Format(time.RFC3339Nano))
	}
	r := &DagRun{
		DagID: d.ID, RunID: runID, RunType: runType, State: RunQueued,
		LogicalDate: logical, DataIntervalStart: start, DataIntervalEnd: end,
		Conf: conf, QueuedAt: time.Now(),
		Tasks: map[string]*TaskInstance{}, XCom: map[string]map[string]any{},
	}
	for _, t := range d.Tasks() {
		r.Tasks[t.ID] = &TaskInstance{TaskID: t.ID, MapIndex: -1}
	}
	return r
}

// ClearTasks resets the given tasks (and optionally their upstream/downstream)
// to state none and re-queues the run, like `airflow tasks clear`.
func ClearTasks(d *dag.DAG, r *DagRun, taskIDs []string, upstream, downstream, onlyFailed bool) ([]string, error) {
	return ClearTaskInstances(d, r, ClearOptions{TaskIDs: taskIDs, Upstream: upstream, Downstream: downstream, OnlyFailed: onlyFailed, MapIndex: -1})
}

// ClearOptions ~ Airflow's clear dialog. MapIndex >= 0 clears a single mapped
// instance of the (single) task in TaskIDs; related upstream/downstream tasks
// are cleared entirely.
type ClearOptions struct {
	TaskIDs                          []string
	Upstream, Downstream, OnlyFailed bool
	MapIndex                         int
}

func ClearTaskInstances(d *dag.DAG, r *DagRun, o ClearOptions) ([]string, error) {
	taskIDs, upstream, downstream, onlyFailed := o.TaskIDs, o.Upstream, o.Downstream, o.OnlyFailed
	if o.MapIndex >= 0 {
		if len(taskIDs) != 1 {
			return nil, fmt.Errorf("map_index needs exactly one task id")
		}
		t, ok := d.Task(taskIDs[0])
		if !ok {
			return nil, fmt.Errorf("task %s not found in dag %s", taskIDs[0], d.ID)
		}
		if o.MapIndex >= len(r.Mapped[t.ID]) {
			return nil, fmt.Errorf("task %s has no mapped instance %d in this run", t.ID, o.MapIndex)
		}
	}
	if r.State == RunRunning {
		return nil, fmt.Errorf("run %s is still running", r.RunID)
	}
	selected := map[string]bool{}
	var walk func(t *dag.Task, up bool)
	walk = func(t *dag.Task, up bool) {
		if selected[t.ID] {
			return
		}
		selected[t.ID] = true
		next := t.Downstream()
		if up {
			next = t.Upstream()
		}
		for _, n := range next {
			walk(n, up)
		}
	}
	if len(taskIDs) == 0 {
		for _, t := range d.Tasks() {
			selected[t.ID] = true
		}
	}
	for _, id := range taskIDs {
		t, ok := d.Task(id)
		if !ok {
			return nil, fmt.Errorf("task %s not found in dag %s", id, d.ID)
		}
		selected[id] = true
		if upstream {
			delete(selected, id)
			walk(t, true)
		}
		if downstream {
			delete(selected, id)
			walk(t, false)
		}
	}
	var cleared []string
	for _, t := range d.Tasks() {
		if !selected[t.ID] {
			continue
		}
		idx := -1
		if o.MapIndex >= 0 && t.ID == taskIDs[0] {
			idx = o.MapIndex
		}
		if !clearTask(r, t, idx, onlyFailed) {
			continue
		}
		if idx >= 0 {
			cleared = append(cleared, fmt.Sprintf("%s[%d]", t.ID, idx))
		} else {
			cleared = append(cleared, t.ID)
		}
	}
	if len(cleared) > 0 {
		r.State, r.EndDate, r.QueuedAt = RunQueued, nil, time.Now()
	}
	return cleared, nil
}
