package engine

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	// JobID: the task-queue job of the current attempt (remote executors).
	JobID int64 `json:"job_id,omitempty"`
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
	ti.State, ti.StartDate, ti.EndDate, ti.Error, ti.JobID = dag.StateNone, nil, nil, "", 0
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

	version int64             // row version when loaded (Postgres optimistic concurrency)
	owned   bool              // executed by this process
	blobs   map[string]string // Postgres: XComs kept outside the run row, slot+key -> checksum of what is stored
	// blobSeen: slot+key -> identity of the value the checksum was computed
	// for, so an unchanged large value is not marshalled again on every save
	blobSeen map[string]valueID
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

// Store is the metadata database (Airflow's metadata DB). The data lives in
// a backend: JSON files under $DAGSFLOW_HOME/state (default, one machine) or
// Postgres (DAGSFLOW_DATABASE_URL: HA schedulers, workers, several webservers).
type Store struct {
	Home string
	// ID identifies this process as owner of runs, leases and pool slots.
	ID string

	b      backend
	compMu sync.Mutex

	// runs this process is executing, and what the last lease renewal said
	leaseMu   sync.Mutex
	held      map[string]time.Time
	renewed   map[string]bool
	renewedAt time.Time

	bgStop chan struct{}
	bgDone chan struct{}
	comp   Component
}

// NewStore opens the JSON-file store under home.
func NewStore(home string) *Store {
	return &Store{Home: home, ID: processID(), b: &jsonBackend{home: home}}
}

// OpenStore opens Postgres when dsn is set, the JSON store otherwise.
func OpenStore(home, dsn string) (*Store, error) {
	if dsn == "" {
		return NewStore(home), nil
	}
	pg, err := openPG(dsn)
	if err != nil {
		return nil, err
	}
	return &Store{Home: home, ID: processID(), b: pg}, nil
}

var randRead = rand.Read

func processID() string {
	host, _ := os.Hostname()
	raw := make([]byte, 3)
	_, _ = rand.Read(raw)
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), hex.EncodeToString(raw))
}

func (s *Store) be() backend {
	if s.b == nil { // zero-value Store{Home: ...}
		s.b = &jsonBackend{home: s.Home}
	}
	return s.b
}

// Backend is "json" or "postgres".
func (s *Store) Backend() string { return s.be().kind() }

// Distributed reports whether several processes/machines can share this store.
func (s *Store) Distributed() bool { return s.Backend() == "postgres" }

// Register records this process in the component table and keeps its
// heartbeat (and the leases of the runs it owns) fresh until Close. A process
// that is both webserver and scheduler (standalone) registers as scheduler.
func (s *Store) Register(kind string, info map[string]any) {
	s.compMu.Lock()
	if s.bgStop != nil {
		if kind == "scheduler" && s.comp.Kind != "scheduler" {
			old := s.comp.ID
			s.comp.Kind, s.comp.Info = kind, info
			if !s.Distributed() {
				s.comp.ID = "scheduler"
			}
			c := s.comp
			s.compMu.Unlock()
			if old != c.ID {
				s.be().removeComponent(old)
			}
			_ = s.be().heartbeat(c)
			return
		}
		s.compMu.Unlock()
		return
	}
	host, _ := os.Hostname()
	now := time.Now()
	s.comp = Component{ID: s.ID, Kind: kind, Host: host, PID: os.Getpid(), Started: now, Heartbeat: now, Info: info}
	if kind == "scheduler" && !s.Distributed() {
		s.comp.ID = "scheduler"
	}
	c := s.comp
	s.bgStop, s.bgDone = make(chan struct{}), make(chan struct{})
	stop, done := s.bgStop, s.bgDone
	s.compMu.Unlock()
	_ = s.be().heartbeat(c)
	go func() {
		defer close(done)
		every := heartbeatInterval
		if !s.Distributed() {
			every = time.Second // the JSON scheduler pid file expects 1s updates
		}
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				s.compMu.Lock()
				s.comp.Heartbeat = time.Now()
				c := s.comp
				s.compMu.Unlock()
				if err := s.be().heartbeat(c); err != nil {
					metricErrors.inc("heartbeat")
				}
				s.renew()
			}
		}
	}()
}

func runKey(dagID, runID string) string { return dagID + "\x1f" + runID }

// renew extends the leases of the runs this process executes.
func (s *Store) renew() {
	s.leaseMu.Lock()
	keys := make([]string, 0, len(s.held))
	for k := range s.held {
		keys = append(keys, k)
	}
	s.leaseMu.Unlock()
	start := time.Now()
	got, err := s.be().renewLeases(s.ID, keys)
	if err != nil {
		metricErrors.inc("lease_renew")
		return
	}
	s.leaseMu.Lock()
	s.renewed, s.renewedAt = got, start
	s.leaseMu.Unlock()
}

// holdRun / releaseRun: the runs this process is executing (their leases
// are renewed; others it may still own expire and get adopted).
func (s *Store) holdRun(dagID, runID string) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if s.held == nil {
		s.held = map[string]time.Time{}
	}
	s.held[runKey(dagID, runID)] = time.Now()
}

func (s *Store) releaseRun(dagID, runID string) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	delete(s.held, runKey(dagID, runID))
}

// OwnsRun reports whether this process can still trust its ownership of a
// run it executes: the last lease renewal included it, and renewals are
// recent enough that the lease cannot have expired (e.g. database
// unreachable). When false the run must stop here (fencing).
func (s *Store) OwnsRun(dagID, runID string) bool {
	if !s.Distributed() {
		return true
	}
	k := runKey(dagID, runID)
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	heldAt := s.held[k]
	if s.renewedAt.After(heldAt) && !s.renewed[k] {
		return false // renewal found it owned by someone else (or gone)
	}
	last := s.renewedAt
	if heldAt.After(last) {
		last = heldAt
	}
	return time.Since(last) < runLease-2*heartbeatInterval
}

// Unregister stops the heartbeat and removes this process from the
// component table (its pool slots and leases are released).
func (s *Store) Unregister() {
	s.compMu.Lock()
	stop, done, id := s.bgStop, s.bgDone, s.comp.ID
	s.bgStop = nil
	s.compMu.Unlock()
	if stop != nil {
		close(stop)
		<-done
		s.be().removeComponent(id)
	}
}

// Close unregisters the process and closes the backend.
func (s *Store) Close() {
	s.Unregister()
	s.be().close()
}

// ensureRegistered: owning runs or pool slots needs a heartbeat (Postgres).
func (s *Store) ensureRegistered() {
	if s.Distributed() {
		s.Register("cli", nil)
	}
}

// Ping checks the metadata database connection.
func (s *Store) Ping() error {
	if p, ok := s.be().(*pgBackend); ok {
		return p.ping()
	}
	_, err := os.Stat(s.Home)
	return err
}

// SchemaStatus returns the applied and expected schema versions (Postgres).
func (s *Store) SchemaStatus() (applied, expected int, err error) {
	if p, ok := s.be().(*pgBackend); ok {
		applied, err = p.schemaVersion()
		return applied, SchemaVersion(), err
	}
	return 0, 0, nil
}

// Components lists registered processes (schedulers, webservers, workers).
func (s *Store) Components() ([]Component, error) { return s.be().components() }

// TryLease acquires or renews a named lease (e.g. scheduler leadership).
func (s *Store) TryLease(name string, ttl time.Duration) bool {
	ok, err := s.be().tryLease(name, s.ID, ttl)
	return ok && err == nil
}

func (s *Store) LogsDir() string { return filepath.Join(s.Home, "logs") }

func mappedKey(taskID string, idx int) string { return fmt.Sprintf("%s#%d", taskID, idx) }

func safeName(runID string) string {
	return strings.NewReplacer(":", "_", "/", "_", "\\", "_").Replace(runID)
}

// SaveRun writes a run. A run owned by this process (being executed here) is
// written unconditionally; any other run only if nobody executes it and it
// did not change since it was loaded, else ErrConflict (Postgres). A new run
// that already exists is an ErrConflict as well.
func (s *Store) SaveRun(r *DagRun) error { return s.be().saveRun(r, s.ID) }

// CreateRun inserts a new run; false when it already exists.
func (s *Store) CreateRun(r *DagRun) (bool, error) {
	if s.RunExists(r.DagID, r.RunID) {
		return false, nil
	}
	err := s.SaveRun(r)
	if errors.Is(err, ErrConflict) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) DeleteRun(dagID, runID string) error { return s.be().deleteRun(dagID, runID) }

// DeleteDagData removes every run, the DAG's metadata and its logs (Airflow's
// "Delete DAG"): the definition itself stays and shows up again empty.
func (s *Store) DeleteDagData(dagID string) error { return s.be().deleteDagData(dagID) }

func (s *Store) RunExists(dagID, runID string) bool { return s.be().runExists(dagID, runID) }

func (s *Store) LoadRun(dagID, runID string) (*DagRun, error) { return s.be().loadRun(dagID, runID) }

// ListRuns returns runs of a DAG ordered by logical date (oldest first).
func (s *Store) ListRuns(dagID string) ([]*DagRun, error) { return s.ListRunsByState(dagID) }

// ListRunsByState: runs in one of the given states (all when none given).
func (s *Store) ListRunsByState(dagID string, states ...string) ([]*DagRun, error) {
	out, err := s.be().listRuns(dagID, states)
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LogicalDate.Equal(out[j].LogicalDate) {
			return out[i].QueuedAt.Before(out[j].QueuedAt)
		}
		return out[i].LogicalDate.Before(out[j].LogicalDate)
	})
	return out, nil
}

// runSummarizer is implemented by backends that can count runs and read the
// latest ones without loading every run (Postgres).
type runSummarizer interface {
	runCounts(dagID string) (map[string]int, error)
	latestRuns(dagID string, n int) ([]*DagRun, error)
}

// ListRunsLite is ListRuns without XComs and rendered fields (Postgres; the
// JSON store returns full runs). For lists that show states and times only:
// never save a run read this way.
func (s *Store) ListRunsLite(dagID string) ([]*DagRun, error) {
	var out []*DagRun
	var err error
	switch b := s.be().(type) {
	case *pgBackend:
		out, err = b.listRunsLite(dagID)
	case *jsonBackend:
		out, err = b.listRunsShared(dagID) // shared, cached: read-only
	default:
		return s.ListRuns(dagID)
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LogicalDate.Equal(out[j].LogicalDate) {
			return out[i].QueuedAt.Before(out[j].QueuedAt)
		}
		return out[i].LogicalDate.Before(out[j].LogicalDate)
	})
	return out, nil
}

// RunFilter selects runs for a page (zero values: no filter).
type RunFilter struct {
	Before  time.Time // logical date at or before
	RunType string
	State   string
}

func (f RunFilter) match(r *DagRun) bool {
	return (f.Before.IsZero() || !r.LogicalDate.After(f.Before)) && (f.RunType == "" || r.RunType == f.RunType) && (f.State == "" || r.State == f.State)
}

// RunPage: the n latest runs matching f (oldest first, without XComs and
// rendered fields), how many match, and the runs per state of the whole DAG.
// Postgres reads only those n rows; this is what the DAG page polls.
func (s *Store) RunPage(dagID string, f RunFilter, n int) (runs []*DagRun, matching int, counts map[string]int, err error) {
	if p, ok := s.be().(*pgBackend); ok {
		return p.runPage(dagID, f, n)
	}
	all, err := s.ListRunsLite(dagID)
	if err != nil {
		return nil, 0, nil, err
	}
	counts = map[string]int{}
	for _, r := range all {
		counts[r.State]++
		if f.match(r) {
			runs = append(runs, r)
		}
	}
	matching = len(runs)
	if n > 0 && len(runs) > n {
		runs = runs[len(runs)-n:]
	}
	return runs, matching, counts, nil
}

// RunSummary returns the number of runs per state and the n latest runs
// (oldest first): what the DAG list shows, without reading every run.
func (s *Store) RunSummary(dagID string, n int) (map[string]int, []*DagRun, error) {
	if rs, ok := s.be().(runSummarizer); ok {
		counts, err := rs.runCounts(dagID)
		if err != nil {
			return nil, nil, err
		}
		latest, err := rs.latestRuns(dagID, n)
		if err != nil {
			return nil, nil, err
		}
		sort.Slice(latest, func(i, j int) bool {
			if latest[i].LogicalDate.Equal(latest[j].LogicalDate) {
				return latest[i].QueuedAt.Before(latest[j].QueuedAt)
			}
			return latest[i].LogicalDate.Before(latest[j].LogicalDate)
		})
		return counts, latest, nil
	}
	runs, err := s.ListRuns(dagID)
	if err != nil {
		return nil, nil, err
	}
	counts := map[string]int{}
	for _, r := range runs {
		counts[r.State]++
	}
	if n > 0 && len(runs) > n {
		runs = runs[len(runs)-n:]
	}
	return counts, runs, nil
}

// ClaimRun atomically moves a queued run to running, owned by this process,
// unless the DAG already has maxActive running runs (across all schedulers).
// Returns nil when the run was not claimed.
func (s *Store) ClaimRun(dagID, runID string, maxActive int) (*DagRun, error) {
	s.ensureRegistered()
	return s.be().claimRun(dagID, runID, s.ID, maxActive)
}

// TakeOwnership marks r running and owned by this process (e.g. `dags test`).
func (s *Store) TakeOwnership(r *DagRun) error {
	if r.owned {
		return nil
	}
	s.ensureRegistered()
	return s.be().takeOwnership(r, s.ID)
}

// RequeueOrphans re-queues running runs whose owner died (lease expired).
func (s *Store) RequeueOrphans() ([]string, error) { return s.be().requeueOrphans() }

// DagIDs lists DAG ids that have runs or metadata.
func (s *Store) DagIDs() ([]string, error) { return s.be().dagIDs() }

func (s *Store) GetDagMeta(dagID string) *DagMeta { return s.be().getDagMeta(dagID) }

func (s *Store) SaveDagMeta(m *DagMeta) error { return s.be().saveDagMeta(m) }

func (s *Store) readTable(name string, v any) error { return s.be().readTable(name, v) }

func (s *Store) updateTable(name string, v any, fn func() error) error {
	return s.be().updateTable(name, v, fn)
}

// SecretKey is the shared webserver session signing key.
func (s *Store) SecretKey() ([]byte, error) { return s.be().secretKey() }

// OpenTaskLog opens (appends to) the log of one attempt. The returned string
// says where it lives (a path, or db://... for Postgres).
func (s *Store) OpenTaskLog(k LogKey) (io.WriteCloser, string, error) { return s.be().openLog(k) }

// ReadTaskLog returns the log of one attempt.
func (s *Store) ReadTaskLog(k LogKey) ([]byte, error) { return s.be().readLog(k) }

// Cleanup deletes finished runs, event logs, jobs and logs older than before
// (Airflow's `db clean`).
func (s *Store) Cleanup(before time.Time) (CleanupStats, error) { return s.be().cleanup(before) }

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
