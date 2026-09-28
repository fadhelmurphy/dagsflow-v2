package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"dagsflow-go/dag"
)

// Engine executes DagRuns in this process (~ Airflow LocalExecutor). Executor
// slots (parallelism) and pools are shared by all runs.
type Engine struct {
	Store *Store
	slots *slotManager

	mu     sync.Mutex
	active map[string]chan struct{} // dagID/runID -> closed when done
	wg     sync.WaitGroup
	cbWg   sync.WaitGroup

	poolMu    sync.Mutex
	poolCache map[string]*Pool
	poolAt    time.Time
}

func NewEngine(store *Store, parallelism int) *Engine {
	if parallelism < 1 {
		parallelism = 16
	}
	e := &Engine{Store: store, active: map[string]chan struct{}{}}
	e.slots = newSlotManager(parallelism, e.poolSize)
	return e
}

func (e *Engine) poolSize(name string) int {
	e.poolMu.Lock()
	defer e.poolMu.Unlock()
	if e.poolCache == nil || time.Since(e.poolAt) > 5*time.Second {
		if p, err := e.Store.Pools(); err == nil {
			e.poolCache, e.poolAt = p, time.Now()
		}
	}
	if p, ok := e.poolCache[name]; ok {
		return p.Slots
	}
	return -1
}

func key(dagID, runID string) string { return dagID + "/" + runID }

func (e *Engine) IsActive(dagID, runID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.active[key(dagID, runID)]
	return ok
}

func (e *Engine) ActiveCount(dagID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for k := range e.active {
		if len(k) > len(dagID) && k[:len(dagID)+1] == dagID+"/" {
			n++
		}
	}
	return n
}

// Submit starts a run in the background and returns a channel closed on completion.
func (e *Engine) Submit(ctx context.Context, r *DagRun) (<-chan struct{}, error) {
	k := key(r.DagID, r.RunID)
	e.mu.Lock()
	if _, busy := e.active[k]; busy {
		e.mu.Unlock()
		return nil, fmt.Errorf("run %s already active", k)
	}
	done := make(chan struct{})
	e.active[k] = done
	e.mu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() {
			e.mu.Lock()
			delete(e.active, k)
			e.mu.Unlock()
			close(done)
		}()
		if err := e.Execute(ctx, r); err != nil {
			fmt.Fprintf(os.Stderr, "run %s: %v\n", k, err)
		}
	}()
	return done, nil
}

// Wait blocks until every submitted run and pending callback has returned.
func (e *Engine) Wait() { e.wg.Wait(); e.cbWg.Wait() }

func (e *Engine) callbacks(cbs []dag.Callback, cc dag.CallbackContext) {
	if len(cbs) == 0 {
		return
	}
	e.cbWg.Add(1)
	go func() {
		defer e.cbWg.Done()
		dag.RunCallbacks(cbs, cc, func(f string, a ...any) {
			fmt.Fprintf(os.Stderr, "[%s] %s/%s %s: %s\n", time.Now().Format(time.DateTime), cc.DagID, cc.RunID, cc.TaskID, fmt.Sprintf(f, a...))
		})
	}()
}

// ---- one DagRun -------------------------------------------------------------

type tiKey struct {
	task string
	idx  int // -1 = not mapped (or the aggregate of a mapped task)
}

type runCtx struct {
	e      *Engine
	d      *dag.DAG
	r      *DagRun
	mu     sync.Mutex
	ctx    context.Context
	events chan tiKey
	weight map[string]int

	// manual "mark state" support (see mark.go)
	cancels   map[tiKey]context.CancelFunc // live task instances
	overrides map[tiKey]dag.State          // state to finish a cancelled TI with
	runMark   dag.State                    // run marked success/failed while executing
}

func (rc *runCtx) save() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if err := rc.e.Store.SaveRun(rc.r); err != nil {
		fmt.Fprintf(os.Stderr, "save run %s: %v\n", rc.r.RunID, err)
	}
}

func (rc *runCtx) tiLocked(k tiKey) *TaskInstance {
	if k.idx < 0 {
		return rc.r.Tasks[k.task]
	}
	return rc.r.Mapped[k.task][k.idx]
}

func (rc *runCtx) state(k tiKey) dag.State {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.tiLocked(k).State
}

func (rc *runCtx) setState(k tiKey, s dag.State, errMsg string) {
	rc.mu.Lock()
	ti := rc.tiLocked(k)
	now := time.Now()
	ti.State = s
	switch {
	case s == dag.StateRunning:
		if ti.StartDate == nil { // new attempt (a rescheduled sensor keeps its start)
			ti.StartDate = &now
		}
		ti.EndDate, ti.Error = nil, ""
	case s.Finished():
		ti.EndDate = &now
	case s == dag.StateNone:
		ti.StartDate, ti.EndDate = nil, nil
	}
	if errMsg != "" {
		ti.Error = errMsg
	}
	rc.mu.Unlock()
	rc.save()
}

// ---- dag.RunState implementation ------------------------------------------------

func (rc *runCtx) XComPush(taskID string, idx int, k string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("xcom value for %q is not JSON serialisable: %w", k, err)
	}
	var norm any
	_ = json.Unmarshal(b, &norm)
	slot := taskID
	if idx >= 0 {
		slot = mappedKey(taskID, idx)
	}
	rc.mu.Lock()
	if rc.r.XCom == nil {
		rc.r.XCom = map[string]map[string]any{}
	}
	if rc.r.XCom[slot] == nil {
		rc.r.XCom[slot] = map[string]any{}
	}
	rc.r.XCom[slot][k] = norm
	rc.mu.Unlock()
	rc.save()
	return nil
}

func (rc *runCtx) XComPull(taskID, k string) (any, bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	pull := func(id string) (any, bool) {
		if kids, mapped := rc.r.Mapped[id]; mapped {
			out := make([]any, len(kids))
			found := false
			for i := range kids {
				if v, ok := rc.r.XCom[mappedKey(id, i)][k]; ok {
					out[i], found = v, true
				}
			}
			return out, found
		}
		v, ok := rc.r.XCom[id][k]
		return v, ok
	}
	if taskID != "" {
		return pull(taskID)
	}
	for _, t := range rc.d.Tasks() {
		if v, ok := pull(t.ID); ok {
			return v, true
		}
	}
	return nil, false
}

func (rc *runCtx) ExternalTaskState(dagID, taskID string, logical time.Time) (dag.State, bool, error) {
	return externalTaskState(rc.e.Store, dagID, taskID, logical)
}

func (rc *runCtx) SetRendered(taskID string, idx int, field, value string) error {
	slot := taskID
	if idx >= 0 {
		slot = mappedKey(taskID, idx)
	}
	rc.mu.Lock()
	if rc.r.Rendered == nil {
		rc.r.Rendered = map[string]map[string]string{}
	}
	if rc.r.Rendered[slot] == nil {
		rc.r.Rendered[slot] = map[string]string{}
	}
	rc.r.Rendered[slot][field] = value
	rc.mu.Unlock()
	rc.save()
	return nil
}

func externalTaskState(s *Store, dagID, taskID string, logical time.Time) (dag.State, bool, error) {
	runs, err := s.ListRuns(dagID)
	if err != nil {
		return "", false, err
	}
	for i := len(runs) - 1; i >= 0; i-- {
		r := runs[i]
		if !r.LogicalDate.Equal(logical) {
			continue
		}
		if taskID == "" {
			switch r.State {
			case RunSuccess:
				return dag.StateSuccess, true, nil
			case RunFailed:
				return dag.StateFailed, true, nil
			}
			return dag.StateRunning, true, nil
		}
		ti, ok := r.Tasks[taskID]
		if !ok {
			return "", false, fmt.Errorf("task %s not in dag %s", taskID, dagID)
		}
		return ti.State, true, nil
	}
	return "", false, nil
}

func (rc *runCtx) TriggerDagRun(ctx context.Context, dagID string, conf map[string]any, wait bool) (string, error) {
	target, ok := dag.Get(dagID)
	if !ok {
		return "", fmt.Errorf("dag %s not found", dagID)
	}
	now := time.Now().UTC()
	r := NewDagRun(target, RunTypeManual, "", now, now, now, conf)
	if err := rc.e.Store.SaveRun(r); err != nil {
		return "", err
	}
	if !wait {
		// Leave it queued: the scheduler picks it up (respects max_active_runs/pause).
		// In `dags test` mode there's no scheduler, so run it here.
		if schedulerAlive(rc.e.Store) {
			return r.RunID, nil
		}
		_, err := rc.e.Submit(context.WithoutCancel(ctx), r)
		return r.RunID, err
	}
	done, err := rc.e.Submit(ctx, r)
	if err != nil {
		return r.RunID, err
	}
	select {
	case <-done:
	case <-ctx.Done():
		return r.RunID, ctx.Err()
	}
	final, err := rc.e.Store.LoadRun(dagID, r.RunID)
	if err != nil {
		return r.RunID, err
	}
	if final.State != RunSuccess {
		return r.RunID, fmt.Errorf("triggered dag %s run %s ended with state %s", dagID, r.RunID, final.State)
	}
	return r.RunID, nil
}

// ---- Execute ---------------------------------------------------------------------

// Execute runs (or resumes) a DagRun until every task instance is finished.
// Finished task instances are kept, so clearing a task and executing the run
// again only reruns what was cleared.
func (e *Engine) Execute(ctx context.Context, r *DagRun) error {
	d, ok := dag.Get(r.DagID)
	if !ok {
		return fmt.Errorf("dag %s not found", r.DagID)
	}
	rc := &runCtx{e: e, d: d, r: r, ctx: ctx, events: make(chan tiKey), weight: downstreamWeights(d),
		cancels: map[tiKey]context.CancelFunc{}, overrides: map[tiKey]dag.State{}}
	defer e.Store.DropCommands(r.DagID, r.RunID)

	now := time.Now()
	rc.mu.Lock()
	for _, t := range d.Tasks() { // tasks added to the DAG after the run was created
		if r.Tasks[t.ID] == nil {
			r.Tasks[t.ID] = &TaskInstance{TaskID: t.ID, MapIndex: -1}
		}
	}
	r.State, r.EndDate = RunRunning, nil
	if r.StartDate == nil {
		r.StartDate = &now
	}
	for id, ti := range r.Tasks { // interrupted attempts from a previous process
		if !ti.State.Finished() {
			ti.State = dag.StateNone
			for _, c := range r.Mapped[id] {
				if !c.State.Finished() {
					c.State = dag.StateNone
				}
			}
		}
	}
	rc.mu.Unlock()
	rc.save()

	stopSLA := rc.watchSLAs()
	defer stopSLA()

	running := 0
	launch := func(k tiKey, item any) {
		rc.setState(k, dag.StateQueued, "")
		running++
		go func() { rc.runTask(k, item); rc.events <- k }()
	}

	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	for {
		for changed := true; changed && ctx.Err() == nil && rc.runMark == ""; {
			changed = false
			for _, t := range d.Tasks() {
				k := tiKey{t.ID, -1}
				if rc.state(k) != dag.StateNone {
					continue
				}
				up := make([]dag.State, 0, len(t.Upstream()))
				for _, u := range t.Upstream() {
					up = append(up, rc.state(tiKey{u.ID, -1}))
				}
				decision := t.TriggerRule().Evaluate(up)
				if decision == dag.Wait {
					continue
				}
				changed = true
				switch decision {
				case dag.Run:
					if t.MapSource() == nil {
						launch(k, nil)
						continue
					}
					items, err := rc.expand(t)
					switch {
					case err != nil:
						rc.failBeforeStart(k, fmt.Errorf("cannot expand mapped task: %w", err))
					case len(items) == 0:
						rc.setState(k, dag.StateSkipped, "")
					default:
						rc.setState(k, dag.StateRunning, "")
						launched := 0
						for i, it := range items {
							if ck := (tiKey{t.ID, i}); rc.state(ck) == dag.StateNone {
								launch(ck, it)
								launched++
							}
						}
						if launched == 0 { // resumed with every instance already finished
							rc.aggregateMapped(t.ID)
						}
					}
				case dag.Skip:
					rc.setState(k, dag.StateSkipped, "")
				case dag.MarkUpstreamFailed:
					rc.setState(k, dag.StateUpstreamFailed, "")
				}
			}
		}
		if running == 0 {
			break
		}
		select {
		case k := <-rc.events:
			running--
			if k.idx >= 0 {
				rc.aggregateMapped(k.task)
			}
		case <-poll.C:
			for _, m := range e.Store.TakeMarks(r.DagID, r.RunID) {
				rc.applyMarkLive(m)
			}
		}
	}

	// Final state: like Airflow, the run fails if any leaf task failed/upstream_failed.
	end := time.Now()
	rc.mu.Lock()
	interrupted := ctx.Err() != nil && !allFinished(r)
	if interrupted {
		r.State = RunQueued // scheduler shutdown: resume on next start
	} else {
		r.State = finalRunState(d, r)
		if rc.runMark != "" {
			r.State = string(rc.runMark)
		}
		r.EndDate = &end
	}
	cc := dag.CallbackContext{DagID: r.DagID, RunID: r.RunID, MapIndex: -1, LogicalDate: r.LogicalDate, State: dag.State(r.State)}
	rc.mu.Unlock()
	rc.save()

	if !interrupted {
		if r.State == RunSuccess {
			cc.Event = "dagrun_success"
			e.callbacks(d.OnSuccess, cc)
		} else {
			cc.Event, cc.Err = "dagrun_failure", errors.New("one or more leaf tasks failed")
			e.callbacks(d.OnFailure, cc)
		}
	}
	return nil
}

func allFinished(r *DagRun) bool {
	for _, ti := range r.Tasks {
		if !ti.State.Finished() {
			return false
		}
	}
	return true
}

// downstreamWeights ~ Airflow weight_rule="downstream": a task's effective
// priority is its own weight plus the weights of all its descendants, so long
// chains get started first.
func downstreamWeights(d *dag.DAG) map[string]int {
	out := map[string]int{}
	for _, t := range d.Tasks() {
		seen := map[string]bool{}
		sum := 0
		var walk func(x *dag.Task)
		walk = func(x *dag.Task) {
			if seen[x.ID] {
				return
			}
			seen[x.ID] = true
			sum += x.PriorityWeight()
			for _, c := range x.Downstream() {
				walk(c)
			}
		}
		walk(t)
		out[t.ID] = sum
	}
	return out
}

// ---- dynamic task mapping ----------------------------------------------------------

func (rc *runCtx) resolveItems(t *dag.Task) ([]any, error) {
	src := t.MapSource()
	if !src.IsXCom() {
		return src.Items, nil
	}
	v, ok := rc.XComPull(src.XComTask, src.XComKey)
	if !ok {
		return nil, fmt.Errorf("xcom %s.%s not found", src.XComTask, src.XComKey)
	}
	return toList(v)
}

func (rc *runCtx) expand(t *dag.Task) ([]any, error) {
	items, err := rc.resolveItems(t)
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if kids, ok := rc.r.Mapped[t.ID]; ok && len(kids) > 0 { // resume: keep existing expansion
		if err != nil || len(items) != len(kids) {
			items = make([]any, len(kids))
		}
		return items, nil
	}
	if err != nil {
		return nil, err
	}
	if len(items) > 1024 {
		return nil, fmt.Errorf("%d items exceeds max_map_length (1024)", len(items))
	}
	if rc.r.Mapped == nil {
		rc.r.Mapped = map[string][]*TaskInstance{}
	}
	kids := make([]*TaskInstance, len(items))
	for i := range items {
		kids[i] = &TaskInstance{TaskID: t.ID, MapIndex: i}
	}
	rc.r.Mapped[t.ID] = kids
	return items, nil
}

// toList accepts a JSON array or a string containing one (e.g. bash stdout).
func toList(v any) ([]any, error) {
	switch x := v.(type) {
	case []any:
		return x, nil
	case string:
		var out []any
		if err := json.Unmarshal([]byte(x), &out); err != nil {
			return nil, fmt.Errorf("xcom value is not a list: %q", truncateStr(x, 80))
		}
		return out, nil
	}
	return nil, fmt.Errorf("xcom value is %T, not a list", v)
}

func truncateStr(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func (rc *runCtx) aggregateMapped(taskID string) {
	rc.mu.Lock()
	kids := rc.r.Mapped[taskID]
	var succ, fail, done int
	for _, c := range kids {
		switch c.State {
		case dag.StateSuccess:
			succ++
		case dag.StateFailed, dag.StateUpstreamFailed:
			fail++
		}
		if c.State.Finished() {
			done++
		}
	}
	rc.mu.Unlock()
	if done < len(kids) {
		return
	}
	switch {
	case fail > 0:
		rc.setState(tiKey{taskID, -1}, dag.StateFailed, fmt.Sprintf("%d of %d mapped instances failed", fail, len(kids)))
	case succ > 0:
		rc.setState(tiKey{taskID, -1}, dag.StateSuccess, "")
	default:
		rc.setState(tiKey{taskID, -1}, dag.StateSkipped, "")
	}
}

// ---- SLA ---------------------------------------------------------------------------

// watchSLAs: for scheduled runs, a task with an SLA must succeed by
// data_interval_end + sla (Airflow semantics). Misses are recorded on the run
// and reported to the DAG's sla_miss callbacks.
func (rc *runCtx) watchSLAs() func() {
	if rc.r.RunType != RunTypeScheduled {
		return func() {}
	}
	var timers []*time.Timer
	for _, t := range rc.d.Tasks() {
		if t.SLA() <= 0 {
			continue
		}
		t := t
		deadline := rc.r.DataIntervalEnd.Add(t.SLA())
		timers = append(timers, time.AfterFunc(time.Until(deadline), func() {
			if rc.ctx.Err() != nil {
				return
			}
			rc.mu.Lock()
			st := rc.r.Tasks[t.ID].State
			for _, m := range rc.r.SLAMisses {
				if m.TaskID == t.ID {
					rc.mu.Unlock()
					return
				}
			}
			if st == dag.StateSuccess || st == dag.StateSkipped {
				rc.mu.Unlock()
				return
			}
			rc.r.SLAMisses = append(rc.r.SLAMisses, SLAMiss{TaskID: t.ID, Deadline: deadline, Detected: time.Now()})
			rc.mu.Unlock()
			rc.save()
			rc.e.callbacks(rc.d.OnSLAMiss, dag.CallbackContext{Event: "sla_miss", DagID: rc.d.ID, RunID: rc.r.RunID,
				TaskID: t.ID, MapIndex: -1, State: st, LogicalDate: rc.r.LogicalDate,
				Err: fmt.Errorf("task %s not done by %s (sla %s)", t.ID, deadline.Format(time.RFC3339), t.SLA())})
		}))
	}
	return func() {
		for _, tm := range timers {
			tm.Stop()
		}
	}
}

// ---- one task instance --------------------------------------------------------------

var errSensorTimeout = errors.New("sensor timed out")

// runTask executes one task instance: slots/pools, retries, timeout, sensors,
// callbacks, dataset events and panic recovery.
func (rc *runCtx) runTask(k tiKey, item any) {
	e := rc.e
	t, _ := rc.d.Task(k.task)
	cc := func(event string, st dag.State, err error, try int, logPath string) dag.CallbackContext {
		return dag.CallbackContext{Event: event, DagID: rc.d.ID, RunID: rc.r.RunID, TaskID: t.ID, MapIndex: k.idx,
			State: st, TryNumber: try, LogicalDate: rc.r.LogicalDate, Err: err, LogPath: logPath}
	}
	prio := rc.weight[t.ID]

	// per task-instance context: cancelled on scheduler shutdown or when the
	// instance is marked success/failed manually
	base, stop := context.WithCancel(rc.ctx)
	rc.mu.Lock()
	rc.cancels[k] = stop
	rc.mu.Unlock()
	defer func() {
		rc.mu.Lock()
		delete(rc.cancels, k)
		rc.mu.Unlock()
		stop()
	}()
	marked := func() bool { // finish with the manually requested state
		rc.mu.Lock()
		st, ok := rc.overrides[k]
		delete(rc.overrides, k)
		rc.mu.Unlock()
		if ok {
			rc.setState(k, st, fmt.Sprintf("marked %s manually", st))
		}
		return ok
	}

	for {
		if err := e.slots.acquire(base, t.Pool(), t.PoolSlots(), prio); err != nil {
			if marked() {
				return
			}
			if rc.ctx.Err() != nil {
				rc.setState(k, dag.StateNone, "")
			} else {
				try, logPath := rc.failBeforeStart(k, err)
				e.callbacks(t.OnFailureCallbacks(), cc("task_failure", dag.StateFailed, err, try, logPath))
			}
			return
		}
		held := true
		release := func() {
			if held {
				e.slots.release(t.Pool(), t.PoolSlots())
				held = false
			}
		}
		reacquire := func() error {
			if err := e.slots.acquire(base, t.Pool(), t.PoolSlots(), prio); err != nil {
				return err
			}
			held = true
			return nil
		}

		rc.mu.Lock()
		ti := rc.tiLocked(k)
		ti.TryNumber++
		ti.Pool = t.Pool()
		try := ti.TryNumber
		info := dag.RunInfo{
			RunID: rc.r.RunID, RunType: rc.r.RunType, LogicalDate: rc.r.LogicalDate,
			DataIntervalStart: rc.r.DataIntervalStart, DataIntervalEnd: rc.r.DataIntervalEnd,
			Conf: rc.r.Conf, TryNumber: try, MapIndex: k.idx, Item: item,
		}
		ti.StartDate = nil // new attempt
		rc.mu.Unlock()
		rc.setState(k, dag.StateRunning, "")

		logF, logPath := openTaskLog(e.Store, rc.r.DagID, rc.r.RunID, t.ID, k.idx, try)
		var out io.Writer = io.Discard
		if logF != nil {
			out = logF
		}
		fmt.Fprintf(out, "=== %s.%s run_id=%s map_index=%d try=%d/%d pool=%s priority=%d ===\n",
			rc.r.DagID, t.ID, rc.r.RunID, k.idx, try, t.Retries()+1, t.Pool(), prio)

		tctx, cancel := context.WithCancel(base)
		if t.Timeout() > 0 {
			tctx, cancel = context.WithTimeout(base, t.Timeout())
		}
		taskCtx := dag.NewTaskContext(tctx, t, info, rc, out)

		var selected []string
		var err error
		if sn := t.Sensor(); sn != nil {
			err = rc.sense(taskCtx, k, sn, out, release, reacquire)
		} else {
			selected, err = safeRun(taskCtx, t)
		}
		cancel()
		release()

		if base.Err() != nil && rc.ctx.Err() == nil {
			fmt.Fprintf(out, "Task instance was marked manually; stopping\n")
			closeLog(logF)
			if marked() {
				return
			}
		}
		if err == nil {
			if t.IsBranch() {
				fmt.Fprintf(out, "Branch selected: %v\n", selected)
				keep := map[string]bool{}
				for _, id := range selected {
					keep[id] = true
				}
				for _, dn := range t.Downstream() {
					if !keep[dn.ID] && rc.state(tiKey{dn.ID, -1}) == dag.StateNone {
						rc.setState(tiKey{dn.ID, -1}, dag.StateSkipped, "")
					}
				}
				_ = rc.XComPush(t.ID, k.idx, "branch", selected)
			}
			for _, uri := range t.Outlets() {
				if err := e.Store.AddDatasetEvent(uri, DatasetEvent{Timestamp: time.Now(), SourceDag: rc.d.ID,
					SourceTask: t.ID, SourceRunID: rc.r.RunID}); err != nil {
					fmt.Fprintf(out, "WARN: dataset event %s: %v\n", uri, err)
				} else {
					fmt.Fprintf(out, "Dataset updated: %s\n", uri)
				}
			}
			fmt.Fprintf(out, "Task succeeded\n")
			closeLog(logF)
			rc.setState(k, dag.StateSuccess, "")
			e.callbacks(t.OnSuccessCallbacks(), cc("task_success", dag.StateSuccess, nil, try, logPath))
			return
		}

		fmt.Fprintf(out, "Task failed: %v\n", err)
		closeLog(logF)

		if rc.ctx.Err() != nil { // scheduler shutting down: not the task's fault
			rc.mu.Lock()
			rc.tiLocked(k).TryNumber--
			rc.mu.Unlock()
			rc.setState(k, dag.StateNone, "")
			return
		}
		if errors.Is(err, errSensorTimeout) && t.Sensor().SoftFail {
			rc.setState(k, dag.StateSkipped, err.Error())
			return
		}
		if try <= t.Retries() {
			rc.setState(k, dag.StateUpForRetry, err.Error())
			e.callbacks(t.OnRetryCallbacks(), cc("task_retry", dag.StateUpForRetry, err, try, logPath))
			select {
			case <-time.After(t.RetryDelay()):
				rc.setState(k, dag.StateQueued, "")
				continue
			case <-base.Done():
				if marked() {
					return
				}
				rc.setState(k, dag.StateNone, "")
				return
			}
		}
		rc.setState(k, dag.StateFailed, fmt.Sprintf("%v (log: %s)", err, logPath))
		e.callbacks(t.OnFailureCallbacks(), cc("task_failure", dag.StateFailed, err, try, logPath))
		return
	}
}

// sense runs a sensor's poke loop. In reschedule mode the executor/pool slot
// is released between pokes (state up_for_reschedule), like a deferred task.
func (rc *runCtx) sense(ctx *dag.TaskContext, k tiKey, sn *dag.SensorSpec, out io.Writer, release func(), reacquire func() error) error {
	deadline := time.Now().Add(sn.Timeout)
	for {
		ok, err := safePoke(ctx, sn)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().Add(sn.PokeInterval).After(deadline) {
			return fmt.Errorf("%w after %s", errSensorTimeout, sn.Timeout)
		}
		if sn.Mode == dag.ModeReschedule {
			release()
			rc.setState(k, dag.StateUpForResched, "")
			fmt.Fprintf(out, "Rescheduling: next poke in %s\n", sn.PokeInterval)
		}
		select {
		case <-time.After(sn.PokeInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
		if sn.Mode == dag.ModeReschedule {
			rc.setState(k, dag.StateQueued, "")
			if err := reacquire(); err != nil {
				return err
			}
			rc.setState(k, dag.StateRunning, "")
		}
	}
}

func safePoke(ctx *dag.TaskContext, sn *dag.SensorSpec) (ok bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v\n%s", p, debug.Stack())
		}
	}()
	return sn.Poke(ctx)
}

type result struct {
	sel []string
	err error
}

func safeRun(ctx *dag.TaskContext, t *dag.Task) ([]string, error) {
	done := make(chan result, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- result{err: fmt.Errorf("panic: %v\n%s", p, debug.Stack())}
			}
		}()
		sel, err := t.Run(ctx)
		done <- result{sel, err}
	}()
	select {
	case r := <-done:
		return r.sel, r.err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("task timed out after %s", t.Timeout())
		}
		return nil, ctx.Err()
	}
}

// TaskLogPath mirrors Airflow's log layout (map_index dir only for mapped tasks).
func TaskLogPath(s *Store, dagID, runID, taskID string, mapIndex, try int) string {
	parts := []string{s.LogsDir(), "dag_id=" + dagID, "run_id=" + safeName(runID), "task_id=" + taskID}
	if mapIndex >= 0 {
		parts = append(parts, fmt.Sprintf("map_index=%d", mapIndex))
	}
	parts = append(parts, fmt.Sprintf("attempt=%d.log", try))
	return filepath.Join(parts...)
}

func openTaskLog(s *Store, dagID, runID, taskID string, mapIndex, try int) (*os.File, string) {
	p := TaskLogPath(s, dagID, runID, taskID, mapIndex, try)
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, p
	}
	return f, p
}

func closeLog(f *os.File) {
	if f != nil {
		_ = f.Close()
	}
}

// applyMarkLive applies a mark request to a run this engine is executing:
// live task instances are cancelled and finish with the requested state,
// everything else is updated in place; the scheduling loop then continues.
func (rc *runCtx) applyMarkLive(m MarkRequest) {
	if err := m.Validate(rc.d); err != nil {
		fmt.Fprintf(os.Stderr, "mark %s/%s: %v\n", rc.r.DagID, rc.r.RunID, err)
		return
	}
	if m.Kind == "note" {
		rc.mu.Lock()
		err := SetNote(rc.d, rc.r, m)
		rc.mu.Unlock()
		if err != nil {
			fmt.Fprintf(os.Stderr, "note %s/%s: %v\n", rc.r.DagID, rc.r.RunID, err)
		}
		rc.save()
		return
	}
	rc.mu.Lock()
	var cancel []context.CancelFunc
	set := func(k tiKey, st dag.State) {
		if c, live := rc.cancels[k]; live {
			rc.overrides[k] = st
			cancel = append(cancel, c)
			return
		}
		if ti := rc.tiLocked(k); ti != nil {
			markTI(ti, st, m.User)
		}
	}
	if m.Target == "run" {
		rc.runMark = m.State
		for _, t := range rc.d.Tasks() {
			keys := []tiKey{{t.ID, -1}}
			for i := range rc.r.Mapped[t.ID] {
				keys = append(keys, tiKey{t.ID, i})
			}
			for _, k := range keys {
				st := rc.tiLocked(k).State
				switch {
				case m.State == dag.StateSuccess && st != dag.StateSuccess && st != dag.StateSkipped:
					set(k, dag.StateSuccess)
				case m.State == dag.StateFailed && st == dag.StateNone:
					set(k, dag.StateSkipped)
				case m.State == dag.StateFailed && !st.Finished():
					set(k, dag.StateFailed)
				}
			}
		}
	} else {
		sel := markTargets(rc.d, m)
		for id := range sel {
			kids := rc.r.Mapped[id]
			if id == m.TaskID && m.MapIndex >= 0 {
				if m.MapIndex < len(kids) {
					set(tiKey{id, m.MapIndex}, m.State)
				}
				continue
			}
			for i := range kids {
				set(tiKey{id, i}, m.State)
			}
			if len(kids) == 0 || rc.tiLocked(tiKey{id, -1}).State.Finished() {
				set(tiKey{id, -1}, m.State)
			}
		}
		for _, id := range descendantsOf(rc.d, sel) {
			if s := rc.r.Tasks[id].State; s == dag.StateFailed || s == dag.StateUpstreamFailed {
				resetTI(rc.r, id)
			}
		}
	}
	rc.mu.Unlock()
	rc.save()
	for _, c := range cancel {
		c()
	}
	if m.Target == "task" && m.MapIndex >= 0 {
		rc.aggregateMapped(m.TaskID)
	}
}

// failBeforeStart fails a task instance that could not even start (e.g. its
// pool does not exist, or a mapped task cannot be expanded). It still records
// an attempt with a log file, so the reason shows up in the task's Logs like
// any other failure. Retries are not used: these are configuration errors.
func (rc *runCtx) failBeforeStart(k tiKey, cause error) (int, string) {
	t, _ := rc.d.Task(k.task)
	rc.mu.Lock()
	ti := rc.tiLocked(k)
	ti.TryNumber++
	ti.Pool = t.Pool()
	try := ti.TryNumber
	rc.mu.Unlock()
	f, logPath := openTaskLog(rc.e.Store, rc.r.DagID, rc.r.RunID, t.ID, k.idx, try)
	if f != nil {
		fmt.Fprintf(f, "=== %s.%s run_id=%s map_index=%d try=%d/%d pool=%s ===\n",
			rc.r.DagID, t.ID, rc.r.RunID, k.idx, try, t.Retries()+1, t.Pool())
		fmt.Fprintf(f, "%s ERROR - Task could not start: %v\n", time.Now().Format("2006/01/02 15:04:05"), cause)
		fmt.Fprintf(f, "Task failed before running (configuration error); retries are not used.\n")
		closeLog(f)
	}
	rc.setState(k, dag.StateFailed, fmt.Sprintf("%v (log: %s)", cause, logPath))
	return try, logPath
}
