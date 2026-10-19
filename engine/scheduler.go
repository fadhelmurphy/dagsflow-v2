package engine

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"dagsflow-go/dag"
)

// Scheduler ~ `airflow scheduler`: a single long-running process for ALL DAGs.
// Each loop it (1) creates DagRuns for data intervals that have ended and
// (2) starts queued runs (scheduled, manual triggers, cleared runs) while
// respecting max_active_runs and pause state.
type Scheduler struct {
	Engine *Engine
	Store  *Store
	Tick   time.Duration
	Log    *log.Logger
	// Reload re-scans the DAG folder (YAML DAGs) every ReloadEvery.
	Reload      func()
	ReloadEvery time.Duration
	lastReload  time.Time
	// RetentionDays > 0: the leader deletes finished runs, logs, event logs
	// and jobs older than this, once an hour (~ `airflow db clean`).
	RetentionDays int

	leader      bool
	lastOrphans time.Time
	lastCleanup time.Time
}

func NewScheduler(e *Engine) *Scheduler {
	return &Scheduler{Engine: e, Store: e.Store, Tick: time.Second, Log: log.New(os.Stdout, "[scheduler] ", log.LstdFlags)}
}

func (s *Scheduler) Run(ctx context.Context) error {
	if !s.Store.Distributed() {
		// JSON files take one active scheduler at a time: the others wait as
		// hot standbys on the lock and take over (adopting the runs of the
		// previous one) as soon as it stops or dies.
		unlock, err := s.waitForLock(ctx)
		if err != nil {
			return err
		}
		if unlock == nil { // stopped while waiting
			return nil
		}
		defer unlock()
	}
	exec := "local"
	if s.Engine.Executor != nil {
		exec = s.Engine.Executor.Name()
	}
	s.Store.Register("scheduler", map[string]any{"executor": exec, "parallelism": s.Engine.slots.parallelism})
	defer s.Store.Unregister()

	s.Log.Printf("started, id=%s, store=%s, executor=%s, dags=%d", s.Store.ID, s.Store.Backend(), exec, len(dag.List()))
	if !s.Store.Distributed() {
		s.recoverOrphans()
	}
	for id, err := range dag.ImportErrors() {
		s.Log.Printf("IMPORT ERROR %s: %v", id, err)
	}

	ctx, stop := context.WithCancel(ctx)
	defer stop()
	_ = os.Remove(stopPath(s.Store))
	t := time.NewTicker(s.Tick)
	defer t.Stop()
	for {
		s.loop(ctx, time.Now())
		select {
		case <-ctx.Done():
			s.Log.Printf("shutting down, waiting for running tasks to stop...")
			s.Engine.Wait()
			return nil
		case <-t.C:
			if _, err := os.Stat(stopPath(s.Store)); err == nil { // `scheduler stop` (works on every OS)
				_ = os.Remove(stopPath(s.Store))
				s.Log.Printf("stop requested")
				stop()
			}
		}
	}
}

// StandbyPoll is how often a standby scheduler (JSON store) retries the lock.
var StandbyPoll = 2 * time.Second

// waitForLock takes the scheduler lock of the JSON store, waiting as a standby
// while another scheduler holds it. It returns (nil, nil) if ctx ends first.
func (s *Scheduler) waitForLock(ctx context.Context) (func(), error) {
	path := filepath.Join(s.Store.Home, "state", "scheduler.lock")
	announced := false
	for {
		unlock, err := lockFile(path)
		if err == nil {
			if announced {
				s.Log.Printf("standby: lock acquired, taking over as the active scheduler")
			}
			return unlock, nil
		}
		if !announced {
			who := "another scheduler is active"
			if pid, _ := SchedulerInfo(s.Store); pid > 0 {
				who = fmt.Sprintf("another scheduler is active (pid %d)", pid)
			}
			s.Log.Printf("standby: %s; waiting to take over", who)
			announced = true
		}
		select {
		case <-ctx.Done():
			return nil, nil
		case <-time.After(StandbyPoll):
		}
	}
}

func stopPath(st *Store) string { return filepath.Join(st.Home, "state", "scheduler.stop") }

// RequestStop asks a running scheduler to shut down gracefully: running task
// instances are stopped and resume on the next start.
func RequestStop(st *Store) error {
	return os.WriteFile(stopPath(st), []byte(time.Now().Format(time.RFC3339)), 0o644)
}

func (s *Scheduler) loop(ctx context.Context, now time.Time) {
	defer func() { metricLoop.observe(time.Since(now).Seconds()) }()
	if s.Reload != nil && now.Sub(s.lastReload) >= s.ReloadEvery {
		s.Reload()
		s.lastReload = now
	}
	// Leader election: one scheduler creates runs and adopts orphans; every
	// scheduler executes queued runs (claimed atomically).
	leader := s.Store.TryLease("scheduler_leader", 15*time.Second)
	if leader != s.leader {
		s.leader = leader
		if s.Store.Distributed() {
			s.Log.Printf("leadership: %v", leader)
		}
	}
	metricLeader.set(map[bool]float64{true: 1, false: 0}[leader])
	if leader && s.Store.Distributed() && now.Sub(s.lastOrphans) >= 5*time.Second {
		s.lastOrphans = now
		if ids, err := s.Store.RequeueOrphans(); err != nil {
			metricErrors.inc("requeue_orphans")
		} else {
			for _, id := range ids {
				metricOrphans.inc()
				s.Log.Printf("re-queued orphaned run %s (its scheduler stopped)", id)
			}
		}
	}
	if leader && s.RetentionDays > 0 && now.Sub(s.lastCleanup) >= time.Hour {
		s.lastCleanup = now
		before := now.AddDate(0, 0, -s.RetentionDays)
		if st, err := s.Store.Cleanup(before); err != nil {
			s.Log.Printf("retention cleanup: %v", err)
		} else if st != (CleanupStats{}) {
			s.Log.Printf("retention cleanup (older than %d days): %+v", s.RetentionDays, st)
		}
	}
	for _, d := range dag.List() {
		meta := s.Store.GetDagMeta(d.ID)
		if meta.IsPaused {
			continue
		}
		// Like Airflow: no new scheduled runs while max_active_runs is reached,
		// so slow DAGs don't pile up an endless queue.
		if pending := s.pendingRuns(d.ID); leader && pending < d.MaxActiveRuns {
			if d.IsDatasetScheduled() {
				s.createDatasetRun(d, meta, now)
			} else if !d.IsManualOnly() {
				s.createScheduledRuns(d, meta, now)
			}
		}
		s.startQueuedRuns(ctx, d)
	}
}

// createScheduledRuns: a run for interval [start, end) is created once `end`
// has passed. logical_date = start (Airflow semantics).
func (s *Scheduler) createScheduledRuns(d *dag.DAG, meta *DagMeta, now time.Time) {
	sch := d.CronSchedule()
	var start time.Time
	if meta.NextDataIntervalStart != nil {
		start = *meta.NextDataIntervalStart
	} else {
		base := d.StartDate
		if base.IsZero() || !d.Catchup {
			// no catchup: only the most recent complete interval
			first := sch.Next(now)
			step := sch.Next(first).Sub(first)
			lookback := now.Add(-2 * step)
			if base.IsZero() || base.Before(lookback) {
				base = lookback
			}
		}
		start = sch.Next(base.Add(-time.Nanosecond))
	}

	type interval struct{ start, end time.Time }
	var due []interval
	for i := 0; i < 1000; i++ {
		end := sch.Next(start)
		if end.After(now) {
			break
		}
		due = append(due, interval{start, end})
		start = end
	}
	if len(due) == 0 {
		if meta.NextDataIntervalStart == nil {
			meta.NextDataIntervalStart = &start
			_ = s.Store.SaveDagMeta(meta)
		}
		return
	}
	if !d.Catchup && len(due) > 1 {
		due = due[len(due)-1:]
	}
	for _, iv := range due {
		runID := "scheduled__" + iv.start.UTC().Format(time.RFC3339)
		r := NewDagRun(d, RunTypeScheduled, runID, iv.start, iv.start, iv.end, nil)
		created, err := s.Store.CreateRun(r)
		if err != nil {
			s.Log.Printf("create run %s/%s: %v", d.ID, runID, err)
			continue
		}
		if !created {
			continue
		}
		s.Log.Printf("created %s/%s (interval %s → %s)", d.ID, runID, iv.start.Format(time.RFC3339), iv.end.Format(time.RFC3339))
	}
	meta.NextDataIntervalStart = &start
	_ = s.Store.SaveDagMeta(meta)
}

// createDatasetRun ~ Airflow data-aware scheduling: the DAG runs once every
// dataset it depends on has a new event since its last dataset-triggered run.
func (s *Scheduler) createDatasetRun(d *dag.DAG, meta *DagMeta, now time.Time) {
	all, err := s.Store.Datasets()
	if err != nil {
		return
	}
	if meta.DatasetConsumed == nil {
		meta.DatasetConsumed = map[string]time.Time{}
	}
	events := map[string]any{}
	for _, uri := range d.Datasets {
		ds := all[uri]
		if ds == nil || !ds.LastUpdated.After(meta.DatasetConsumed[uri]) {
			return
		}
		var fresh []DatasetEvent
		for _, ev := range ds.Events {
			if ev.Timestamp.After(meta.DatasetConsumed[uri]) {
				fresh = append(fresh, ev)
			}
		}
		events[uri] = fresh
	}
	r := NewDagRun(d, RunTypeDataset, "", now.UTC(), now.UTC(), now.UTC(), map[string]any{"triggering_dataset_events": events})
	if _, err := s.Store.CreateRun(r); err != nil {
		s.Log.Printf("create dataset run %s: %v", d.ID, err)
		return
	}
	for _, uri := range d.Datasets {
		meta.DatasetConsumed[uri] = all[uri].LastUpdated
	}
	_ = s.Store.SaveDagMeta(meta)
	s.Log.Printf("created %s/%s (datasets updated: %v)", d.ID, r.RunID, d.Datasets)
}

func (s *Scheduler) pendingRuns(dagID string) int {
	runs, _ := s.Store.ListRunsByState(dagID, RunQueued, RunRunning)
	return len(runs)
}

func (s *Scheduler) startQueuedRuns(ctx context.Context, d *dag.DAG) {
	runs, err := s.Store.ListRunsByState(d.ID, RunQueued)
	if err != nil {
		metricErrors.inc("list_runs")
		return
	}
	for _, r := range runs {
		if s.Engine.IsActive(d.ID, r.RunID) {
			continue
		}
		if s.Engine.ActiveCount(d.ID) >= d.MaxActiveRuns {
			return
		}
		// atomic queued -> running (max_active_runs checked across schedulers)
		claimed, err := s.Store.ClaimRun(d.ID, r.RunID, d.MaxActiveRuns)
		if err != nil {
			metricErrors.inc("claim_run")
			s.Log.Printf("claim %s/%s: %v", d.ID, r.RunID, err)
			continue
		}
		if claimed == nil {
			continue // another scheduler took it, or max_active_runs reached
		}
		s.Log.Printf("starting %s/%s", d.ID, r.RunID)
		if _, err := s.Engine.Submit(ctx, claimed); err != nil {
			s.Log.Printf("submit %s/%s: %v", d.ID, r.RunID, err)
		}
	}
}

// recoverOrphans re-queues runs left "running" by a crashed/killed scheduler, so
// they resume (finished tasks are kept, interrupted ones rerun).
func (s *Scheduler) recoverOrphans() {
	for _, d := range dag.List() {
		runs, _ := s.Store.ListRuns(d.ID)
		for _, r := range runs {
			if r.State == RunRunning {
				r.State = RunQueued
				_ = s.Store.SaveRun(r)
				s.Log.Printf("re-queued orphaned run %s/%s", d.ID, r.RunID)
			}
		}
	}
}

// ---- liveness ------------------------------------------------------------------

// SchedulerInfo returns the pid of a live scheduler and whether there is one.
func SchedulerInfo(st *Store) (pid int, alive bool) {
	cs, err := st.Components()
	if err != nil {
		return 0, false
	}
	for _, c := range cs {
		if c.Kind == "scheduler" && c.Alive() {
			return c.PID, true
		}
	}
	return 0, false
}

func schedulerAlive(st *Store) bool { _, ok := SchedulerInfo(st); return ok }

// LocalSchedulerInfo is SchedulerInfo restricted to this machine (for
// signalling it: pids are only meaningful locally).
func LocalSchedulerInfo(st *Store) (pid int, alive bool) {
	cs, err := st.Components()
	if err != nil {
		return 0, false
	}
	host, _ := os.Hostname()
	for _, c := range cs {
		if c.Kind == "scheduler" && c.Alive() && (c.Host == "" || c.Host == host) {
			return c.PID, true
		}
	}
	return 0, false
}
