package engine

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
}

func NewScheduler(e *Engine) *Scheduler {
	return &Scheduler{Engine: e, Store: e.Store, Tick: time.Second, Log: log.New(os.Stdout, "[scheduler] ", log.LstdFlags)}
}

func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.writeHeartbeat(); err != nil {
		return err
	}
	defer os.Remove(heartbeatPath(s.Store))

	s.Log.Printf("started, pid=%d, dags=%d", os.Getpid(), len(dag.List()))
	s.recoverOrphans()
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
			_ = s.writeHeartbeat()
			if _, err := os.Stat(stopPath(s.Store)); err == nil { // `scheduler stop` (works on every OS)
				_ = os.Remove(stopPath(s.Store))
				s.Log.Printf("stop requested")
				stop()
			}
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
	if s.Reload != nil && now.Sub(s.lastReload) >= s.ReloadEvery {
		s.Reload()
		s.lastReload = now
	}
	for _, d := range dag.List() {
		meta := s.Store.GetDagMeta(d.ID)
		if meta.IsPaused {
			continue
		}
		// Like Airflow: no new scheduled runs while max_active_runs is reached,
		// so slow DAGs don't pile up an endless queue.
		if pending := s.pendingRuns(d.ID); pending < d.MaxActiveRuns {
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
		if s.Store.RunExists(d.ID, runID) {
			continue
		}
		r := NewDagRun(d, RunTypeScheduled, runID, iv.start, iv.start, iv.end, nil)
		if err := s.Store.SaveRun(r); err != nil {
			s.Log.Printf("create run %s/%s: %v", d.ID, runID, err)
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
	if err := s.Store.SaveRun(r); err != nil {
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
	runs, _ := s.Store.ListRuns(dagID)
	n := 0
	for _, r := range runs {
		if r.State == RunQueued || r.State == RunRunning {
			n++
		}
	}
	return n
}

func (s *Scheduler) startQueuedRuns(ctx context.Context, d *dag.DAG) {
	runs, err := s.Store.ListRuns(d.ID)
	if err != nil {
		return
	}
	for _, r := range runs {
		if r.State != RunQueued || s.Engine.IsActive(d.ID, r.RunID) {
			continue
		}
		if s.Engine.ActiveCount(d.ID) >= d.MaxActiveRuns {
			return
		}
		s.Log.Printf("starting %s/%s", d.ID, r.RunID)
		if _, err := s.Engine.Submit(ctx, r); err != nil {
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

// ---- heartbeat / pid ---------------------------------------------------------

func heartbeatPath(st *Store) string { return filepath.Join(st.Home, "state", "scheduler.pid") }

func (s *Scheduler) writeHeartbeat() error {
	p := heartbeatPath(s.Store)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(fmt.Sprintf("%d %d", os.Getpid(), time.Now().Unix())), 0o644)
}

// SchedulerInfo returns the scheduler pid and whether its heartbeat is fresh.
func SchedulerInfo(st *Store) (pid int, alive bool) {
	b, err := os.ReadFile(heartbeatPath(st))
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return 0, false
	}
	pid, _ = strconv.Atoi(f[0])
	ts, _ := strconv.ParseInt(f[1], 10, 64)
	return pid, time.Since(time.Unix(ts, 0)) < 10*time.Second
}

func schedulerAlive(st *Store) bool { _, ok := SchedulerInfo(st); return ok }
