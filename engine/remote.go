package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"dagsflow-go/dag"
)

// TaskExecutor runs task attempts outside the scheduler process. The
// scheduler keeps supervising the DagRun (trigger rules, retries, callbacks,
// marks); the executor only runs one attempt and returns what it produced.
type TaskExecutor interface {
	Name() string
	// Run executes the attempt described by job and blocks until it ends.
	// Cancelling ctx (timeout, mark, shutdown) must stop the attempt.
	// dispatched is called with the job id as soon as it is queued.
	Run(ctx context.Context, t *dag.Task, job *TaskJob, out io.Writer, dispatched func(id int64)) (*JobResult, error)
	// Resume waits for an attempt that was already dispatched (job id) by a
	// scheduler that stopped: the new owner of the run re-attaches to it.
	Resume(ctx context.Context, t *dag.Task, id int64, out io.Writer) (*JobResult, error)
}

// NewExecutor returns the executor for name: "local" (nil: in-process),
// "worker" (dagsflow-go worker processes) or "kubernetes" (a pod per task).
func NewExecutor(name string, st *Store) (TaskExecutor, error) {
	switch name {
	case "", "local":
		return nil, nil
	case "worker", "celery":
		if !st.Distributed() {
			return nil, fmt.Errorf("executor %q: %w", name, ErrNeedsDatabase)
		}
		return &QueueExecutor{Store: st}, nil
	case "kubernetes", "k8s":
		if !st.Distributed() {
			return nil, fmt.Errorf("executor %q: %w", name, ErrNeedsDatabase)
		}
		return NewKubernetesExecutor(st)
	}
	return nil, fmt.Errorf("unknown executor %q (local, worker, kubernetes)", name)
}

// errDetached: the scheduler stopped following an attempt that keeps running
// on its worker (shutdown or failover); the next owner of the run re-attaches.
var errDetached = errors.New("detached from a running job")

type detachKey struct{}

// detaching reports whether ctx was cancelled because the scheduler lets go
// of the run (rather than to stop the task).
func detaching(ctx context.Context) bool {
	f, _ := ctx.Value(detachKey{}).(func() bool)
	return f != nil && f()
}

// runRemote hands one attempt to the executor and applies its result to the run.
func (rc *runCtx) runRemote(ctx context.Context, k tiKey, t *dag.Task, info dag.RunInfo, out io.Writer) ([]string, error) {
	rc.mu.Lock()
	xcom := make(map[string]map[string]any, len(rc.r.XCom))
	for slot, kv := range rc.r.XCom {
		m := make(map[string]any, len(kv))
		for key, v := range kv {
			m[key] = v
		}
		xcom[slot] = m
	}
	job := &TaskJob{Queue: t.Queue(), Payload: JobPayload{
		DagID: rc.r.DagID, RunID: rc.r.RunID, RunType: rc.r.RunType, LogicalDate: rc.r.LogicalDate,
		DataIntervalStart: rc.r.DataIntervalStart, DataIntervalEnd: rc.r.DataIntervalEnd, Conf: rc.r.Conf,
		TaskID: t.ID, MapIndex: k.idx, Try: info.TryNumber, Item: info.Item,
		XCom: xcom, MappedLen: mappedLens(rc.r),
	}}
	rc.mu.Unlock()

	ctx = context.WithValue(ctx, detachKey{}, rc.detached)
	res, err := rc.e.Executor.Run(ctx, t, job, out, func(id int64) {
		rc.mu.Lock()
		rc.tiLocked(k).JobID = id
		rc.mu.Unlock()
		if err := rc.trySave(); errors.Is(err, ErrConflict) {
			// we no longer own the run: nobody would follow this job
			_ = rc.e.Store.be().cancelJob(id)
		}
	})
	if err != nil {
		return nil, err
	}
	return rc.applyResult(k, t, res)
}

// resumeRemote re-attaches to an attempt dispatched before a failover.
func (rc *runCtx) resumeRemote(ctx context.Context, k tiKey, t *dag.Task, id int64, out io.Writer) ([]string, error) {
	fmt.Fprintf(out, "%s INFO - Scheduler failover: re-attached to job %d\n", time.Now().Format("2006/01/02 15:04:05"), id)
	ctx = context.WithValue(ctx, detachKey{}, rc.detached)
	res, err := rc.e.Executor.Resume(ctx, t, id, out)
	if err != nil {
		return nil, err
	}
	return rc.applyResult(k, t, res)
}

func (rc *runCtx) applyResult(k tiKey, t *dag.Task, res *JobResult) ([]string, error) {
	for key, v := range res.XCom {
		if err := rc.XComPush(t.ID, k.idx, key, v); err != nil {
			return nil, err
		}
	}
	for f, v := range res.Rendered {
		_ = rc.SetRendered(t.ID, k.idx, f, v)
	}
	for uri, extra := range res.OutletExtra {
		_ = rc.SetOutletExtra(t.ID, k.idx, uri, extra)
	}
	if res.Error != "" {
		if res.SensorTimeout {
			return nil, fmt.Errorf("%w: %s", errSensorTimeout, res.Error)
		}
		return nil, errors.New(res.Error)
	}
	return res.Selected, nil
}

// QueueExecutor (~ CeleryExecutor): attempts go to the task_job table and are
// picked up by `dagsflow-go worker` processes listening on the task's queue.
type QueueExecutor struct {
	Store *Store
	// LostAfter: a running job whose worker stopped heart-beating for this
	// long is failed (the task's retries apply). Default 60s.
	LostAfter time.Duration
	// QueuePrefix keeps jobs away from generic workers (Kubernetes pods
	// claim their job by id).
	QueuePrefix string
	// Hooks used by the Kubernetes executor.
	launch  func(ctx context.Context, t *dag.Task, id int64) (string, error)
	handle  func(t *dag.Task, id int64) (string, error) // handle of an already launched job
	check   func(handle string) error
	cleanup func(handle string, success bool)
}

func (q *QueueExecutor) Name() string { return "worker" }

func (q *QueueExecutor) Run(ctx context.Context, t *dag.Task, job *TaskJob, out io.Writer, dispatched func(int64)) (*JobResult, error) {
	job.Queue = q.QueuePrefix + job.Queue
	id, err := q.Store.be().enqueueJob(job)
	if err != nil {
		return nil, fmt.Errorf("enqueue task: %w", err)
	}
	if dispatched != nil {
		dispatched(id)
	}
	fmt.Fprintf(out, "%s INFO - Queued as job %d on queue %q\n", time.Now().Format("2006/01/02 15:04:05"), id, job.Queue)

	var handle string
	if q.launch != nil {
		if handle, err = q.launch(ctx, t, id); err != nil {
			_ = q.Store.be().cancelJob(id)
			return nil, err
		}
	}
	return q.wait(ctx, id, handle)
}

func (q *QueueExecutor) Resume(ctx context.Context, t *dag.Task, id int64, out io.Writer) (*JobResult, error) {
	handle := ""
	if q.handle != nil {
		h, err := q.handle(t, id)
		if err != nil {
			return nil, err
		}
		handle = h
	}
	return q.wait(ctx, id, handle)
}

// wait follows a job until it ends, is lost, or ctx is cancelled.
func (q *QueueExecutor) wait(ctx context.Context, id int64, handle string) (*JobResult, error) {
	lost := q.LostAfter
	if lost <= 0 {
		lost = 60 * time.Second
	}
	finish := func(success bool) {
		if q.cleanup != nil && handle != "" {
			q.cleanup(handle, success)
		}
	}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	lastCheck := time.Now()
	for {
		select {
		case <-ctx.Done():
			if detaching(ctx) {
				return nil, errDetached // the job keeps running; the next owner re-attaches
			}
			_ = q.Store.be().cancelJob(id)
			q.waitStopped(id, 15*time.Second)
			finish(false)
			return nil, ctx.Err()
		case <-tick.C:
		}
		j, err := q.Store.be().getJob(id)
		if err != nil {
			metricErrors.inc("get_job")
			continue
		}
		switch j.State {
		case JobSuccess, JobFailed:
			finish(j.State == JobSuccess)
			if j.Result == nil {
				return &JobResult{Error: "job finished without a result"}, nil
			}
			return j.Result, nil
		case JobCancelled:
			finish(false)
			return nil, fmt.Errorf("job %d was cancelled", id)
		case JobRunning:
			if j.Heartbeat != nil && time.Since(*j.Heartbeat) > lost {
				_ = q.Store.be().cancelJob(id)
				finish(false)
				return nil, fmt.Errorf("worker %s lost: no heartbeat for %s (job %d)", j.Worker, lost.Round(time.Second), id)
			}
		}
		if q.check != nil && handle != "" && time.Since(lastCheck) > 5*time.Second {
			lastCheck = time.Now()
			if err := q.check(handle); err != nil {
				// the pod ended without reporting (OOMKilled, image pull error, ...)
				if j, e2 := q.Store.be().getJob(id); e2 == nil && (j.State == JobSuccess || j.State == JobFailed) {
					continue // it did report after all: handled on the next tick
				}
				_ = q.Store.be().cancelJob(id)
				finish(false)
				return nil, err
			}
		}
	}
}

// waitStopped gives the worker a moment to acknowledge a cancellation.
func (q *QueueExecutor) waitStopped(id int64, max time.Duration) {
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		j, err := q.Store.be().getJob(id)
		if err != nil || j.State != JobRunning {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
}
