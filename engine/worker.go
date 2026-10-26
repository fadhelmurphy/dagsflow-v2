package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"dagsflow-go/dag"
)

// Worker (~ `airflow celery worker`) executes jobs from the task queue. It
// needs the same DAG folder and connections as the scheduler; everything
// else (run state, XComs, logs) goes through the metadata database.
type Worker struct {
	Store       *Store
	Queues      []string
	Concurrency int
	// Grace: on shutdown, running jobs get this long to finish before they are
	// cancelled (their tasks then fail and retry per their retries).
	Grace time.Duration
	Log   *log.Logger
	// Reload re-scans the DAG folder (new DAG files) every ReloadEvery.
	Reload      func()
	ReloadEvery time.Duration
}

func (w *Worker) logger() *log.Logger {
	if w.Log == nil {
		w.Log = log.New(os.Stdout, "[worker] ", log.LstdFlags)
	}
	return w.Log
}

// Run claims and executes jobs until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	if !w.Store.Distributed() {
		return fmt.Errorf("worker: %w", ErrNeedsDatabase)
	}
	w.Store.InstallCatalogHooks()
	if w.Concurrency < 1 {
		w.Concurrency = 8
	}
	if len(w.Queues) == 0 {
		w.Queues = []string{dag.DefaultQueue}
	}
	w.Store.Register("worker", map[string]any{"queues": w.Queues, "concurrency": w.Concurrency})
	w.logger().Printf("started id=%s queues=%v concurrency=%d dags=%d", w.Store.ID, w.Queues, w.Concurrency, len(dag.List()))

	// jobs keep running until they finish, or until Grace after shutdown
	jobCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()
	sem := make(chan struct{}, w.Concurrency)
	var wg sync.WaitGroup
	var lastReload time.Time
	idle := time.NewTicker(time.Second)
	defer idle.Stop()
	for ctx.Err() == nil {
		if w.Reload != nil && time.Since(lastReload) >= w.ReloadEvery {
			w.Reload()
			lastReload = time.Now()
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			continue
		}
		j, err := w.Store.be().claimJob(w.Queues, w.Store.ID, 0)
		if err != nil || j == nil {
			<-sem
			if err != nil {
				metricErrors.inc("claim_job")
				w.logger().Printf("claim: %v", err)
			}
			select {
			case <-ctx.Done():
			case <-idle.C:
			}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w.execute(jobCtx, j)
		}()
	}
	w.logger().Printf("shutting down: waiting up to %s for running jobs", w.Grace)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.Grace):
		w.logger().Printf("grace period over: cancelling running jobs")
		cancelJobs()
		<-done
	}
	return nil
}

// RunJob executes exactly one job (the Kubernetes executor's pod command).
func (w *Worker) RunJob(ctx context.Context, id int64) error {
	if !w.Store.Distributed() {
		return fmt.Errorf("worker: %w", ErrNeedsDatabase)
	}
	w.Store.InstallCatalogHooks()
	w.Store.Register("worker", map[string]any{"job": id})
	j, err := w.Store.be().claimJob(nil, w.Store.ID, id)
	if err != nil {
		return err
	}
	if j == nil {
		return fmt.Errorf("job %d is not queued (already taken or cancelled)", id)
	}
	w.execute(ctx, j)
	return nil
}

// execute runs one job and reports its result.
func (w *Worker) execute(ctx context.Context, j *TaskJob) {
	p := j.Payload
	metricWorkerBusy.add(1)
	defer metricWorkerBusy.add(-1)
	res := w.run(ctx, j)
	state := JobSuccess
	if res.Error != "" {
		state = JobFailed
	}
	metricJobs.inc(j.Queue, state)
	if err := w.Store.be().finishJob(j.ID, w.Store.ID, res); err != nil {
		metricErrors.inc("finish_job")
		w.logger().Printf("job %d: report result: %v", j.ID, err)
	}
	w.logger().Printf("job %d %s.%s[%d] try %d: %s", j.ID, p.DagID, p.TaskID, p.MapIndex, p.Try, state)
}

func (w *Worker) run(ctx context.Context, j *TaskJob) (res JobResult) {
	p := j.Payload
	if p.Preview != nil { // builder "Run node": nothing is logged or stored
		pctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			tk := time.NewTicker(2 * time.Second)
			defer tk.Stop()
			for {
				select {
				case <-pctx.Done():
					return
				case <-tk.C:
					if stop, err := w.Store.be().jobHeartbeat(j.ID, w.Store.ID); err == nil && stop {
						cancel()
						return
					}
				}
			}
		}()
		pr := RunPreview(pctx, *p.Preview)
		res.Preview = &pr
		return res
	}
	out, _, err := w.Store.OpenTaskLog(LogKey{p.DagID, p.RunID, p.TaskID, p.MapIndex, p.Try})
	if err != nil {
		out = nopCloser{io.Discard}
	}
	defer out.Close()
	host, _ := os.Hostname()
	fmt.Fprintf(out, "%s INFO - Running on worker %s (host %s, job %d)\n", time.Now().Format("2006/01/02 15:04:05"), w.Store.ID, host, j.ID)

	d, ok := dag.Get(p.DagID)
	if !ok {
		res.Error = fmt.Sprintf("dag %s is not loaded on worker %s (is its DAG folder in sync with the scheduler?)", p.DagID, host)
		fmt.Fprintln(out, "ERROR - "+res.Error)
		return res
	}
	t, ok := d.Task(p.TaskID)
	if !ok {
		res.Error = fmt.Sprintf("task %s not in dag %s on worker %s (DAG folder out of sync?)", p.TaskID, p.DagID, host)
		fmt.Fprintln(out, "ERROR - "+res.Error)
		return res
	}

	base, cancel := context.WithCancel(ctx)
	defer cancel()
	// heartbeat + cancellation requests from the scheduler (timeout, mark, ...)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		tk := time.NewTicker(3 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-base.Done():
				return
			case <-tk.C:
				stop, err := w.Store.be().jobHeartbeat(j.ID, w.Store.ID)
				if err != nil {
					metricErrors.inc("job_heartbeat")
					continue
				}
				if stop {
					fmt.Fprintf(out, "%s INFO - Cancellation requested by the scheduler\n", time.Now().Format("2006/01/02 15:04:05"))
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-hbDone }()

	tctx := base
	if t.Timeout() > 0 {
		var c context.CancelFunc
		tctx, c = context.WithTimeout(base, t.Timeout())
		defer c()
	}
	wr := &workerRun{st: w.Store, d: d, p: p, res: &res}
	info := dag.RunInfo{RunID: p.RunID, RunType: p.RunType, LogicalDate: p.LogicalDate,
		DataIntervalStart: p.DataIntervalStart, DataIntervalEnd: p.DataIntervalEnd,
		Conf: p.Conf, TryNumber: p.Try, MapIndex: p.MapIndex, Item: p.Item}
	taskCtx := dag.NewTaskContext(tctx, t, info, wr, out)

	var selected []string
	if sn := t.Sensor(); sn != nil {
		err = pokeLoop(taskCtx, sn, out)
	} else {
		selected, err = safeRun(taskCtx, t)
	}
	switch {
	case err == nil:
		res.Selected = selected
	case errors.Is(err, errSensorTimeout):
		res.Error, res.SensorTimeout = err.Error(), true
	case tctx.Err() != nil && errors.Is(tctx.Err(), context.DeadlineExceeded) && base.Err() == nil:
		res.Error = fmt.Sprintf("task timed out after %s", t.Timeout())
	case ctx.Err() != nil:
		res.Error = "worker shutting down: " + err.Error()
	default:
		res.Error = err.Error()
	}
	if res.Error != "" {
		fmt.Fprintf(out, "%s ERROR - %s\n", time.Now().Format("2006/01/02 15:04:05"), res.Error)
	}
	return res
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

// pokeLoop: sensors on remote executors always poke (the worker keeps the slot).
func pokeLoop(ctx *dag.TaskContext, sn *dag.SensorSpec, out io.Writer) error {
	deadline := time.Now().Add(sn.Timeout)
	for {
		ok, err := safePoke(ctx, ctx.Task)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().Add(sn.PokeInterval).After(deadline) {
			return fmt.Errorf("%w after %s", errSensorTimeout, sn.Timeout)
		}
		select {
		case <-time.After(sn.PokeInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// workerRun is the dag.RunState of a task running in a worker: reads come
// from the job's snapshot, writes are collected into the result.
type workerRun struct {
	st  *Store
	d   *dag.DAG
	p   JobPayload
	mu  sync.Mutex
	res *JobResult
}

func (w *workerRun) XComPush(taskID string, idx int, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("xcom value for %q is not JSON serialisable: %w", key, err)
	}
	var norm any
	_ = json.Unmarshal(b, &norm)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.res.XCom == nil {
		w.res.XCom = map[string]any{}
	}
	w.res.XCom[key] = norm
	slot := taskID
	if idx >= 0 {
		slot = mappedKey(taskID, idx)
	}
	if w.p.XCom == nil {
		w.p.XCom = map[string]map[string]any{}
	}
	if w.p.XCom[slot] == nil {
		w.p.XCom[slot] = map[string]any{}
	}
	w.p.XCom[slot][key] = norm
	return nil
}

func (w *workerRun) XComPull(taskID, key string) (any, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return pullXCom(w.d, w.p.XCom, w.p.MappedLen, taskID, key)
}

func (w *workerRun) TriggerDagRun(ctx context.Context, dagID string, conf map[string]any, wait bool) (string, error) {
	target, ok := dag.Get(dagID)
	if !ok {
		return "", fmt.Errorf("dag %s not found", dagID)
	}
	now := time.Now().UTC()
	r := NewDagRun(target, RunTypeManual, "", now, now, now, conf)
	if err := w.st.SaveRun(r); err != nil {
		return "", err
	}
	if !wait {
		return r.RunID, nil
	}
	for {
		select {
		case <-ctx.Done():
			return r.RunID, ctx.Err()
		case <-time.After(2 * time.Second):
		}
		final, err := w.st.LoadRun(dagID, r.RunID)
		if err != nil {
			return r.RunID, err
		}
		switch final.State {
		case RunSuccess:
			return r.RunID, nil
		case RunFailed:
			return r.RunID, fmt.Errorf("triggered dag %s run %s ended with state %s", dagID, r.RunID, final.State)
		}
	}
}

func (w *workerRun) ExternalTaskState(dagID, taskID string, logical time.Time) (dag.State, bool, error) {
	return externalTaskState(w.st, dagID, taskID, logical)
}

func (w *workerRun) SetRendered(_ string, _ int, field, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.res.Rendered == nil {
		w.res.Rendered = map[string]string{}
	}
	w.res.Rendered[field] = value
	return nil
}

func (w *workerRun) SetOutletExtra(_ string, _ int, uri string, extra map[string]any) error {
	b, err := json.Marshal(extra)
	if err != nil {
		return fmt.Errorf("dataset extra is not JSON serialisable: %w", err)
	}
	var norm map[string]any
	_ = json.Unmarshal(b, &norm)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.res.OutletExtra == nil {
		w.res.OutletExtra = map[string]map[string]any{}
	}
	w.res.OutletExtra[uri] = norm
	return nil
}
