package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dagsflow-go/dag"
)

// Postgres tests run when DAGSFLOW_TEST_DATABASE_URL points at a database the
// tests may wipe, e.g. postgres://postgres@127.0.0.1:5432/dagsflow_test.

func TestMain(m *testing.M) {
	runLease = 3 * time.Second
	componentTimeout = 3 * time.Second
	heartbeatInterval = 500 * time.Millisecond
	os.Exit(m.Run())
}

func pgStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("DAGSFLOW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("DAGSFLOW_TEST_DATABASE_URL not set")
	}
	st, err := OpenStore(t.TempDir(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

// resetPG wipes every table (tests share one database).
func resetPG(t *testing.T) {
	t.Helper()
	st := pgStore(t)
	pg := st.b.(*pgBackend)
	ctx := context.Background()
	if _, err := pg.pool.Exec(ctx, `TRUNCATE dag_run, dag_meta, kv_table, run_command, audit_log, task_log, component, lease, pool_slot, task_job`); err != nil {
		t.Fatal(err)
	}
}

func TestPGRunConcurrency(t *testing.T) {
	resetPG(t)
	a, b := pgStore(t), pgStore(t)
	d := dag.NewDAG("pg_conc", "")
	d.NewTask("x", func(*dag.TaskContext) error { return nil })
	dag.Register(d)
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "r1", now, now, now, nil)
	if ok, err := a.CreateRun(r); !ok || err != nil {
		t.Fatalf("create: %v %v", ok, err)
	}
	if ok, _ := b.CreateRun(NewDagRun(d, RunTypeManual, "r1", now, now, now, nil)); ok {
		t.Fatal("duplicate create succeeded")
	}
	// optimistic concurrency between two webservers
	r1, _ := a.LoadRun(d.ID, "r1")
	r2, _ := b.LoadRun(d.ID, "r1")
	r1.Note = "a"
	if err := a.SaveRun(r1); err != nil {
		t.Fatal(err)
	}
	r2.Note = "b"
	if err := b.SaveRun(r2); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale save: %v", err)
	}
	// exactly one scheduler claims a queued run
	var wins atomic.Int32
	var wg sync.WaitGroup
	for _, st := range []*Store{a, b, a, b} {
		wg.Add(1)
		go func(st *Store) {
			defer wg.Done()
			if c, err := st.ClaimRun(d.ID, "r1", 1); err == nil && c != nil {
				wins.Add(1)
			}
		}(st)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("claims won: %d", wins.Load())
	}
	// a non-owner cannot modify a running run
	r3, _ := b.LoadRun(d.ID, "r1")
	if r3.State != RunRunning {
		t.Fatalf("state %s", r3.State)
	}
	r3.Note = "x"
	if err := b.SaveRun(r3); !errors.Is(err, ErrConflict) && !strings.Contains(fmt.Sprint(err), "changed") {
		// b may be the owner; then this is fine
		if c, _ := b.ClaimRun(d.ID, "r1", 1); c != nil {
			t.Fatal("claimed twice")
		}
	}
	// max_active_runs across schedulers
	r4 := NewDagRun(d, RunTypeManual, "r2", now, now, now, nil)
	_, _ = a.CreateRun(r4)
	if c, _ := b.ClaimRun(d.ID, "r2", 1); c != nil {
		t.Fatal("max_active_runs=1 ignored across schedulers")
	}
	if c, _ := b.ClaimRun(d.ID, "r2", 2); c == nil {
		t.Fatal("claim with room failed")
	}
}

func TestPGOrphanAdoption(t *testing.T) {
	resetPG(t)
	crashed, survivor := pgStore(t), pgStore(t)
	d := dag.NewDAG("pg_orphan", "")
	d.NewTask("x", func(*dag.TaskContext) error { return nil })
	dag.Register(d)
	now := time.Now().UTC()
	_, _ = crashed.CreateRun(NewDagRun(d, RunTypeManual, "r1", now, now, now, nil))
	c, err := crashed.ClaimRun(d.ID, "r1", 1)
	if err != nil || c == nil {
		t.Fatalf("claim: %v", err)
	}
	crashed.Unregister() // stops renewing: like a killed scheduler
	if ids, _ := survivor.RequeueOrphans(); len(ids) != 0 {
		t.Fatalf("requeued a live run: %v", ids)
	}
	time.Sleep(runLease + time.Second)
	ids, err := survivor.RequeueOrphans()
	if err != nil || len(ids) != 1 {
		t.Fatalf("orphans: %v %v", ids, err)
	}
	// the survivor resumes it
	r, err := survivor.ClaimRun(d.ID, "r1", 1)
	if err != nil || r == nil {
		t.Fatalf("adopt: %v", err)
	}
	if err := NewEngine(survivor, 2).Execute(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	final, _ := survivor.LoadRun(d.ID, "r1")
	if final.State != RunSuccess {
		t.Fatalf("state %s", final.State)
	}
	// the old owner lost the run: its writes are rejected (fencing)
	c.Note = "zombie"
	if err := crashed.SaveRun(c); !errors.Is(err, ErrConflict) {
		t.Fatalf("zombie write: %v", err)
	}
}

func TestPGLeaseAndTables(t *testing.T) {
	resetPG(t)
	a, b := pgStore(t), pgStore(t)
	if !a.TryLease("leader", time.Second) || b.TryLease("leader", time.Second) {
		t.Fatal("lease not exclusive")
	}
	if !a.TryLease("leader", time.Second) {
		t.Fatal("renew failed")
	}
	time.Sleep(1100 * time.Millisecond)
	if !b.TryLease("leader", time.Second) {
		t.Fatal("expired lease not taken over")
	}
	// transactional table updates from many goroutines/processes
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := a
			if i%2 == 1 {
				st = b
			}
			if err := st.SetVariable(fmt.Sprintf("k%d", i), "v", ""); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	vars, _ := a.Variables()
	if len(vars) != 20 {
		t.Fatalf("lost updates: %d variables", len(vars))
	}
	k1, _ := a.SecretKey()
	k2, _ := b.SecretKey()
	if len(k1) < 32 || string(k1) != string(k2) {
		t.Fatal("secret key not shared")
	}
}

// Two schedulers (engines on separate stores) share a pool of 1 slot.
func TestPGGlobalPool(t *testing.T) {
	resetPG(t)
	a, b := pgStore(t), pgStore(t)
	a.Register("scheduler", nil)
	b.Register("scheduler", nil)
	if err := a.SetPool("bq", 1, ""); err != nil {
		t.Fatal(err)
	}
	var cur, peak atomic.Int32
	d := dag.NewDAG("pg_pool", "")
	for i := 0; i < 3; i++ {
		d.NewTask(fmt.Sprintf("t%d", i), func(*dag.TaskContext) error {
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(300 * time.Millisecond)
			cur.Add(-1)
			return nil
		}, dag.WithPool("bq"))
	}
	dag.Register(d)
	now := time.Now().UTC()
	var wg sync.WaitGroup
	for i, st := range []*Store{a, b} {
		r := NewDagRun(d, RunTypeManual, fmt.Sprintf("r%d", i), now, now, now, nil)
		wg.Add(1)
		go func(st *Store, r *DagRun) {
			defer wg.Done()
			if err := NewEngine(st, 8).Execute(context.Background(), r); err != nil {
				t.Error(err)
			}
			if r.State != RunSuccess {
				t.Errorf("run %s: %s", r.RunID, r.State)
			}
		}(st, r)
	}
	wg.Wait()
	if peak.Load() != 1 {
		t.Fatalf("pool of 1 slot ran %d tasks at once across schedulers", peak.Load())
	}
}

func remoteDAG(id string) *dag.DAG {
	d := dag.NewDAG(id, "")
	items := dag.Fn(d, "list", func(ctx *dag.TaskContext) ([]string, error) {
		ctx.Logf("listing on %s", "worker")
		return []string{"a", "b", "c"}, nil
	})
	up := dag.Map(d, "upper", items, func(ctx *dag.TaskContext, s string) (string, error) {
		return strings.ToUpper(s), nil
	})
	dag.Consume(d, "join", up, func(ctx *dag.TaskContext, xs []string) error {
		if strings.Join(xs, "") != "ABC" {
			return fmt.Errorf("got %v", xs)
		}
		return ctx.XComPush("joined", strings.Join(xs, "-"))
	})
	dag.Register(d)
	return d
}

func TestPGWorkerExecutor(t *testing.T) {
	resetPG(t)
	sched, wst := pgStore(t), pgStore(t)
	sched.Register("scheduler", nil)
	d := remoteDAG("pg_remote")
	flaky := dag.NewDAG("pg_remote_retry", "")
	var n atomic.Int32
	flaky.NewTask("flaky", func(ctx *dag.TaskContext) error {
		if n.Add(1) < 2 {
			return errors.New("first try fails")
		}
		return nil
	}, dag.WithRetries(1, 0))
	flaky.NewTask("slow", func(ctx *dag.TaskContext) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Minute):
			return nil
		}
	}, dag.WithTimeout(time.Second))
	dag.Register(flaky)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &Worker{Store: wst, Concurrency: 4, Grace: time.Second, Log: testLogger(t)}
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	e := NewEngine(sched, 8)
	e.Executor = &QueueExecutor{Store: sched}
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "r1", now, now, now, nil)
	if err := e.Execute(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if r.State != RunSuccess {
		t.Fatalf("state %s: %+v", r.State, r.Tasks)
	}
	if v := r.XCom["join"]["joined"]; v != "A-B-C" {
		t.Fatalf("xcom from worker: %v", v)
	}
	log, err := sched.ReadTaskLog(LogKey{d.ID, "r1", "list", -1, 1})
	if err != nil || !strings.Contains(string(log), "Running on worker") || !strings.Contains(string(log), "listing on worker") {
		t.Fatalf("log: %v %q", err, log)
	}

	r2 := NewDagRun(flaky, RunTypeManual, "r2", now, now, now, nil)
	if err := e.Execute(context.Background(), r2); err != nil {
		t.Fatal(err)
	}
	if ti := r2.Tasks["flaky"]; ti.State != dag.StateSuccess || ti.TryNumber != 2 {
		t.Fatalf("retry on worker: %s try %d", ti.State, ti.TryNumber)
	}
	if ti := r2.Tasks["slow"]; ti.State != dag.StateFailed || !strings.Contains(ti.Error, "timed out") {
		t.Fatalf("timeout on worker: %s %q", ti.State, ti.Error)
	}
	cancel()
	<-done
}

func testLogger(t *testing.T) *log.Logger { return log.New(testWriter{t}, "", 0) }

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

func TestPGWorkerLost(t *testing.T) {
	resetPG(t)
	sched := pgStore(t)
	sched.Register("scheduler", nil)
	d := dag.NewDAG("pg_lost", "")
	d.NewTask("x", func(*dag.TaskContext) error { return nil })
	dag.Register(d)
	// a "worker" claims the job and dies without heartbeats
	go func() {
		for i := 0; i < 50; i++ {
			if j, _ := sched.be().claimJob([]string{"default"}, "dead-worker", 0); j != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	e := NewEngine(sched, 2)
	e.Executor = &QueueExecutor{Store: sched, LostAfter: 2 * time.Second}
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "r1", now, now, now, nil)
	_ = e.Execute(context.Background(), r)
	if ti := r.Tasks["x"]; ti.State != dag.StateFailed || !strings.Contains(ti.Error, "lost") {
		t.Fatalf("%s %q", ti.State, ti.Error)
	}
}

// Fake Kubernetes API: creating a pod starts `worker --job-id` in-process.
func TestPGKubernetesExecutor(t *testing.T) {
	resetPG(t)
	sched := pgStore(t)
	sched.Register("scheduler", nil)
	d := remoteDAG("pg_k8s")
	var mu sync.Mutex
	pods := map[string]string{} // name -> phase
	var created []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(401)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/jobs/pods":
			var pod map[string]any
			_ = json.NewDecoder(r.Body).Decode(&pod)
			name := pod["metadata"].(map[string]any)["name"].(string)
			args := pod["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["args"].([]any)
			var id int64
			fmt.Sscan(args[2].(string), &id)
			mu.Lock()
			pods[name] = "Running"
			created = append(created, pod)
			mu.Unlock()
			go func() {
				wst, err := OpenStore(t.TempDir(), os.Getenv("DAGSFLOW_TEST_DATABASE_URL"))
				if err != nil {
					t.Error(err)
					return
				}
				defer wst.Close()
				_ = (&Worker{Store: wst, Log: testLogger(t)}).RunJob(context.Background(), id)
				mu.Lock()
				pods[name] = "Succeeded"
				mu.Unlock()
			}()
			w.WriteHeader(201)
			_ = json.NewEncoder(w).Encode(pod)
		case r.Method == http.MethodGet:
			name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			mu.Lock()
			ph, ok := pods[name]
			mu.Unlock()
			if !ok {
				w.WriteHeader(404)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"phase": ph}})
		case r.Method == http.MethodDelete:
			name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			mu.Lock()
			delete(pods, name)
			mu.Unlock()
		default:
			w.WriteHeader(400)
		}
	}))
	defer srv.Close()
	t.Setenv("DAGSFLOW_K8S_API_URL", srv.URL)
	t.Setenv("DAGSFLOW_K8S_TOKEN", "tok")
	t.Setenv("DAGSFLOW_K8S_NAMESPACE", "jobs")
	t.Setenv("DAGSFLOW_K8S_IMAGE", "example/dagsflow:1")
	ex, err := NewExecutor("kubernetes", sched)
	if err != nil {
		t.Fatal(err)
	}
	e := NewEngine(sched, 8)
	e.Executor = ex
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "r1", now, now, now, nil)
	if err := e.Execute(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if r.State != RunSuccess {
		t.Fatalf("state %s %+v", r.State, r.Tasks)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(created) != 5 || len(pods) != 0 {
		t.Fatalf("pods created %d, left %d", len(created), len(pods))
	}
	c := created[0]["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
	if c["image"] != "example/dagsflow:1" || c["command"].([]any)[0] != "dagsflow-go" {
		t.Fatalf("container: %v", c)
	}
	if created[0]["spec"].(map[string]any)["restartPolicy"] != "Never" {
		t.Fatal("restartPolicy")
	}
}

func TestPodOverrideMerge(t *testing.T) {
	k := &KubernetesExecutor{command: "dagsflow-go", template: map[string]any{"spec": map[string]any{
		"containers": []any{map[string]any{"name": "base", "image": "img", "env": []any{map[string]any{"name": "A", "value": "1"}}},
			map[string]any{"name": "sidecar", "image": "proxy"}}}}}
	d := dag.NewDAG("pod_override_dag", "")
	tk := d.NewTask("heavy_Task_1", func(*dag.TaskContext) error { return nil }, dag.WithPodOverride(map[string]any{
		"spec": map[string]any{"containers": []any{map[string]any{"name": "base",
			"resources": map[string]any{"limits": map[string]any{"memory": "2Gi"}}}},
			"nodeSelector": map[string]any{"pool": "highmem"}}}))
	pod, err := k.PodFor(tk, 42, JobPayload{DagID: d.ID, TaskID: tk.ID, RunID: "manual__x", Try: 1, MapIndex: -1})
	if err != nil {
		t.Fatal(err)
	}
	spec := pod["spec"].(map[string]any)
	cs := spec["containers"].([]any)
	base := cs[0].(map[string]any)
	if len(cs) != 2 || base["image"] != "img" || base["resources"] == nil || base["env"] == nil {
		t.Fatalf("merge: %v", cs)
	}
	if spec["nodeSelector"].(map[string]any)["pool"] != "highmem" {
		t.Fatal("nodeSelector")
	}
	if a := base["args"].([]any); a[0] != "worker" || a[2] != "42" {
		t.Fatalf("args %v", a)
	}
	name := pod["metadata"].(map[string]any)["name"].(string)
	if name != "df-pod-override-dag-heavy-task-1-42" || cs[1].(map[string]any)["args"] != nil {
		t.Fatalf("name %s", name)
	}
}

func TestPGBackupRoundTrip(t *testing.T) {
	resetPG(t)
	js := NewStore(t.TempDir())
	d := dag.NewDAG("pg_backup", "")
	d.NewTask("x", func(*dag.TaskContext) error { return nil })
	dag.Register(d)
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "r1", now, now, now, nil)
	if err := NewEngine(js, 2).Execute(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	_ = js.SetVariable("k", "v", "")
	_ = js.CreateUser("admin", "password123", RoleAdmin)
	js.Audit(AuditEvent{Event: "hello", DagID: d.ID})
	b, err := js.Export(1000)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(b)
	var b2 Backup
	_ = json.Unmarshal(raw, &b2)
	pg := pgStore(t)
	st, err := pg.Import(&b2)
	if err != nil {
		t.Fatal(err)
	}
	if st.Runs != 1 || st.Tables < 2 {
		t.Fatalf("%+v", st)
	}
	got, err := pg.LoadRun(d.ID, "r1")
	if err != nil || got.State != RunSuccess {
		t.Fatalf("%v %v", got, err)
	}
	if u, ok := pg.Authenticate("admin", "password123", ""); !ok || u.Role != RoleAdmin {
		t.Fatal("user not imported")
	}
	if ev, _ := pg.AuditEvents(d.ID, "", "", 10); len(ev) != 1 {
		t.Fatalf("events %v", ev)
	}
	// import is idempotent for runs
	if st, _ := pg.Import(&b2); st.SkippedRuns != 1 {
		t.Fatalf("%+v", st)
	}
	stats, err := pg.Cleanup(time.Now().Add(time.Hour))
	if err != nil || stats.Runs != 1 {
		t.Fatalf("cleanup %+v %v", stats, err)
	}
}

// A scheduler that stops (graceful shutdown or crash) does not kill the task
// running on a worker: the scheduler that adopts the run re-attaches to it.
func TestPGFailoverReattach(t *testing.T) {
	resetPG(t)
	a, b, wst := pgStore(t), pgStore(t), pgStore(t)
	a.Register("scheduler", nil)
	b.Register("scheduler", nil)
	var runs atomic.Int32
	d := dag.NewDAG("pg_reattach", "")
	d.NewTask("long", func(ctx *dag.TaskContext) error {
		runs.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(4 * time.Second):
		}
		return ctx.XComPush("done", true)
	})
	dag.Register(d)
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	go func() { _ = (&Worker{Store: wst, Concurrency: 2, Grace: time.Second, Log: testLogger(t)}).Run(wctx) }()

	now := time.Now().UTC()
	_, _ = a.CreateRun(NewDagRun(d, RunTypeManual, "r1", now, now, now, nil))
	ra, _ := a.ClaimRun(d.ID, "r1", 1)
	ea := NewEngine(a, 4)
	ea.Executor = &QueueExecutor{Store: a}
	actx, stopA := context.WithCancel(context.Background())
	doneA := make(chan struct{})
	go func() { _ = ea.Execute(actx, ra); close(doneA) }()
	time.Sleep(1500 * time.Millisecond)
	stopA() // graceful scheduler shutdown while the task runs on the worker
	<-doneA
	mid, _ := b.LoadRun(d.ID, "r1")
	if mid.State != RunQueued || mid.Tasks["long"].JobID == 0 {
		t.Fatalf("after shutdown: %s job %d", mid.State, mid.Tasks["long"].JobID)
	}
	rb, err := b.ClaimRun(d.ID, "r1", 1)
	if err != nil || rb == nil {
		t.Fatalf("adopt: %v", err)
	}
	eb := NewEngine(b, 4)
	eb.Executor = &QueueExecutor{Store: b}
	if err := eb.Execute(context.Background(), rb); err != nil {
		t.Fatal(err)
	}
	ti := rb.Tasks["long"]
	if rb.State != RunSuccess || ti.TryNumber != 1 || runs.Load() != 1 || rb.XCom["long"]["done"] != true {
		t.Fatalf("state %s try %d executions %d xcom %v", rb.State, ti.TryNumber, runs.Load(), rb.XCom["long"])
	}
	log, _ := b.ReadTaskLog(LogKey{d.ID, "r1", "long", -1, 1})
	if !strings.Contains(string(log), "re-attached to job") {
		t.Fatalf("log: %s", log)
	}
}

// A scheduler whose run was taken over stops executing it even if it never
// writes to the run again (fencing through the lease renewal).
func TestPGFencingWithoutWrite(t *testing.T) {
	resetPG(t)
	a, b := pgStore(t), pgStore(t)
	a.Register("scheduler", nil)
	b.Register("scheduler", nil)
	stopped := make(chan struct{})
	d := dag.NewDAG("pg_fence", "")
	d.NewTask("long", func(ctx *dag.TaskContext) error {
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	})
	dag.Register(d)
	now := time.Now().UTC()
	_, _ = a.CreateRun(NewDagRun(d, RunTypeManual, "r1", now, now, now, nil))
	ra, _ := a.ClaimRun(d.ID, "r1", 1)
	done := make(chan struct{})
	go func() { _ = NewEngine(a, 2).Execute(context.Background(), ra); close(done) }()
	time.Sleep(time.Second)
	// b takes the run over (as if a's lease had expired during a partition)
	pg := b.b.(*pgBackend)
	if _, err := pg.pool.Exec(context.Background(), `UPDATE dag_run SET owner=$1, lease_until=now()+interval '1 minute' WHERE dag_id=$2`, b.ID, d.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("old owner kept executing the task")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("old owner's Execute did not return")
	}
	if r, _ := b.LoadRun(d.ID, "r1"); r.State != RunRunning {
		t.Fatalf("old owner overwrote the run: %s", r.State)
	}
}
