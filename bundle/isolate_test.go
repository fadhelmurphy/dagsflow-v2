package bundle

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dagsflow-go/dag"
)

// The test binary doubles as the child process: `<test binary> __task run`.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == TaskCommand {
		TaskMain(os.Args[2:])
	}
	os.Exit(m.Run())
}

const isoDag = "iso_test"

// Registered at init, like a DAG in builtin_dags/, so the child has it too.
func init() {
	d := dag.NewDAG(isoDag, "")
	d.NewTask("stubborn", func(ctx *dag.TaskContext) error {
		time.Sleep(4 * time.Second) // ignores ctx on purpose
		return os.WriteFile(os.Getenv("ISO_TEST_MARKER"), []byte("still ran"), 0o644)
	}, dag.WithTimeout(time.Second))
	d.NewTask("push", func(ctx *dag.TaskContext) error {
		if !inChild() {
			return errors.New("expected to run in a child process")
		}
		return ctx.XComPush("pid", os.Getpid())
	})
	br := d.NewBranchTask("pick", func(ctx *dag.TaskContext) ([]string, error) { return []string{"left"}, nil })
	br.ThenAll(d.NewTask("left", nil), d.NewTask("right", nil))
	d.NewSensor("ready", func(ctx *dag.TaskContext) (bool, error) { return true, nil }, dag.SensorConfig{})
	d.NewTask("boom", func(ctx *dag.TaskContext) error { panic("kaboom") })
	dag.Register(d)
}

func inChild() bool { return len(os.Args) > 1 && os.Args[1] == TaskCommand }

type memRun struct {
	mu   sync.Mutex
	xcom map[string]any
}

func (m *memRun) XComPush(taskID string, _ int, key string, v any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.xcom[taskID+"."+key] = v
	return nil
}
func (m *memRun) XComPull(taskID, key string) (any, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.xcom[taskID+"."+key]
	return v, ok
}
func (m *memRun) TriggerDagRun(context.Context, string, map[string]any, bool) (string, error) {
	return "", nil
}
func (m *memRun) ExternalTaskState(string, string, time.Time) (dag.State, bool, error) {
	return "", false, nil
}
func (m *memRun) SetRendered(string, int, string, string) error            { return nil }
func (m *memRun) SetOutletExtra(string, int, string, map[string]any) error { return nil }

func isoTask(t *testing.T, id string) *dag.Task {
	t.Helper()
	d, ok := dag.Get(isoDag)
	if !ok {
		t.Fatal("dag not registered")
	}
	tk, ok := d.Task(id)
	if !ok {
		t.Fatalf("task %s missing", id)
	}
	return tk
}

func TestIsolatedGoTasks(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	IsolateCompiledIn(self)
	defer dag.SetIsolatedRunner(nil)
	mem := &memRun{xcom: map[string]any{}}
	marker := filepath.Join(t.TempDir(), "marker")
	info := dag.RunInfo{RunID: "manual__1", LogicalDate: time.Now(), TryNumber: 1, MapIndex: -1,
		Conf: map[string]any{}}

	t.Run("timeout kills code that ignores ctx", func(t *testing.T) {
		tk := isoTask(t, "stubborn")
		ctx, cancel := context.WithTimeout(context.Background(), tk.Timeout())
		defer cancel()
		t.Setenv("ISO_TEST_MARKER", marker) // inherited by the child
		tctx := dag.NewTaskContext(ctx, tk, info, mem, io.Discard)
		start := time.Now()
		_, err := tk.Run(tctx)
		if err == nil {
			t.Fatal("expected an error after the timeout")
		}
		if el := time.Since(start); el > 3*time.Second {
			t.Fatalf("Run returned after %s, want about 1s", el)
		}
		time.Sleep(4500 * time.Millisecond) // the task would have written the marker by now
		if _, err := os.Stat(marker); err == nil {
			t.Fatal("the task kept running after the timeout")
		}
	})

	t.Run("xcom goes through rpc and runs in another process", func(t *testing.T) {
		tk := isoTask(t, "push")
		if _, err := tk.Run(dag.NewTaskContext(context.Background(), tk, info, mem, io.Discard)); err != nil {
			t.Fatal(err)
		}
		v, ok := mem.XComPull("push", "pid")
		if !ok {
			t.Fatal("xcom not pushed")
		}
		if pid, _ := v.(float64); int(pid) == os.Getpid() || pid == 0 {
			t.Fatalf("task ran in pid %v, the test is pid %d", v, os.Getpid())
		}
	})

	t.Run("branch and sensor", func(t *testing.T) {
		tk := isoTask(t, "pick")
		sel, err := tk.Run(dag.NewTaskContext(context.Background(), tk, info, mem, io.Discard))
		if err != nil || len(sel) != 1 || sel[0] != "left" {
			t.Fatalf("branch = %v, %v", sel, err)
		}
		sn := isoTask(t, "ready")
		ok, err := sn.Poke(dag.NewTaskContext(context.Background(), sn, info, mem, io.Discard))
		if err != nil || !ok {
			t.Fatalf("poke = %v, %v", ok, err)
		}
	})

	t.Run("panic is reported", func(t *testing.T) {
		tk := isoTask(t, "boom")
		_, err := tk.Run(dag.NewTaskContext(context.Background(), tk, info, mem, io.Discard))
		if err == nil || !strings.Contains(err.Error(), "kaboom") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("unregistered DAGs stay in-process", func(t *testing.T) {
		// e.g. a builder preview parsed from YAML: a child would not know it
		d := dag.NewDAG("iso_unregistered", "")
		tk := d.NewTask("x", func(ctx *dag.TaskContext) error { return ctx.XComPush("pid", os.Getpid()) })
		if err := d.Validate(); err != nil {
			t.Fatal(err)
		}
		if _, err := tk.Run(dag.NewTaskContext(context.Background(), tk, info, mem, io.Discard)); err != nil {
			t.Fatal(err)
		}
		if v, _ := mem.XComPull("x", "pid"); v != os.Getpid() {
			t.Fatalf("ran in pid %v, want this process %d", v, os.Getpid())
		}
	})

	t.Run("in-process when turned off", func(t *testing.T) {
		dag.SetIsolatedRunner(nil)
		defer IsolateCompiledIn(self)
		tk := isoTask(t, "push")
		err := func() error {
			_, err := tk.Run(dag.NewTaskContext(context.Background(), tk, info, mem, io.Discard))
			return err
		}()
		if err == nil || !strings.Contains(err.Error(), "child process") {
			t.Fatalf("expected the in-process run to be detected, got %v", err)
		}
	})
}
