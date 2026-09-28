package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"dagsflow-go/dag"
)

type branchRows struct {
	Branch string `json:"branch"`
	Rows   int    `json:"rows"`
}

// A typed TaskFlow DAG runs end to end: outputs are passed between tasks,
// mapped results come back as a list in map-index order.
func TestTypedDagRuns(t *testing.T) {
	d := dag.NewDAG("typed_e2e", "")
	branches := dag.Fn(d, "list_branches", func(*dag.TaskContext) ([]string, error) {
		return []string{"jakarta", "bandung", "surabaya"}, nil
	})
	loaded := dag.Map(d, "load_branch", branches, func(ctx *dag.TaskContext, b string) (branchRows, error) {
		return branchRows{b, len(b) * 10}, nil
	})
	factor := dag.Fn(d, "factor", func(*dag.TaskContext) (int, error) { return 2, nil })
	total := dag.Fn2(d, "total", loaded, factor, func(_ *dag.TaskContext, rs []branchRows, f int) (int, error) {
		sum := 0
		for _, r := range rs {
			sum += r.Rows
		}
		return sum * f, nil
	})
	var published string
	dag.Consume(d, "publish", total, func(_ *dag.TaskContext, n int) error {
		published = fmt.Sprint(n)
		return nil
	})
	dag.Register(d)
	if err := dag.ImportErrors()["typed_e2e"]; err != nil {
		t.Fatal(err)
	}

	st := NewStore(t.TempDir())
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "", now, now, now, nil)
	if err := NewEngine(st, 4).Execute(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if r.State != RunSuccess {
		for id, ti := range r.Tasks {
			t.Logf("%s: %s %s", id, ti.State, ti.Error)
		}
		t.Fatalf("run state %s", r.State)
	}
	if published != "440" { // (70+70+80)*2: len("jakarta")*10, len("bandung")*10, len("surabaya")*10
		t.Errorf("published %q, want 440", published)
	}
	if n := len(r.Mapped["load_branch"]); n != 3 {
		t.Errorf("%d mapped instances", n)
	}
}

// A typed task fails with a clear error when its input is missing.
func TestTypedMissingInput(t *testing.T) {
	d := dag.NewDAG("typed_missing", "")
	src := d.NewBashTask("src", "true", nil) // pushes "" as return_value
	out := dag.OutputOf[int](src, "not_pushed")
	dag.Consume(d, "use", out, func(*dag.TaskContext, int) error { return nil })
	dag.Register(d)
	st := NewStore(t.TempDir())
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "", now, now, now, nil)
	_ = NewEngine(st, 2).Execute(context.Background(), r)
	if ti := r.Tasks["use"]; ti.State != dag.StateFailed || !strings.Contains(ti.Error, "input from src") {
		t.Errorf("use: %s %q", ti.State, ti.Error)
	}
}
