package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"dagsflow-go/dag"
)

func runOnce(t *testing.T, st *Store, r *DagRun) {
	t.Helper()
	if err := NewEngine(st, 4).Execute(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}

// Clearing a mapped task keeps its instances, so try numbers keep growing
// (Airflow semantics) and a single map index can be cleared on its own.
func TestClearMappedKeepsTryNumbers(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	d := dag.NewDAG("clear_mapped", "")
	items := dag.Fn(d, "list", func(*dag.TaskContext) ([]string, error) { return []string{"a", "b", "c"}, nil })
	dag.Map(d, "load", items, func(_ *dag.TaskContext, s string) (string, error) {
		mu.Lock()
		calls[s]++
		n := calls[s]
		mu.Unlock()
		if s == "b" && n < 3 {
			return "", errors.New("b is flaky")
		}
		return s, nil
	})
	dag.Register(d)
	st := NewStore(t.TempDir())
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "", now, now, now, nil)

	runOnce(t, st, r)
	if r.State != RunFailed || r.Mapped["load"][1].TryNumber != 1 {
		t.Fatalf("first run: %s, b try %d", r.State, r.Mapped["load"][1].TryNumber)
	}

	// clear only map index 1
	cleared, err := ClearTaskInstances(d, r, ClearOptions{TaskIDs: []string{"load"}, MapIndex: 1})
	if err != nil || len(cleared) != 1 || cleared[0] != "load[1]" {
		t.Fatalf("clear: %v %v", cleared, err)
	}
	runOnce(t, st, r)
	kids := r.Mapped["load"]
	if kids[1].TryNumber != 2 || kids[0].TryNumber != 1 || kids[2].TryNumber != 1 {
		t.Fatalf("after clearing [1]: tries %d %d %d", kids[0].TryNumber, kids[1].TryNumber, kids[2].TryNumber)
	}
	if calls["a"] != 1 || calls["c"] != 1 {
		t.Fatalf("other instances reran: %v", calls)
	}

	// clear failed instances of the whole task: b succeeds on its third try
	if _, err := ClearTasks(d, r, []string{"load"}, false, false, true); err != nil {
		t.Fatal(err)
	}
	runOnce(t, st, r)
	kids = r.Mapped["load"]
	if r.State != RunSuccess || kids[1].TryNumber != 3 || kids[1].State != dag.StateSuccess {
		t.Fatalf("third run: %s, b %s try %d", r.State, kids[1].State, kids[1].TryNumber)
	}
	if calls["a"] != 1 {
		t.Fatalf("only_failed reran a: %v", calls)
	}
	// XCom of the mapped task is still complete, in map-index order
	if v, _ := r.XCom[mappedKey("load", 1)]["return_value"]; v != "b" {
		t.Fatalf("xcom of [1] = %v", v)
	}
}

// After a clear the task gets its full retries again (max_tries = try + retries).
func TestClearExtendsRetries(t *testing.T) {
	d := dag.NewDAG("clear_retries", "")
	d.NewTask("boom", func(*dag.TaskContext) error { return errors.New("always") }, dag.WithRetries(1, 0))
	dag.Register(d)
	st := NewStore(t.TempDir())
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "", now, now, now, nil)
	runOnce(t, st, r)
	if ti := r.Tasks["boom"]; ti.TryNumber != 2 {
		t.Fatalf("first run tries %d, want 2", ti.TryNumber)
	}
	if _, err := ClearTasks(d, r, []string{"boom"}, false, false, false); err != nil {
		t.Fatal(err)
	}
	if ti := r.Tasks["boom"]; ti.MaxTries != 4 {
		t.Fatalf("max_tries after clear %d, want 4", ti.MaxTries)
	}
	runOnce(t, st, r)
	if ti := r.Tasks["boom"]; ti.TryNumber != 4 || ti.State != dag.StateFailed {
		t.Fatalf("after clear: %s try %d, want failed try 4", ti.State, ti.TryNumber)
	}
}
