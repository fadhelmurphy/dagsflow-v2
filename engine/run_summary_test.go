package engine

import (
	"fmt"
	"testing"
	"time"

	"dagsflow-go/dag"
)

func checkRunSummary(t *testing.T, st *Store) {
	t.Helper()
	d := dag.NewDAG("sum_dag", "")
	d.NewTask("x", func(*dag.TaskContext) error { return nil })
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 30; i++ {
		r := NewDagRun(d, "manual", fmt.Sprintf("r%02d", i), base.Add(time.Duration(i)*time.Hour), base, base, nil)
		if i%3 == 0 {
			r.State = RunFailed
		} else {
			r.State = RunSuccess
		}
		if _, err := st.CreateRun(r); err != nil {
			t.Fatal(err)
		}
	}
	counts, latest, err := st.RunSummary("sum_dag", 5)
	if err != nil {
		t.Fatal(err)
	}
	if counts[RunFailed] != 10 || counts[RunSuccess] != 20 {
		t.Fatalf("counts: %v", counts)
	}
	if len(latest) != 5 || latest[0].RunID != "r25" || latest[4].RunID != "r29" {
		var ids []string
		for _, r := range latest {
			ids = append(ids, r.RunID)
		}
		t.Fatalf("latest: %v", ids)
	}
}

func TestRunSummaryJSON(t *testing.T) { checkRunSummary(t, &Store{Home: t.TempDir()}) }

func TestPGRunSummary(t *testing.T) {
	resetPG(t)
	checkRunSummary(t, pgStore(t))
}

func TestRunPage(t *testing.T) { runPage(t, NewStore(t.TempDir())) }

func TestPGRunPage(t *testing.T) { resetPG(t); runPage(t, pgStore(t)) }

func runPage(t *testing.T, st *Store) {
	d := dag.NewDAG("page_dag", "")
	d.NewTask("a", func(*dag.TaskContext) error { return nil })
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		at := base.Add(time.Duration(i) * time.Hour)
		typ, state := "scheduled", RunSuccess
		if i%3 == 0 {
			typ = "manual"
		}
		if i == 9 {
			state = RunFailed
		}
		r := NewDagRun(d, typ, "", at, at, at, nil)
		r.State = state
		if _, err := st.CreateRun(r); err != nil {
			t.Fatal(err)
		}
	}
	runs, n, counts, err := st.RunPage("page_dag", RunFilter{}, 4)
	if err != nil || n != 10 || len(runs) != 4 || counts[RunSuccess] != 9 || counts[RunFailed] != 1 || !runs[3].LogicalDate.Equal(base.Add(9*time.Hour)) || !runs[0].LogicalDate.Before(runs[3].LogicalDate) {
		t.Fatalf("all: %d runs of %d, %v, %v", len(runs), n, counts, err)
	}
	runs, n, _, _ = st.RunPage("page_dag", RunFilter{RunType: "manual", Before: base.Add(5 * time.Hour)}, 10)
	if n != 2 || len(runs) != 2 || !runs[1].LogicalDate.Equal(base.Add(3*time.Hour)) {
		t.Fatalf("manual before 05:00: %d %d", n, len(runs))
	}
	runs, n, _, _ = st.RunPage("page_dag", RunFilter{State: RunFailed}, 10)
	if n != 1 || len(runs) != 1 {
		t.Fatalf("failed: %d", n)
	}
	// a run that changes is read again (the JSON store caches unchanged files)
	r, _ := st.LoadRun("page_dag", runs[0].RunID)
	r.State = RunSuccess
	if err := st.SaveRun(r); err != nil {
		t.Fatal(err)
	}
	if _, n, counts, _ := st.RunPage("page_dag", RunFilter{State: RunFailed}, 10); n != 0 || counts[RunSuccess] != 10 {
		t.Fatalf("after a change: %d failed, %v", n, counts)
	}
}
