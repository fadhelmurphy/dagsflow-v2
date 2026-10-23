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
