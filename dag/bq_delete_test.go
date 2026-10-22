package dag

import (
	"strings"
	"testing"
)

func TestBigQueryDeleteTable(t *testing.T) {
	f := setupGoogle(t)
	yml := "dag_id: del\nparams: {project_id: prj}\ntasks:\n  - id: drop\n    operator: bigquery_delete_table\n    tables: ['stg.old_{{ ds_nodash }}', 'prj.stg.gone']\n"
	dags, err := ParseYAML("d.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs) // logical date 2026-01-31
	v, _ := rs.XComPull("drop", "return_value")
	m, _ := v.(map[string]any)
	if m == nil {
		t.Fatalf("no output; calls: %v", f.calls)
	}
	if !strings.Contains(strings.Join(f.calls, " "), "DELETE /bigquery/v2/projects/prj/datasets/stg/tables/gone") {
		t.Fatalf("calls: %v", f.calls)
	}
	if d := m["deleted"].([]any); len(d) != 1 || d[0] != "prj.stg.old_20260131" {
		t.Fatalf("deleted: %v", m)
	}
	if len(m["missing"].([]any)) != 1 {
		t.Fatalf("gone should be missing: %v", m)
	}
	// fail_if_missing
	dags, _ = ParseYAML("d.yaml", []byte("dag_id: del\nparams: {project_id: prj}\ntasks:\n  - {id: drop, operator: bigquery_delete_table, tables: [stg.gone], fail_if_missing: true}\n"))
	task, _ := dags[0].Task("drop")
	if _, err := task.Run(loadCtxFor(t, task)); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("fail_if_missing: %v", err)
	}
	for _, y := range []string{
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_delete_table}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_delete_table, tables: [justname]}\n",
	} {
		dd, err := ParseYAML("a.yaml", []byte(y))
		if err == nil {
			err = dd[0].validate()
		}
		if err == nil {
			t.Fatalf("expected error for %s", y)
		}
	}
}

func loadCtxFor(t *testing.T, task *Task) *TaskContext {
	return NewTaskContext(t.Context(), task, RunInfo{RunID: "r", MapIndex: -1}, &memRun{}, nil)
}
