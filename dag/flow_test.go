package dag

import (
	"sort"
	"strings"
	"testing"
	"time"
)

func noop(*TaskContext) error { return nil }

func ups(t *Task) string {
	var ids []string
	for _, u := range t.Upstream() {
		ids = append(ids, u.ID)
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

func TestChainParallelAfter(t *testing.T) {
	d := NewDAG("t_chain", "")
	a, b, c, e := d.NewTask("a", noop), d.NewTask("b", noop), d.NewTask("c", noop), d.NewTask("e", noop)
	f := d.NewTask("f", noop)
	Chain(a, Parallel(b, c), e) // a >> [b, c] >> e
	f.After(b, c)               // [b, c] >> f
	if err := d.validate(); err != nil {
		t.Fatal(err)
	}
	for task, want := range map[*Task]string{b: "a", c: "a", e: "b,c", f: "b,c", a: ""} {
		if got := ups(task); got != want {
			t.Errorf("%s upstream = %q, want %q", task.ID, got, want)
		}
	}
	// linking twice is idempotent
	a.Then(b)
	if got := ups(b); got != "a" {
		t.Errorf("duplicate link: %q", got)
	}
}

func TestDefinitionErrorsDoNotPanic(t *testing.T) {
	d := NewDAG("t_dup", "")
	x := d.NewTask("x", noop)
	x2 := d.NewTask("x", noop) // duplicate
	x.Then(x2)
	x.Then(x) // self
	other := NewDAG("t_other", "")
	x.Then(other.NewTask("y", noop)) // cross DAG
	err := d.validate()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"duplicate task id \"x\"", "cannot depend on itself", "different DAGs"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestTypedWiring(t *testing.T) {
	d := NewDAG("t_typed", "")
	list := Fn(d, "list", func(*TaskContext) ([]string, error) { return nil, nil })
	n := Fn(d, "n", func(*TaskContext) (int, error) { return 2, nil })
	load := Map(d, "load", list, func(_ *TaskContext, s string) (int, error) { return len(s), nil })
	total := Fn2(d, "total", load, n, func(_ *TaskContext, rows []int, k int) (int, error) { return len(rows) * k, nil })
	Consume(d, "publish", total, func(*TaskContext, int) error { return nil })
	if err := d.validate(); err != nil {
		t.Fatal(err)
	}
	if got := ups(load.Task()); got != "list" {
		t.Errorf("load upstream %q", got)
	}
	if got := ups(total.Task()); got != "load,n" {
		t.Errorf("total upstream %q", got)
	}
	if ms := load.Task().MapSource(); ms == nil || ms.XComTask != "list" || ms.XComKey != ReturnKey {
		t.Errorf("load is not mapped over list: %+v", ms)
	}
}

func TestJinjaCompat(t *testing.T) {
	known := map[string]bool{"ds": true, "ds_nodash": true, "params": true, "conf": true, "run_id": true, "item": true, "project_id": true}
	cases := map[string]string{
		"{{ ds }}":                                            "{{ .ds }}",
		"{{ .ds }}":                                           "{{ .ds }}",
		"`{{ params.project_id }}.bronze.t`":                  "`{{ .params.project_id }}.bronze.t`",
		"{{ project_id }}":                                    "{{ .project_id }}",
		"{{ dag_run.conf['table'] }}":                         "{{ .conf.table }}",
		"{{ dag_run.conf.table }}":                            "{{ .conf.table }}",
		"{{ var.value.region }}":                              `{{ var "region" }}`,
		"{{ macros.ds_add(ds, -1) }}":                         "{{ ds_add .ds -1 }}",
		"{{ ti.xcom_pull(task_ids='a', key='k') }}":           `{{ xcom_pull "a" "k" }}`,
		"{{ ti.xcom_pull(task_ids=\"a\") }}":                  `{{ xcom_pull "a" "return_value" }}`,
		`{{ if eq .x "ds" }}ok{{ end }}`:                      `{{ if eq .x "ds" }}ok{{ end }}`,
		"{{ range $i, $x := .params.list }}{{ $x }}{{ end }}": "{{ range $i, $x := .params.list }}{{ $x }}{{ end }}",
		"{{- ds_nodash -}}":                                   "{{- .ds_nodash -}}",
		"{{ len .params }}":                                   "{{ len .params }}",
		"{{ printf \"%s\" .ds }}":                             "{{ printf \"%s\" .ds }}",
		"{{ index .conf \"k\" }}":                             "{{ index .conf \"k\" }}",
		"{{ not .flag }}":                                     "{{ not .flag }}",
		"no template":                                         "no template",
	}
	for in, want := range cases {
		if got := jinjaCompat(in, known); got != want {
			t.Errorf("jinjaCompat(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRenderJinjaAndGoStyles(t *testing.T) {
	d := NewDAG("t_render", "", WithParams(map[string]any{"project_id": "p"}))
	task := d.NewTask("x", noop)
	logical := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	ctx := NewPreviewContext(task, RunInfo{LogicalDate: logical, RunID: "r1", MapIndex: -1}, nil)
	got, err := ctx.Render("SELECT '{{ ds }}', '{{ .ds_nodash }}' FROM `{{ params.project_id }}.x` -- {{ macros.ds_add(ds, -1) }}", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT '2026-09-28', '20260928' FROM `p.x` -- 2026-09-27"
	if got != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}
