package dag

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runOne(t *testing.T, d *DAG, id string, rs *memRun) ([]string, error) {
	t.Helper()
	task, ok := d.Task(id)
	if !ok {
		t.Fatalf("no task %s", id)
	}
	now := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
	ctx := NewTaskContext(context.Background(), task, RunInfo{RunID: "r", LogicalDate: now, DataIntervalStart: now,
		DataIntervalEnd: now, MapIndex: -1, Conf: map[string]any{}}, rs, io.Discard)
	return task.Run(ctx)
}

func TestIfNodeYAML(t *testing.T) {
	yml := `dag_id: ifs
params: {mode: full}
tasks:
  - id: src
    operator: bash
    command: echo
  - id: check
    operator: branch
    upstream: [src]
    path: length
    op: '>'
    value: 1
    then: [load]
    else: [notify]
  - id: load
    upstream: [check]
  - id: notify
    upstream: [check]
  - id: by_param
    operator: branch
    left: " {{ .params.mode }}\n"
    op: '=='
    value: full
    then: [full]
  - id: full
    upstream: [by_param]
`
	dags, err := ParseYAML("ifs.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	d := dags[0]
	if err := d.validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		out  any
		want string
	}{
		{[]any{map[string]any{"a": 1}, map[string]any{"a": 2}}, "load"},
		{[]any{map[string]any{"a": 1}}, "notify"},
		{`[{"a":1},{"a":2},{"a":3}]`, "load"}, // JSON text (an HTTP body) is parsed
		{nil, "notify"},
		{"[]\n", "notify"},
	} {
		rs := &memRun{}
		_ = rs.XComPush("src", -1, "return_value", tc.out)
		sel, err := runOne(t, d, "check", rs)
		if err != nil {
			t.Fatal(err)
		}
		if len(sel) != 1 || sel[0] != tc.want {
			t.Fatalf("%v: selected %v, want %s", tc.out, sel, tc.want)
		}
		if v, _ := rs.XComPull("check", "return_value"); v != (tc.want == "load") {
			t.Fatalf("return_value = %v", v)
		}
	}
	sel, err := runOne(t, d, "by_param", &memRun{})
	if err != nil || len(sel) != 1 || sel[0] != "full" {
		t.Fatalf("left template: %v %v", sel, err)
	}
}

func TestIfNodeOpsAndErrors(t *testing.T) {
	cases := []struct {
		got  any
		op   string
		want any
		ok   bool
	}{
		{"", "empty", nil, true}, {[]any{}, "empty", nil, true}, {map[string]any{"a": 1}, "not_empty", nil, true},
		{"paid", "==", "paid", true}, {3.0, ">=", "3", true}, {"abc", "contains", "b", true},
		{"abc", "not_contains", "b", false}, {nil, "exists", nil, false}, {nil, "not_exists", nil, true},
	}
	for _, c := range cases {
		if got := evalIf(c.got, c.op, c.want); got != c.ok {
			t.Errorf("%v %s %v = %v, want %v", c.got, c.op, c.want, got, c.ok)
		}
	}
	for yml, want := range map[string]string{
		"dag_id: e1\ntasks:\n  - {id: c, operator: branch, op: nope, then: [x]}\n  - {id: x, upstream: [c]}\n":  "unknown op",
		"dag_id: e2\ntasks:\n  - {id: c, operator: branch}\n":                                                   "true or false side",
		"dag_id: e3\ntasks:\n  - {id: c, operator: branch, then: [x], else: [x]}\n  - {id: x, upstream: [c]}\n": "both the true and the false",
	} {
		_, err := ParseYAML("e.yaml", []byte(yml))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v, want %q", yml, err, want)
		}
	}
}

func TestBuilderIfGraphRoundTrip(t *testing.T) {
	g := &Graph{DagID: "if_flow", Settings: map[string]any{},
		Nodes: []GNode{{ID: "src", Type: "bash", Config: map[string]any{"command": "echo '[1,2]'"}},
			{ID: "check", Type: "if", Config: map[string]any{"path": "length", "op": ">", "value": "0"}},
			{ID: "load", Type: "empty", Config: map[string]any{}},
			{ID: "alert", Type: "empty", Config: map[string]any{}},
			{ID: "done", Type: "empty", Config: map[string]any{}}},
		Edges: []GEdge{{From: "src", To: "check"}, {From: "check", To: "load", Branch: "true"},
			{From: "check", To: "alert", Branch: "false"}, {From: "load", To: "done"}, {From: "alert", To: "done"}}}
	out, iss := g.Validate()
	if out == nil {
		t.Fatalf("issues: %v", iss)
	}
	y := string(out)
	for _, want := range []string{"operator: branch", "then:\n      - load", "else:\n      - alert", "trigger_rule: none_failed_min_one_success"} {
		if !strings.Contains(y, want) {
			t.Fatalf("YAML misses %q:\n%s", want, y)
		}
	}
	back, err := ParseGraph(out)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{}
	for _, e := range back.Edges {
		if e.From == "check" {
			labels[e.To] = e.Branch
		}
	}
	if labels["load"] != "true" || labels["alert"] != "false" {
		t.Fatalf("edge labels after reload: %v", labels)
	}
	for _, n := range back.Nodes {
		if n.ID == "check" && (n.Type != "if" || n.Config["then"] != nil) {
			t.Fatalf("if node reloaded as %+v", n)
		}
	}
	// an unlabelled connection out of an If is an error
	g.Edges[1].Branch = ""
	if out, iss := g.Validate(); out != nil || !strings.Contains(issueText(iss), "true or false") {
		t.Fatalf("want a label error, got %v", iss)
	}
}

func issueText(iss []GraphIssue) string {
	var b strings.Builder
	for _, i := range iss {
		b.WriteString(i.Message + "\n")
	}
	return b.String()
}

func TestHTTPMaxBytesAndSaveTo(t *testing.T) {
	big := strings.Repeat("x", 3000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rows" {
			_, _ = w.Write([]byte(`[{"id":1,"orderId":"A"},{"id":2,"orderId":"B","extra":true}]`))
			return
		}
		_, _ = w.Write([]byte(big))
	}))
	defer srv.Close()
	dir := t.TempDir()
	old := UploadsDir
	UploadsDir = dir
	defer func() { UploadsDir = old }()
	yml := "dag_id: h\ntasks:\n" +
		"  - {id: small, operator: http, url: '" + srv.URL + "/big', max_bytes: 1000}\n" +
		"  - {id: ok, operator: http, url: '" + srv.URL + "/big', max_bytes: 5000}\n" +
		"  - {id: save, operator: http, url: '" + srv.URL + "/rows', save_to: 'rows_{{ .ds }}.json'}\n" +
		"  - {id: read, operator: file_read, file: 'rows_{{ .ds }}.json', upstream: [save]}\n"
	dags, err := ParseYAML("h.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	d := dags[0]
	rs := &memRun{}
	if _, err := runOne(t, d, "small", rs); err == nil || !strings.Contains(err.Error(), "larger than 1000 bytes") {
		t.Fatalf("want a size error, got %v", err)
	}
	if _, err := runOne(t, d, "ok", rs); err != nil {
		t.Fatal(err)
	}
	if v, _ := rs.XComPull("ok", "return_value"); v != big {
		t.Fatal("body should be complete")
	}
	if _, err := runOne(t, d, "save", rs); err != nil {
		t.Fatal(err)
	}
	out, _ := rs.XComPull("save", "return_value")
	m, _ := out.(map[string]any)
	if m["path"] != filepath.Join(dir, "rows_2026-01-31.json") || m["status_code"] != float64(200) {
		t.Fatalf("save_to output: %v", out)
	}
	if _, err := os.Stat(filepath.Join(dir, "rows_2026-01-31.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := runOne(t, d, "read", rs); err != nil {
		t.Fatal(err)
	}
	v, _ := rs.XComPull("read", "return_value")
	rows, _ := v.([]any)
	if len(rows) != 2 {
		t.Fatalf("rows: %v", v)
	}
	r2 := rows[1].(map[string]any)
	if r2["orderId"] != "B" || r2["extra"] != true || rows[0].(map[string]any)["extra"] != nil {
		t.Fatalf("JSON keys must be kept and missing ones empty: %v", rows)
	}
}

func TestReadJSONGridNDJSON(t *testing.T) {
	g, err := readJSONGrid([]byte("{\"a\":1}\n{\"b\":\"x\"}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(g) != 3 || g[0][0] != "a" || g[0][1] != "b" || g[2][1] != "x" {
		t.Fatalf("grid: %v", g)
	}
	if _, err := readJSONGrid([]byte("[1,2")); err == nil {
		t.Fatal("want an error for broken JSON")
	}
}
