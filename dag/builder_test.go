package dag

import (
	"strings"
	"testing"
)

func sampleGraph() *Graph {
	return &Graph{
		DagID:    "orders_etl",
		Settings: map[string]any{"schedule": "@daily", "tags": []any{"etl"}, "catchup": false, "description": ""},
		Nodes: []GNode{
			{ID: "fetch", Type: "http", X: 10, Y: 20, Config: map[string]any{"method": "GET", "url": "https://x.test/orders?d={{ .ds }}", "retries": float64(2)}},
			{ID: "shape", Type: "transform", X: 300, Y: 20, Config: map[string]any{"steps": []any{
				map[string]any{"op": "extract", "path": "data"},
				map[string]any{"op": "select", "fields": []any{"id", "total"}},
			}}},
			{ID: "load", Type: "postgres_insert", X: 600, Y: 20, Config: map[string]any{"conn_id": "wh", "table": "public.orders", "on_conflict": "do_nothing"}},
		},
		Edges: []GEdge{{"fetch", "shape"}, {"shape", "load"}},
	}
}

func TestBuilderRoundTrip(t *testing.T) {
	g := sampleGraph()
	y, iss := g.Validate()
	for _, i := range iss {
		if !i.Warning {
			t.Fatalf("unexpected issue: %+v", i)
		}
	}
	if y == nil {
		t.Fatal("no yaml")
	}
	s := string(y)
	for _, want := range []string{"dag_id: orders_etl", "operator: postgres", "upstream:", "layout:", "retries: 2"} {
		if !strings.Contains(s, want) {
			t.Errorf("yaml lacks %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "catchup") || strings.Contains(s, "description") {
		t.Errorf("empty values were written:\n%s", s)
	}
	dags, err := ParseYAML("x.yaml", y)
	if err != nil || len(dags) != 1 || len(dags[0].taskMap) != 3 {
		t.Fatalf("parse: %v", err)
	}
	back, err := ParseGraph(y)
	if err != nil {
		t.Fatal(err)
	}
	if back.NeedsLayout || len(back.Nodes) != 3 || len(back.Edges) != 2 {
		t.Fatalf("round trip: %+v", back)
	}
	if back.Nodes[2].Type != "postgres_insert" || back.Nodes[0].Type != "http" || back.Nodes[1].X != 300 {
		t.Fatalf("types/positions lost: %+v", back.Nodes)
	}
	y2, iss2 := back.Validate()
	if hardErrors(iss2) || string(y2) != string(y) {
		t.Fatalf("not stable:\n%s\n---\n%s", y, y2)
	}
}

func TestBuilderValidation(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Graph)
		want string
	}{
		{"cycle", func(g *Graph) { g.Edges = append(g.Edges, GEdge{"load", "fetch"}) }, "loop"},
		{"self", func(g *Graph) { g.Edges = append(g.Edges, GEdge{"fetch", "fetch"}) }, "itself"},
		{"dup id", func(g *Graph) { g.Nodes[1].ID = "fetch" }, "duplicate"},
		{"bad dag id", func(g *Graph) { g.DagID = "../evil" }, "DAG id"},
		{"no input", func(g *Graph) { g.Edges = g.Edges[:1] }, "provides the rows"},
		{"missing url", func(g *Graph) { delete(g.Nodes[0].Config, "url") }, "URL is required"},
		{"bad duration", func(g *Graph) { g.Nodes[0].Config["retry_delay"] = "soon" }, "retry_delay"},
		{"unknown field", func(g *Graph) { g.Nodes[0].Config["bogus"] = "x" }, "unknown setting"},
		{"bad step", func(g *Graph) { g.Nodes[1].Config["steps"] = []any{map[string]any{"op": "filter", "field": "a"}} }, "comparison"},
		{"bad schedule", func(g *Graph) { g.Settings["schedule"] = "whenever" }, "schedule"},
		{"bad table", func(g *Graph) { g.Nodes[2].Config["table"] = "a.b.c" }, "schema.table"},
	}
	for _, c := range cases {
		g := sampleGraph()
		c.mut(g)
		_, iss := g.Validate()
		found := false
		for _, i := range iss {
			if !i.Warning && strings.Contains(i.Message, c.want) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: want issue containing %q, got %+v", c.name, c.want, iss)
		}
	}
}

func TestParseGraphLegacyYAML(t *testing.T) {
	g, err := ParseGraph([]byte("dag_id: a\nschedule: '@daily'\ntasks:\n  - id: t1\n    operator: bash\n    command: echo hi\n  - id: t2\n    upstream: [t1]\n"))
	if err != nil || !g.NeedsLayout || len(g.Nodes) != 2 || g.Nodes[1].Type != "empty" || g.Settings["schedule"] != "@daily" {
		t.Fatalf("%v %+v", err, g)
	}
	if _, err := ParseGraph([]byte("dag_id: a\n---\ndag_id: b\n")); err == nil {
		t.Fatal("multi-doc accepted")
	}
	if _, err := ParseGraph([]byte("dag_id: a\ntasks:\n  - id: x\n    operator: nope\n")); err == nil {
		t.Fatal("unknown operator accepted")
	}
}

func TestTransformSteps(t *testing.T) {
	in := `{"data":[{"id":1,"name":"a","total":"10","st":"paid"},{"id":2,"name":"b","total":5,"st":"open"},{"id":3,"name":"c","total":20,"st":"paid"}]}`
	steps := []TransformStep{
		{Op: "extract", Path: "data"},
		{Op: "filter", Field: "st", Cmp: "==", Value: "paid"},
		{Op: "filter", Field: "total", Cmp: ">", Value: float64(5)},
		{Op: "set", Field: "src", Value: "web-{{ .row.name }}"},
		{Op: "rename", Map: map[string]string{"total": "amount"}},
		{Op: "select", Fields: []string{"id", "amount", "src"}},
		{Op: "sort", Field: "amount", Desc: true},
		{Op: "limit", N: 5},
	}
	out, err := ApplyTransform(in, steps, func(raw string, row map[string]any) (string, error) {
		return strings.ReplaceAll(raw, "{{ .row.name }}", row["name"].(string)), nil
	})
	if err != nil || len(out) != 2 {
		t.Fatalf("%v %v", err, out)
	}
	first := out[0].(map[string]any)
	if first["id"] != float64(3) || first["src"] != "web-c" || first["amount"] != float64(20) || len(first) != 3 {
		t.Fatalf("%v", first)
	}
	if _, err := ApplyTransform("not json", steps, nil); err == nil || !strings.Contains(err.Error(), "valid JSON") {
		t.Fatalf("want json error, got %v", err)
	}
	dd, _ := ApplyTransform([]any{map[string]any{"a": 1.0}, map[string]any{"a": 1.0}, map[string]any{"a": 2.0}}, []TransformStep{{Op: "dedupe", Fields: []string{"a"}}}, nil)
	if len(dd) != 2 {
		t.Fatalf("dedupe: %v", dd)
	}
}

func TestParseGraphDrawsExpandEdge(t *testing.T) {
	g, err := ParseGraph([]byte("dag_id: a\ntasks:\n  - id: list\n    operator: bash\n    command: echo\n  - id: each\n    operator: bash\n    command: echo\n    expand: {xcom: {task: list}}\n"))
	if err != nil || len(g.Edges) != 1 || g.Edges[0] != (GEdge{From: "list", To: "each"}) {
		t.Fatalf("%v %+v", err, g)
	}
	y, iss := g.Validate()
	if hardErrors(iss) || !strings.Contains(string(y), "upstream:") {
		t.Fatalf("%v\n%s", iss, y)
	}
}

func TestBuilderGoogleNodes(t *testing.T) {
	g := Graph{DagID: "g_etl", Nodes: []GNode{
		{ID: "r", Type: "bigquery_query", Config: map[string]any{"sql": "select 1"}},
		{ID: "w", Type: "gsheet_write", Config: map[string]any{"spreadsheet_id": "S", "range": "Sheet1", "columns": []any{"a", "b"}}},
	}, Edges: []GEdge{{From: "r", To: "w"}}}
	y, iss := g.Validate()
	for _, i := range iss {
		if !i.Warning {
			t.Fatalf("%v", iss)
		}
	}
	back, err := ParseGraph(y)
	if err != nil {
		t.Fatal(err)
	}
	if back.Nodes[0].Type != "bigquery_query" || back.Nodes[1].Type != "gsheet_write" || len(back.Edges) != 1 {
		t.Fatalf("%+v", back)
	}
	g.Nodes[1].Config = map[string]any{"range": "x"}
	_, iss = g.Validate()
	bad := false
	for _, i := range iss {
		bad = bad || !i.Warning
	}
	if !bad {
		t.Fatal("expected a missing-field error")
	}
}
