package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dagsflow-go/engine"
)

func node(id, typ string, cfg map[string]any) map[string]any {
	return map[string]any{"id": id, "type": typ, "x": 0, "y": 0, "config": cfg}
}

func edge(from, to string, branch ...string) map[string]any {
	e := map[string]any{"from": from, "to": to}
	if len(branch) > 0 {
		e["branch"] = branch[0]
	}
	return e
}

func TestBuilderRunNode(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":1,"st":"paid"},{"id":2,"st":"open"},{"id":3,"st":"paid"}]}`))
	}))
	defer api.Close()
	b := newBld(t)
	g := map[string]any{"dag_id": "prev_flow", "settings": map[string]any{},
		"nodes": []any{
			node("fetch", "http", map[string]any{"url": api.URL}),
			node("paid", "transform", map[string]any{"steps": []any{
				map[string]any{"op": "extract", "path": "data"},
				map[string]any{"op": "filter", "field": "st", "cmp": "==", "value": "paid"}}}),
			node("check", "if", map[string]any{"path": "length", "op": ">", "value": "1"}),
			node("load", "empty", map[string]any{}),
			node("alert", "empty", map[string]any{}),
			node("broken", "postgres_insert", map[string]any{}), // other nodes may be unfinished
			node("sh", "bash", map[string]any{"command": "echo '[{\"x\":1}]'"}),
			node("after_sh", "transform", map[string]any{"steps": []any{}}),
		},
		"edges": []any{edge("fetch", "paid"), edge("paid", "check"), edge("check", "load", "true"),
			edge("check", "alert", "false"), edge("sh", "after_sh")}}

	// a source: its own output
	c, r := b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "fetch"})
	if c != 200 || r["status"] != "success" {
		t.Fatalf("fetch: %d %v", c, r)
	}
	// a transform: the read-only source before it runs automatically
	c, r = b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "paid"})
	if c != 200 || r["status"] != "success" {
		t.Fatalf("paid: %d %v", c, r)
	}
	rows, _ := r["output"].([]any)
	if len(rows) != 2 || r["rows_total"] != float64(2) {
		t.Fatalf("paid rows: %v", r["output"])
	}
	if ran, _ := r["ran"].([]any); len(ran) != 1 || ran[0] != "fetch" {
		t.Fatalf("ran: %v", r["ran"])
	}
	// the If node reports the selected side
	c, r = b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "check"})
	if sel, _ := r["selected"].([]any); c != 200 || len(sel) != 1 || sel[0] != "load" {
		t.Fatalf("check: %d %v", c, r)
	}
	// outputs kept by the UI are used instead of running the source again
	c, r = b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "check",
		"outputs": map[string]any{"paid": []any{map[string]any{"id": 9}}}})
	if sel, _ := r["selected"].([]any); c != 200 || len(sel) != 1 || sel[0] != "alert" || len(r["ran"].([]any)) != 0 {
		t.Fatalf("check with outputs: %d %v", c, r)
	}
	// a shell node is not run automatically to get an input
	c, r = b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "after_sh"})
	if c != 409 || !strings.Contains(r["error"].(string), "sh has no output yet") {
		t.Fatalf("after_sh: %d %v", c, r)
	}
	// ... but it runs when it is the node asked for
	c, r = b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "sh"})
	if c != 200 || r["status"] != "success" || !strings.Contains(r["log"].(string), "[{\"x\":1}]") {
		t.Fatalf("sh: %d %v", c, r)
	}
	// a failing node returns its error and log, not a 500
	c, r = b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "broken",
		"outputs": map[string]any{}})
	if c != 400 && (c != 200 || r["status"] != "failed") {
		t.Fatalf("broken: %d %v", c, r)
	}
	if c, _ := b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "nope"}); c != 400 {
		t.Fatalf("unknown node: %d", c)
	}
	// nothing was stored
	if runs, _ := b.st.ListRuns("prev_flow"); len(runs) != 0 {
		t.Fatalf("a preview must not create runs: %d", len(runs))
	}
}

func TestBuilderRunNodePermissions(t *testing.T) {
	b := newBld(t)
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	perm := func(s string) engine.Permission {
		p, err := engine.ParsePermission(s)
		must(err)
		return p
	}
	must(b.st.CreateUser("root", "password123", "Admin"))
	must(b.st.CreateRole(engine.RoleDef{Name: "editor_only", Permissions: []engine.Permission{
		perm("builder.edit"), perm("dag.edit:sales_*")}}))
	must(b.st.CreateUser("ed", "password123", "editor_only"))
	g := graph("sales_x", "")
	b.user = "ed"
	if c, _ := b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "a"}); c != 403 {
		t.Fatalf("edit without trigger: %d", c)
	}
	b.user = "root"
	if c, r := b.do("POST", "/builder/run-node", map[string]any{"graph": g, "node": "a"}); c != 200 || r["status"] != "success" {
		t.Fatalf("admin: %d %v", c, r)
	}
}
