package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dagsflow-go/dag"
	"dagsflow-go/engine"
)

type bld struct {
	t    *testing.T
	srv  *httptest.Server
	st   *engine.Store
	root string
	user string
}

func newBld(t *testing.T) *bld {
	root := t.TempDir()
	st := engine.NewStore(t.TempDir())
	s := &Server{Store: st, BuilderDir: filepath.Join(root, "builder")}
	s.Reload = func(p string) error {
		dag.InvalidateYAML(p)
		dag.LoadDagFolder(root)
		return nil
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		srv.Close()
		for src := range dag.Sources() {
			if strings.HasPrefix(src, root) {
				dag.ReplaceSource(src, nil, nil)
			}
		}
	})
	return &bld{t: t, srv: srv, st: st, root: root}
}

func (b *bld) do(method, path string, body any) (int, map[string]any) {
	var rd *bytes.Reader
	if body != nil {
		buf, _ := json.Marshal(body)
		rd = bytes.NewReader(buf)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, b.srv.URL+"/api/v1"+path, rd)
	req.Header.Set("X-Dagsflow", "1")
	req.Header.Set("Content-Type", "application/json")
	if b.user != "" {
		req.SetBasicAuth(b.user, "password123")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		b.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func graph(id string, schedule string) map[string]any {
	st := map[string]any{}
	if schedule != "" {
		st["schedule"] = schedule
	}
	return map[string]any{"dag_id": id, "settings": st,
		"nodes": []any{
			map[string]any{"id": "a", "type": "bash", "x": 0, "y": 0, "config": map[string]any{"command": "echo hi"}},
			map[string]any{"id": "b", "type": "empty", "x": 300, "y": 0, "config": map[string]any{}}},
		"edges": []any{map[string]any{"from": "a", "to": "b"}}}
}

func TestBuilderAPI(t *testing.T) {
	b := newBld(t)

	if c, r := b.do("GET", "/builder/nodes", nil); c != 200 || r["nodes"] == nil {
		t.Fatalf("nodes: %d %v", c, r)
	}
	// validate reports problems without saving
	bad := graph("bt_one", "")
	bad["nodes"].([]any)[0].(map[string]any)["config"] = map[string]any{}
	if c, r := b.do("POST", "/builder/validate", bad); c != 200 || r["ok"] != false {
		t.Fatalf("validate: %d %v", c, r)
	}
	if c, r := b.do("PUT", "/builder/workflows/bt_one", bad); c != 422 || !strings.Contains(r["error"].(string), "Command is required") {
		t.Fatalf("invalid save: %d %v", c, r)
	}
	if _, err := os.Stat(filepath.Join(b.root, "builder", "bt_one.yaml")); err == nil {
		t.Fatal("invalid workflow was written")
	}

	// create
	c, r := b.do("PUT", "/builder/workflows/bt_one", graph("ignored", ""))
	if c != 200 || r["created"] != true || r["loaded"] != true {
		t.Fatalf("create: %d %v", c, r)
	}
	etag := r["etag"].(string)
	if _, ok := dag.Get("bt_one"); !ok {
		t.Fatal("not loaded into the registry")
	}
	// same id again without etag = conflict; stale etag = conflict; right etag works
	if c, _ := b.do("PUT", "/builder/workflows/bt_one", graph("bt_one", "")); c != 409 {
		t.Fatalf("overwrite without etag: %d", c)
	}
	g := graph("bt_one", "")
	g["etag"] = "deadbeef"
	if c, _ := b.do("PUT", "/builder/workflows/bt_one", g); c != 409 {
		t.Fatalf("stale etag: %d", c)
	}
	g["etag"] = etag
	g["nodes"].([]any)[0].(map[string]any)["config"] = map[string]any{"command": "echo changed"}
	if c, r := b.do("PUT", "/builder/workflows/bt_one", g); c != 200 || r["etag"] == etag {
		t.Fatalf("update: %d %v", c, r)
	}
	// read it back
	c, r = b.do("GET", "/builder/workflows/bt_one", nil)
	if c != 200 || r["editable"] != true {
		t.Fatalf("get: %d %v", c, r)
	}
	gg := r["graph"].(map[string]any)
	if len(gg["nodes"].([]any)) != 2 || len(gg["edges"].([]any)) != 1 {
		t.Fatalf("graph: %v", gg)
	}
	// bad ids never touch the file system outside the folder
	for _, id := range []string{"..%2Fevil", "a%2Fb", ".hidden", "x y"} {
		if c, _ := b.do("PUT", "/builder/workflows/"+id, graph("x", "")); c == 200 {
			t.Fatalf("id %q accepted", id)
		}
	}
	if entries, _ := os.ReadDir(b.root); len(entries) != 1 || entries[0].Name() != "builder" {
		t.Fatalf("files outside builder dir: %v", entries)
	}
	// a hand-written YAML DAG opens read-only and cannot be overwritten
	hand := "dag_id: hand_made\ntasks:\n  - id: t\n    operator: bash\n    command: echo 1\n"
	if err := os.WriteFile(filepath.Join(b.root, "hand.yaml"), []byte(hand), 0o644); err != nil {
		t.Fatal(err)
	}
	dag.LoadDagFolder(b.root)
	if c, r := b.do("GET", "/builder/workflows/hand_made", nil); c != 200 || r["editable"] != false || r["graph"].(map[string]any)["needs_layout"] != true {
		t.Fatalf("hand made: %d %v", c, r)
	}
	if c, _ := b.do("PUT", "/builder/workflows/hand_made", graph("hand_made", "")); c != 409 {
		t.Fatalf("overwrote a hand-written DAG: %d", c)
	}
	if c, _ := b.do("DELETE", "/builder/workflows/hand_made", nil); c != 409 {
		t.Fatalf("deleted a hand-written DAG: %d", c)
	}
	// list
	c, r = b.do("GET", "/builder/workflows", nil)
	if c != 200 || len(r["workflows"].([]any)) != 2 {
		t.Fatalf("list: %d %v", c, r)
	}
	// a broken file in the builder folder is listed with its error
	_ = os.WriteFile(filepath.Join(b.root, "builder", "broken.yaml"), []byte("dag_id: broken\ntasks:\n  - id: x\n    operator: nope\n"), 0o644)
	dag.LoadDagFolder(b.root)
	_, r = b.do("GET", "/builder/workflows", nil)
	found := false
	for _, w := range r["workflows"].([]any) {
		if m := w.(map[string]any); m["dag_id"] == "broken" && m["error"] != nil {
			found = true
		}
	}
	if !found {
		t.Fatalf("broken workflow not listed: %v", r)
	}
	// delete
	if c, _ := b.do("DELETE", "/builder/workflows/bt_one", nil); c != 200 {
		t.Fatalf("delete: %d", c)
	}
	if _, ok := dag.Get("bt_one"); ok {
		t.Fatal("still registered after delete")
	}
	if c, _ := b.do("DELETE", "/builder/workflows/bt_one", nil); c != 404 {
		t.Fatalf("delete twice: %d", c)
	}
}

func TestBuilderScheduledStartsPaused(t *testing.T) {
	b := newBld(t)
	c, r := b.do("PUT", "/builder/workflows/bt_sched", graph("bt_sched", "@daily"))
	if c != 200 || r["paused"] != true {
		t.Fatalf("%d %v", c, r)
	}
	if !b.st.GetDagMeta("bt_sched").IsPaused {
		t.Fatal("not paused in the store")
	}
}

func TestBuilderRBAC(t *testing.T) {
	b := newBld(t)
	must := func(err error) {
		t.Helper()
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
	must(b.st.CreateUser("viewer", "password123", "Viewer"))
	must(b.st.CreateRole(engine.RoleDef{Name: "sales_builder", Permissions: []engine.Permission{
		perm("builder.edit"), perm("dag.edit:sales_*"), perm("dag.delete:sales_*")}}))
	must(b.st.CreateUser("sam", "password123", "sales_builder"))

	if c, _ := b.do("GET", "/builder/nodes", nil); c != 401 {
		t.Fatalf("anonymous: %d", c)
	}
	b.user = "viewer"
	for _, p := range []string{"/builder/nodes", "/builder/workflows"} {
		if c, _ := b.do("GET", p, nil); c != 403 {
			t.Fatalf("viewer %s: %d", p, c)
		}
	}
	if c, _ := b.do("PUT", "/builder/workflows/anything", graph("x", "")); c != 403 {
		t.Fatalf("viewer save: %d", c)
	}

	b.user = "sam"
	if c, r := b.do("PUT", "/builder/workflows/sales_daily", graph("x", "")); c != 200 {
		t.Fatalf("sam own dag: %d %v", c, r)
	}
	if c, _ := b.do("PUT", "/builder/workflows/hr_daily", graph("x", "")); c != 403 {
		t.Fatalf("sam outside pattern: %d", c)
	}
	b.user = "root"
	if c, _ := b.do("PUT", "/builder/workflows/hr_daily", graph("x", "")); c != 200 {
		t.Fatalf("root: %d", c)
	}
	b.user = "sam"
	if c, _ := b.do("GET", "/builder/workflows/hr_daily", nil); c != 403 {
		t.Fatalf("sam reads hr: %d", c)
	}
	if c, _ := b.do("DELETE", "/builder/workflows/hr_daily", nil); c != 403 {
		t.Fatalf("sam deletes hr: %d", c)
	}
	_, r := b.do("GET", "/builder/workflows", nil)
	for _, w := range r["workflows"].([]any) {
		if w.(map[string]any)["dag_id"] == "hr_daily" {
			t.Fatal("list leaks a DAG the user cannot read")
		}
	}
	if c, _ := b.do("DELETE", "/builder/workflows/sales_daily", nil); c != 200 {
		t.Fatalf("sam deletes own: %d", c)
	}
}
