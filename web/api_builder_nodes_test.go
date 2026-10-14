package web

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dagsflow-go/dag"
)

func TestCustomNodeAPI(t *testing.T) {
	b := newBld(t)
	def := map[string]any{"label": "Say hello", "operator": "bash",
		"fields":   []any{map[string]any{"name": "who", "label": "Who", "kind": "text", "required": true}},
		"template": map[string]any{"command": "echo hello ${who} ${HOME}"}}

	if c, _ := b.do("PUT", "/builder/custom-nodes/Bad-Name", def); c != 422 {
		t.Fatalf("bad type name: %d", c)
	}
	bad := map[string]any{"label": "x", "operator": "nope", "template": map[string]any{}}
	if c, _ := b.do("PUT", "/builder/custom-nodes/say_hello", bad); c != 422 {
		t.Fatalf("bad operator: %d", c)
	}
	if c, o := b.do("PUT", "/builder/custom-nodes/say_hello", def); c != 200 || o["created"] != true {
		t.Fatalf("create: %d %v", c, o)
	}
	if _, err := os.Stat(filepath.Join(b.root, "builder", "nodes", "say_hello.yaml")); err != nil {
		t.Fatal(err)
	}
	_, o := b.do("GET", "/builder/nodes", nil)
	found := false
	for _, n := range o["nodes"].([]any) {
		if n.(map[string]any)["type"] == "say_hello" {
			found = true
		}
	}
	if !found {
		t.Fatal("custom node missing from palette")
	}

	g := map[string]any{"dag_id": "cn1", "settings": map[string]any{},
		"nodes": []any{map[string]any{"id": "h", "type": "say_hello", "x": 0, "y": 0,
			"config": map[string]any{"who": "world", "retries": 2}}}}
	if c, o := b.do("POST", "/builder/validate", map[string]any{"dag_id": "cn1", "settings": map[string]any{},
		"nodes": []any{map[string]any{"id": "h", "type": "say_hello", "config": map[string]any{}}}}); c != 200 || o["ok"] != false {
		t.Fatalf("missing required field must fail: %v", o)
	}
	if c, o := b.do("PUT", "/builder/workflows/cn1", g); c != 200 {
		t.Fatalf("save: %d %v", c, o)
	}
	data, _ := os.ReadFile(filepath.Join(b.root, "builder", "cn1.yaml"))
	for _, want := range []string{"operator: custom", "node: say_hello", "who: world", "retries: 2"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("yaml lacks %q:\n%s", want, data)
		}
	}
	d, ok := dag.Get("cn1")
	if !ok || len(d.Tasks()) != 1 {
		t.Fatal("dag not loaded")
	}
	_, o = b.do("GET", "/builder/workflows/cn1", nil)
	n := o["graph"].(map[string]any)["nodes"].([]any)[0].(map[string]any)
	cfg := n["config"].(map[string]any)
	if n["type"] != "say_hello" || cfg["who"] != "world" {
		t.Fatalf("round trip: %v", n)
	}
	if c, _ := b.do("DELETE", "/builder/custom-nodes/say_hello", nil); c != 409 {
		t.Fatalf("delete while used: %d", c)
	}
	if _, o := b.do("GET", "/builder/custom-nodes/say_hello/usage", nil); len(o["used_by"].([]any)) != 1 {
		t.Fatalf("usage: %v", o)
	}
	// second workflow: custom node + a normal node connected to it
	g2 := map[string]any{"dag_id": "cn2", "settings": map[string]any{},
		"nodes": []any{map[string]any{"id": "h", "type": "say_hello", "config": map[string]any{"who": "x"}},
			map[string]any{"id": "keep", "type": "empty", "x": 300, "config": map[string]any{}}},
		"edges": []any{map[string]any{"from": "h", "to": "keep"}}}
	if c, o := b.do("PUT", "/builder/workflows/cn2", g2); c != 200 {
		t.Fatalf("save cn2: %d %v", c, o)
	}
	c, o := b.do("DELETE", "/builder/custom-nodes/say_hello?force=1", nil)
	if c != 200 {
		t.Fatalf("forced delete: %d %v", c, o)
	}
	if _, err := os.Stat(filepath.Join(b.root, "builder", "cn1.yaml")); err == nil {
		t.Fatal("workflow left empty must be deleted")
	}
	data, _ = os.ReadFile(filepath.Join(b.root, "builder", "cn2.yaml"))
	if strings.Contains(string(data), "say_hello") || !strings.Contains(string(data), "id: keep") || strings.Contains(string(data), "upstream") {
		t.Fatalf("cn2 not cleaned:\n%s", data)
	}
	if _, err := os.Stat(filepath.Join(b.root, "builder", "nodes", "say_hello.yaml")); err == nil {
		t.Fatal("definition file must be gone")
	}
	b.do("DELETE", "/builder/workflows/cn2", nil)
}
