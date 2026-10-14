package dag

import (
	"strings"
	"testing"
)

func TestRegisterNodeTypeGo(t *testing.T) {
	called := ""
	err := RegisterNodeType(NodeType{Type: "go_ping", Label: "Go ping",
		Fields: []NodeField{{Name: "target", Label: "Target", Kind: "text", Required: true}}},
		func(d *DAG, id string, with map[string]any, opts ...TaskOption) error {
			called = id + ":" + str(with["target"])
			d.NewEmptyTask(id, opts...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterNodeType(NodeType{Type: "bash", Label: "x"}, func(*DAG, string, map[string]any, ...TaskOption) error { return nil }); err == nil {
		t.Fatal("built-in name must be refused")
	}
	g := &Graph{DagID: "gp", Nodes: []GNode{{ID: "p", Type: "go_ping", Config: map[string]any{"target": "db"}}}}
	y, iss := g.Validate()
	if y == nil {
		t.Fatalf("validate: %v", iss)
	}
	if !strings.Contains(string(y), "node: go_ping") {
		t.Fatal(string(y))
	}
	if called != "p:db" {
		t.Fatalf("Build not called: %q", called)
	}
	g2, err := ParseGraph(y)
	if err != nil || g2.Nodes[0].Type != "go_ping" || g2.Nodes[0].Config["target"] != "db" {
		t.Fatalf("parse back: %v %+v", err, g2)
	}
	g.Nodes[0].Config = map[string]any{}
	if y, _ := g.Validate(); y != nil {
		t.Fatal("required field must be enforced")
	}
}

func TestCustomTemplateTypes(t *testing.T) {
	d := &CustomNodeDef{Type: "pg_count", Label: "Count rows", Operator: "postgres",
		Fields:   []NodeField{{Name: "table", Label: "Table", Kind: "text", Required: true}, {Name: "cap", Label: "Cap", Kind: "number"}},
		Template: map[string]any{"conn_id": "main", "sql": "select count(*) from ${table}", "limit": "${cap}"}}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	yt, err := d.expand(map[string]any{"table": "orders", "cap": 5}, false)
	if err != nil || yt.SQL != "select count(*) from orders" || yt.Limit != 5 {
		t.Fatalf("%+v %v", yt, err)
	}
	yt, _ = d.expand(map[string]any{"table": "orders"}, false)
	if yt.Limit != 0 {
		t.Fatal("empty placeholder must drop the key")
	}
	if _, err := d.expand(nil, false); err == nil {
		t.Fatal("required not enforced")
	}
}

func TestMissingCustomNodeStillOpens(t *testing.T) {
	y := []byte("dag_id: miss\ntasks:\n  - id: a\n    operator: custom\n    node: gone_type\n    with: {x: 1}\n  - id: b\n    operator: empty\n    upstream: [a]\nlayout:\n  a: [0, 0]\n  b: [300, 0]\n")
	g, err := ParseGraph(y)
	if err != nil {
		t.Fatal(err)
	}
	if g.Nodes[0].Type != "gone_type" || g.Nodes[0].Config["x"] != 1 {
		t.Fatalf("%+v", g.Nodes[0])
	}
	out, iss := g.Validate()
	if out != nil || len(iss) == 0 || !strings.Contains(iss[0].Message, "not defined") {
		t.Fatalf("must block saving: %v", iss)
	}
	g.Nodes, g.Edges = g.Nodes[1:], nil
	if out, iss := g.Validate(); out == nil {
		t.Fatalf("after removing the node it must save: %v", iss)
	}
}
