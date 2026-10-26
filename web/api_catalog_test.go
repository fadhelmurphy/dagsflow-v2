package web

import (
	"testing"

	"dagsflow-go/engine"
)

func TestCatalogRBAC(t *testing.T) {
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
	// sheet -> node rows (dag sales_load) -> mart table -> hr table
	must(b.st.UpdateCatalog(engine.CatalogUpdate{
		Tables: []engine.CatalogTable{
			{ID: "bigquery:p.mart.orders", Columns: []engine.CatalogColumn{{Name: "a"}}},
			{ID: "bigquery:p.hr.salary", Columns: []engine.CatalogColumn{{Name: "a"}}},
		},
		Edges: []engine.LineageEdge{
			{From: "gsheet:s!T", FromCol: "a", To: "task:sales_load.read", ToCol: "a", Via: "sales_load.read", Kind: "dagsflow"},
			{From: "task:sales_load.read", FromCol: "a", To: "bigquery:p.mart.orders", ToCol: "a", Via: "sales_load.write", Kind: "dagsflow"},
			{From: "bigquery:p.mart.orders", FromCol: "a", To: "bigquery:p.hr.salary", ToCol: "a", Via: "hr_dag.x", Kind: "dagsflow"},
		},
	}))
	must(b.st.CreateUser("viewer", "password123", "Viewer"))
	must(b.st.CreateRole(engine.RoleDef{Name: "mart_only", Permissions: []engine.Permission{perm("catalog.read:bigquery:p.mart.*"), perm("dag.read:sales_*")}}))
	must(b.st.CreateUser("ann", "password123", "mart_only"))
	must(b.st.CreateRole(engine.RoleDef{Name: "no_catalog", Permissions: []engine.Permission{perm("dag.read:*")}}))
	must(b.st.CreateUser("bob", "password123", "no_catalog"))

	b.user = "viewer"
	if c, r := b.do("GET", "/catalog", nil); c != 200 || len(r["tables"].([]any)) != 3 {
		t.Fatalf("viewer: %d %v", c, r)
	}
	b.user = "bob"
	if c, _ := b.do("GET", "/catalog", nil); c != 403 {
		t.Fatalf("no catalog.read: %d", c)
	}
	b.user = "ann"
	_, r := b.do("GET", "/catalog", nil)
	if l := r["tables"].([]any); len(l) != 1 || l[0].(map[string]any)["id"] != "bigquery:p.mart.orders" {
		t.Fatalf("ann list: %v", r)
	}
	if c, _ := b.do("GET", "/catalog/table?id=bigquery:p.hr.salary", nil); c != 403 {
		t.Fatalf("ann outside pattern: %d", c)
	}
	_, r = b.do("GET", "/catalog/table?id=bigquery:p.mart.orders", nil)
	if up := r["upstream"].([]any); len(up) != 1 || up[0].(map[string]any)["via"] != "sales_load.write" {
		t.Fatalf("ann upstream: %v", r)
	}
	if down := r["downstream"].([]any); len(down) != 0 {
		t.Fatalf("hr table must stay hidden: %v", down)
	}
	_, r = b.do("GET", "/catalog/lineage?id=bigquery:p.mart.orders&column=a", nil)
	up, down := r["upstream"].([]any), r["downstream"].([]any)
	if len(up) != 1 || up[0].(map[string]any)["dataset"] != "task:sales_load.read" || len(down) != 0 {
		t.Fatalf("ann lineage (the sheet and the hr table are hidden): %v", r)
	}
}
