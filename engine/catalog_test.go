package engine

import (
	"testing"
	"time"

	"dagsflow-go/dag"
)

func colNames(t *CatalogTable, live bool) []string {
	var out []string
	for _, c := range t.Columns {
		if !live || c.Removed == nil {
			out = append(out, c.Name)
		}
	}
	return out
}

// A table's columns, schema changes and drop come from successive scans.
func TestCatalogSchemaChangesAndDrop(t *testing.T) { catalogSchemaChanges(t, NewStore(t.TempDir())) }

func TestPGCatalog(t *testing.T) { resetPG(t); catalogSchemaChanges(t, pgStore(t)) }

func catalogSchemaChanges(t *testing.T, st *Store) {
	scope := "bigquery:p:us"
	tbl := func(id string, cols ...string) CatalogTable {
		ct := CatalogTable{ID: id, Source: "scan", Scope: scope, Columns: []CatalogColumn{}}
		for _, c := range cols {
			ct.Columns = append(ct.Columns, CatalogColumn{Name: c, Type: "STRING"})
		}
		return ct
	}
	if err := st.UpdateCatalog(CatalogUpdate{FullScope: scope, Tables: []CatalogTable{tbl("bigquery:p.d.a", "id", "name"), tbl("bigquery:p.d.b", "x")}}); err != nil {
		t.Fatal(err)
	}
	st.dropCatalogCache()
	a2 := tbl("bigquery:p.d.a", "id", "email")
	a2.Columns[0].Type = "INT64"
	if err := st.UpdateCatalog(CatalogUpdate{FullScope: scope, Tables: []CatalogTable{a2}}); err != nil {
		t.Fatal(err)
	}
	a, _, _, err := st.CatalogTable("bigquery:p.d.a")
	if err != nil || a == nil {
		t.Fatal(a, err)
	}
	if got := colNames(a, true); len(got) != 2 || got[0] != "id" || got[1] != "email" {
		t.Fatalf("live columns %v", got)
	}
	if len(a.Changes) != 1 || a.Changes[0].Added[0] != "email" || a.Changes[0].Removed[0] != "name" || a.Changes[0].Retyped["id"] != "STRING -> INT64" {
		t.Fatalf("changes %+v", a.Changes)
	}
	b, _, _, _ := st.CatalogTable("bigquery:p.d.b")
	if b.Dropped == nil {
		t.Fatal("b should be marked dropped")
	}
	if got := st.CatalogColumnNames("BIGQUERY:p.d.a"); len(got) != 2 {
		t.Fatalf("column names %v", got)
	}
}

// Lineage reported by nodes is stored when the task succeeds, chained through
// the rows passed between nodes, and walked up and down by column.
func TestTaskLineageRecordedOnSuccess(t *testing.T) { taskLineage(t, NewStore(t.TempDir())) }

func TestPGTaskLineage(t *testing.T) { resetPG(t); taskLineage(t, pgStore(t)) }

func taskLineage(t *testing.T, st *Store) {
	d := dag.NewDAG("lin_dag", "")
	read := d.NewTask("read", func(ctx *dag.TaskContext) error {
		ctx.ReportLineage(dag.Lineage{
			Datasets: []dag.LineageDataset{{ID: "gsheet:abc!Sheet1", Kind: "sheet", Columns: []dag.LineageColumn{{Name: "Nama"}}}},
			Edges:    []dag.LineageLink{{From: "gsheet:abc!Sheet1", FromCol: "Nama", To: "task:lin_dag.read", ToCol: "name"}},
		})
		return ctx.XComPush("return_value", []any{map[string]any{"name": "x"}})
	})
	write := d.NewTask("load", func(ctx *dag.TaskContext) error {
		ctx.ReportLineage(dag.Lineage{
			Datasets: []dag.LineageDataset{{ID: "bigquery:p.d.people", Columns: []dag.LineageColumn{{Name: "full_name", Type: "STRING"}}}},
			Edges:    []dag.LineageLink{{From: "task:lin_dag.read", FromCol: "name", To: "bigquery:p.d.people", ToCol: "full_name", Expr: "UPPER(name)"}},
		})
		return nil
	})
	read.Then(write)
	dag.Register(d)
	now := time.Now().UTC()
	r := NewDagRun(d, RunTypeManual, "", now, now, now, nil)
	runOnce(t, st, r)
	if r.State != RunSuccess {
		t.Fatalf("run %s", r.State)
	}
	if _, ok := r.XCom["read"][dag.LineageXCom]; ok {
		t.Fatal("the lineage XCom should be removed once stored")
	}
	tb, up, _, err := st.CatalogTable("bigquery:p.d.people")
	if err != nil || tb == nil || len(up) != 1 || up[0].Via != "lin_dag.load" || tb.Writers[0] != "lin_dag.load" {
		t.Fatalf("table %+v up %+v err %v", tb, up, err)
	}
	nodes, err := st.ColumnLineage("bigquery:p.d.people", "full_name", true, 10)
	if err != nil || len(nodes) != 2 || nodes[1].Dataset != "gsheet:abc!Sheet1" || nodes[1].Column != "Nama" || nodes[0].Expr != "UPPER(name)" {
		t.Fatalf("upstream %+v %v", nodes, err)
	}
	down, _ := st.ColumnLineage("gsheet:abc!Sheet1", "", false, 10)
	if len(down) != 2 || down[1].Dataset != "bigquery:p.d.people" {
		t.Fatalf("downstream %+v", down)
	}
}

// Sync scans go through the hook the engine installs.
func TestCatalogSinkScan(t *testing.T) { catalogSinkScan(t, NewStore(t.TempDir())) }

func TestPGCatalogSinkScan(t *testing.T) { resetPG(t); catalogSinkScan(t, pgStore(t)) }

func catalogSinkScan(t *testing.T, st *Store) {
	st.InstallCatalogHooks()
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	err := dag.Catalog.SaveCatalogScan(dag.CatalogScan{Scope: "redshift:wg:dev", FullScope: true,
		Tables: []dag.ScannedTable{
			{ID: "redshift:dev.public.orders", Kind: "table", Columns: []dag.LineageColumn{{Name: "id", Type: "integer"}}},
			{ID: "redshift:dev.public.orders", Creator: "alice", CreatedAt: &created},
		},
		Edges: []dag.ScannedEdge{{LineageLink: dag.LineageLink{From: "redshift:dev.stg.o", FromCol: "id", To: "redshift:dev.public.orders", ToCol: "id"}, Via: "query 1 by alice", At: created}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tb, up, _, _ := st.CatalogTable("redshift:dev.public.orders")
	if tb == nil || tb.Creator != "alice" || len(tb.Columns) != 1 || tb.Kind != "table" || len(up) != 1 || up[0].Kind != "query" {
		t.Fatalf("%+v %+v", tb, up)
	}
	if stub, _, _, _ := st.CatalogTable("redshift:dev.stg.o"); stub == nil {
		t.Fatal("source table known only from a query should be listed")
	}
	if err := dag.Catalog.SetCatalogSyncState("s", created); err != nil {
		t.Fatal(err)
	}
	if at, _ := dag.Catalog.CatalogSyncState("s"); !at.Equal(created) {
		t.Fatal(at)
	}
}

func TestCatalogSearch(t *testing.T) { catalogSearch(t, NewStore(t.TempDir())) }

func TestPGCatalogSearch(t *testing.T) { resetPG(t); catalogSearch(t, pgStore(t)) }

func catalogSearch(t *testing.T, st *Store) {
	err := st.UpdateCatalog(CatalogUpdate{Tables: []CatalogTable{
		{ID: "bigquery:p.d.Orders", Columns: []CatalogColumn{{Name: "Customer_ID"}, {Name: "amount_50%"}}},
		{ID: "bigquery:p.d.people", Columns: []CatalogColumn{{Name: "name"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for q, want := range map[string]int{"": 2, "orders": 1, "customer_id": 1, "NAME": 1, "50%": 1, "5_%": 0, "zzz": 0} {
		got, err := st.CatalogSummaries(q)
		if err != nil || len(got) != want {
			t.Fatalf("%q: %d results (%v), want %d", q, len(got), err, want)
		}
	}
	if got := st.CatalogColumnNames("bigquery:p.d.orders"); len(got) != 2 {
		t.Fatalf("case-insensitive lookup: %v", got)
	}
}
