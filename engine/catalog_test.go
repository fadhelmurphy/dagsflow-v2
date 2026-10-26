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
	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
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

func TestCatalogStaleLineage(t *testing.T) { catalogStale(t, NewStore(t.TempDir())) }

func TestPGCatalogStaleLineage(t *testing.T) { resetPG(t); catalogStale(t, pgStore(t)) }

func catalogStale(t *testing.T, st *Store) {
	report := func(from string, replace bool) {
		t.Helper()
		v := map[string]any{"edges": []any{map[string]any{"from": from, "from_col": "a", "to": "bigquery:p.d.out", "to_col": "a"}}}
		if err := st.RecordTaskLineage("d", "t", v, replace); err != nil {
			t.Fatal(err)
		}
	}
	upstream := func() []string {
		t.Helper()
		_, up, _, err := st.CatalogTable("bigquery:p.d.out")
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range up {
			out = append(out, e.From)
		}
		return out
	}
	report("bigquery:p.d.old", true)
	report("bigquery:p.d.new", true) // the task changed: its old edge goes
	if got := upstream(); len(got) != 1 || got[0] != "bigquery:p.d.new" {
		t.Fatalf("after re-run: %v", got)
	}
	report("bigquery:p.d.other", false) // a mapped instance adds, never replaces
	if got := upstream(); len(got) != 2 {
		t.Fatalf("mapped: %v", got)
	}

	// a query that redefines a table drops the query lineage seen before it
	t0 := time.Now().Add(-48 * time.Hour)
	st.InstallCatalogHooks()
	edge := func(from string, at time.Time) dag.ScannedEdge {
		return dag.ScannedEdge{LineageLink: dag.LineageLink{From: from, FromCol: "x", To: "bigquery:p.d.mart", ToCol: "x"}, Via: "job", At: at}
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(dag.Catalog.SaveCatalogScan(dag.CatalogScan{Scope: "s", Edges: []dag.ScannedEdge{edge("bigquery:p.d.v1", t0)}}))
	t1 := t0.Add(time.Hour)
	must(dag.Catalog.SaveCatalogScan(dag.CatalogScan{Scope: "s", Edges: []dag.ScannedEdge{edge("bigquery:p.d.v1b", t0.Add(time.Minute)), edge("bigquery:p.d.v2", t1)},
		Redefined: map[string]time.Time{"bigquery:p.d.mart": t1}}))
	_, up, _, _ := st.CatalogTable("bigquery:p.d.mart")
	if len(up) != 1 || up[0].From != "bigquery:p.d.v2" {
		t.Fatalf("redefined: %+v", up)
	}

	// expiry: hidden after the TTL, removed by clean; long-dropped tables purged
	t.Setenv("DAGSFLOW_CATALOG_EDGE_TTL_DAYS", "1")
	if _, up, _, _ := st.CatalogTable("bigquery:p.d.mart"); len(up) != 0 {
		t.Fatalf("old lineage should be hidden: %+v", up)
	}
	if n, _ := st.ColumnLineage("bigquery:p.d.mart", "x", true, 5); len(n) != 0 {
		t.Fatalf("old lineage in a walk: %+v", n)
	}
	must(st.UpdateCatalog(CatalogUpdate{FullScope: "gone", Tables: []CatalogTable{{ID: "bigquery:p.d.g", Scope: "gone", Columns: []CatalogColumn{}}}}))
	must(st.UpdateCatalog(CatalogUpdate{FullScope: "gone"}))
	time.Sleep(10 * time.Millisecond)
	edges, tables, err := st.CleanCatalog(CatalogEdgeTTL(), time.Millisecond)
	if err != nil || edges != 1 || tables != 1 {
		t.Fatalf("clean: %d edges, %d tables, %v", edges, tables, err)
	}
	if g, _, _, _ := st.CatalogTable("bigquery:p.d.g"); g != nil {
		t.Fatal("dropped table should be purged")
	}
}

func TestPGCatalogBackupRoundTrip(t *testing.T) {
	src := NewStore(t.TempDir())
	if err := src.RecordTaskLineage("d", "t", map[string]any{
		"datasets": []any{map[string]any{"id": "gsheet:x!S", "columns": []any{map[string]any{"name": "a"}}}},
		"edges":    []any{map[string]any{"from": "gsheet:x!S", "from_col": "a", "to": "bigquery:p.d.t", "to_col": "a"}},
	}, true); err != nil {
		t.Fatal(err)
	}
	_ = src.SetSyncState("bigquery:p:us", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	b, err := src.Export(10)
	if err != nil || b.Catalog == nil || len(b.Catalog.Tables) != 2 || len(b.Catalog.Edges) != 1 {
		t.Fatalf("export: %+v %v", b.Catalog, err)
	}
	resetPG(t)
	dst := pgStore(t)
	st, err := dst.Import(b)
	if err != nil || st.CatalogTables != 2 || st.CatalogEdges != 1 {
		t.Fatalf("import: %+v %v", st, err)
	}
	tb, up, _, _ := dst.CatalogTable("bigquery:p.d.t")
	if tb == nil || len(up) != 1 || up[0].Via != "d.t" {
		t.Fatalf("%+v %+v", tb, up)
	}
	if at, _ := dst.SyncState("bigquery:p:us"); at.Year() != 2026 {
		t.Fatal("sync state not restored")
	}
}

func TestNormalizeDatasetID(t *testing.T) {
	for in, want := range map[string]string{
		"s3:s3://lake/events/2026/10/27/":  "s3:s3://lake/events/{date}/",
		"gcs:gs://b/x/2026-10-27/a.csv":    "gcs:gs://b/x/{date}/a.csv",
		"gcs:gs://b/x/orders_20261027.csv": "gcs:gs://b/x/orders_{date}.csv",
		"bigquery:p.d.orders_20261027":     "bigquery:p.d.orders_20261027",
	} {
		if got := NormalizeDatasetID(in); got != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}
