package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

// DAGSFLOW_CATALOG_BENCH=1 go test ./engine -run CatalogScale -v
// Times the catalog with a large warehouse (JSON store, and Postgres when
// DAGSFLOW_TEST_DATABASE_URL is set).
func TestCatalogScale(t *testing.T) {
	if os.Getenv("DAGSFLOW_CATALOG_BENCH") == "" {
		t.Skip("set DAGSFLOW_CATALOG_BENCH=1")
	}
	t.Run("json", func(t *testing.T) { catalogScale(t, NewStore(t.TempDir())) })
	t.Run("postgres", func(t *testing.T) { resetPG(t); catalogScale(t, pgStore(t)) })
}

func catalogScale(t *testing.T, st *Store) {
	const nTables, nCols, nEdges = 10000, 30, 20000
	scope := "bigquery:p:us"
	tables := make([]CatalogTable, nTables)
	for i := range tables {
		cols := make([]CatalogColumn, nCols)
		for c := range cols {
			cols[c] = CatalogColumn{Name: fmt.Sprintf("column_%02d", c), Type: "STRING"}
		}
		tables[i] = CatalogTable{ID: fmt.Sprintf("bigquery:p.ds%02d.table_%05d", i%50, i), Kind: "table", Source: "scan", Scope: scope, Columns: cols}
	}
	edges := make([]LineageEdge, nEdges)
	for i := range edges {
		edges[i] = LineageEdge{From: tables[i%nTables].ID, FromCol: fmt.Sprintf("column_%02d", i%nCols),
			To: tables[(i*7+1)%nTables].ID, ToCol: fmt.Sprintf("column_%02d", (i+3)%nCols), Via: "job x by a@b.c", Kind: "query"}
	}
	timeIt := func(name string, n int, f func()) {
		start := time.Now()
		for i := 0; i < n; i++ {
			f()
		}
		t.Logf("%-46s %v", name, (time.Since(start) / time.Duration(n)).Round(time.Microsecond))
	}
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	timeIt("first sync (10k tables, 300k cols, 20k edges)", 1, func() { must(st.UpdateCatalog(CatalogUpdate{FullScope: scope, Tables: tables, Edges: edges})) })
	timeIt("next sync, nothing changed", 1, func() { must(st.UpdateCatalog(CatalogUpdate{FullScope: scope, Tables: tables, Edges: edges[:500]})) })
	i := 0
	timeIt("task success: record lineage (12 edges)", 50, func() {
		i++
		v := map[string]any{"datasets": []any{map[string]any{"id": fmt.Sprintf("bigquery:p.mart.t%d", i%5), "columns": []any{map[string]any{"name": "a"}}}}}
		var es []any
		for k := 0; k < 12; k++ {
			es = append(es, map[string]any{"from": tables[k].ID, "from_col": "column_01", "to": fmt.Sprintf("bigquery:p.mart.t%d", i%5), "to_col": "a"})
		}
		v["edges"] = es
		must(st.RecordTaskLineage("d", "t", v))
	})
	timeIt("column lookup for SQL (cached)", 1000, func() { st.CatalogColumnNames(tables[123].ID) })
	timeIt("column lookup for SQL (cold)", 3, func() { st.dropCatalogCache(); st.CatalogColumnNames(tables[123].ID) })
	var size int
	timeIt("catalog page: list all (cold) + JSON", 3, func() {
		st.dropCatalogCache()
		ts, err := st.CatalogSummaries("")
		must(err)
		b, _ := json.Marshal(ts)
		size = len(b)
	})
	t.Logf("%-46s %.1f MB", "catalog page response", float64(size)/1e6)
	timeIt("search a column name (cold)", 3, func() { st.dropCatalogCache(); _, err := st.CatalogSummaries("column_07"); must(err) })
	timeIt("table page: table + edges", 5, func() { _, _, _, err := st.CatalogTable(tables[10].ID); must(err) })
	timeIt("column lineage, both ways, depth 10", 5, func() {
		_, err := st.ColumnLineage(tables[10].ID, "column_05", true, 10)
		must(err)
		_, err = st.ColumnLineage(tables[10].ID, "column_05", false, 10)
		must(err)
	})
}
