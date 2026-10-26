package web

import (
	"net/http"
	"sort"
	"strconv"
	"strings"

	"dagsflow-go/engine"
)

// ---- data catalog & lineage --------------------------------------------------------
//
// catalog.read is scoped to dataset patterns: a user sees the datasets that
// match, and the steps between nodes of DAGs they can read. Lineage through a
// dataset they can't see is still followed, but that dataset is left out.

// via: "dag.task" is shown only to users who can read that DAG.
func maskVia(a *engine.Access, via string) string {
	if via == "" || strings.HasPrefix(via, "job ") || strings.HasPrefix(via, "query ") {
		return via
	}
	if dagID, _, ok := strings.Cut(via, "."); ok && !a.DAG(engine.ActDagRead, dagID) {
		return ""
	}
	return via
}

// GET /api/v1/catalog?q=&engine=&all=1 (q: a table or column name; all: also
// the rows passed between nodes)
func (s *Server) catalogList(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.CatalogSummaries(r.URL.Query().Get("q"))
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	a := accessFrom(r)
	eng := r.URL.Query().Get("engine")
	all := r.URL.Query().Get("all") == "1"
	out := []engine.CatalogSummary{}
	engines := map[string]int{}
	for _, t := range list {
		if t.Kind == "task" && !all || !a.CanDataset(t.ID) {
			continue
		}
		engines[t.Engine]++
		if eng == "" || t.Engine == eng {
			w := t.Writers[:0:0]
			for _, v := range t.Writers {
				if m := maskVia(a, v); m != "" {
					w = append(w, m)
				}
			}
			t.Writers = w
			out = append(out, t)
		}
	}
	writeJSON(w, 200, map[string]any{"tables": out, "engines": engines})
}

// GET /api/v1/catalog/table?id=
func (s *Server) catalogTable(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	a := accessFrom(r)
	if !a.CanDataset(id) {
		fail(w, 403, "your roles cannot see %s in the catalog (catalog.read)", id)
		return
	}
	t, up, down, err := s.Store.CatalogTable(id)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	if t == nil {
		fail(w, 404, "%s is not in the catalog", id)
		return
	}
	visible := func(l []engine.LineageEdge, other func(engine.LineageEdge) string) []engine.LineageEdge {
		out := []engine.LineageEdge{}
		for _, e := range l {
			if a.CanDataset(other(e)) {
				e.Via = maskVia(a, e.Via)
				out = append(out, e)
			}
		}
		return out
	}
	cp := *t
	cp.Writers = nil
	for _, v := range t.Writers {
		if m := maskVia(a, v); m != "" {
			cp.Writers = append(cp.Writers, m)
		}
	}
	writeJSON(w, 200, map[string]any{"table": cp,
		"upstream":   visible(up, func(e engine.LineageEdge) string { return e.From }),
		"downstream": visible(down, func(e engine.LineageEdge) string { return e.To })})
}

// GET /api/v1/catalog/lineage?id=&column=&depth= : both directions.
func (s *Server) catalogLineage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id, col := q.Get("id"), q.Get("column")
	a := accessFrom(r)
	if !a.CanDataset(id) {
		fail(w, 403, "your roles cannot see %s in the catalog (catalog.read)", id)
		return
	}
	depth, _ := strconv.Atoi(q.Get("depth"))
	if depth <= 0 || depth > 50 {
		depth = 10
	}
	walk := func(upstream bool) ([]engine.LineageNode, error) {
		l, err := s.Store.ColumnLineage(id, col, upstream, depth)
		out := []engine.LineageNode{}
		for _, n := range l {
			if a.CanDataset(n.Dataset) {
				n.Via = maskVia(a, n.Via)
				out = append(out, n)
			}
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].Depth < out[j].Depth })
		return out, err
	}
	up, err := walk(true)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	down, err := walk(false)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "column": col, "upstream": up, "downstream": down})
}
