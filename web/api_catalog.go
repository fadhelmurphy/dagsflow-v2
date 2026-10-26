package web

import (
	"net/http"
	"sort"
	"strconv"

	"dagsflow-go/engine"
)

// ---- data catalog & lineage --------------------------------------------------------

// GET /api/v1/catalog?q=&engine=&all=1 (q: a table or column name; all: also
// the rows passed between nodes)
func (s *Server) catalogList(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.CatalogSummaries(r.URL.Query().Get("q"))
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	eng := r.URL.Query().Get("engine")
	all := r.URL.Query().Get("all") == "1"
	out := []engine.CatalogSummary{}
	engines := map[string]int{}
	for _, t := range list {
		if t.Kind == "task" && !all {
			continue
		}
		engines[t.Engine]++
		if eng == "" || t.Engine == eng {
			out = append(out, t)
		}
	}
	writeJSON(w, 200, map[string]any{"tables": out, "engines": engines})
}

// GET /api/v1/catalog/table?id=
func (s *Server) catalogTable(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	t, up, down, err := s.Store.CatalogTable(id)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	if t == nil {
		fail(w, 404, "%s is not in the catalog", id)
		return
	}
	if up == nil {
		up = []engine.LineageEdge{}
	}
	if down == nil {
		down = []engine.LineageEdge{}
	}
	writeJSON(w, 200, map[string]any{"table": t, "upstream": up, "downstream": down})
}

// GET /api/v1/catalog/lineage?id=&column=&depth= : both directions.
func (s *Server) catalogLineage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id, col := q.Get("id"), q.Get("column")
	depth, _ := strconv.Atoi(q.Get("depth"))
	if depth <= 0 || depth > 50 {
		depth = 10
	}
	up, err := s.Store.ColumnLineage(id, col, true, depth)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	down, err := s.Store.ColumnLineage(id, col, false, depth)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	for _, l := range [][]engine.LineageNode{up, down} {
		sort.SliceStable(l, func(i, j int) bool { return l[i].Depth < l[j].Depth })
	}
	if up == nil {
		up = []engine.LineageNode{}
	}
	if down == nil {
		down = []engine.LineageNode{}
	}
	writeJSON(w, 200, map[string]any{"id": id, "column": col, "upstream": up, "downstream": down})
}
