package web

import (
	"os"
	"path/filepath"
	"strings"

	"net/http"

	"dagsflow-go/dag"
	"dagsflow-go/engine"
)

// Custom builder nodes: definitions live in <dags>/builder/nodes/<type>.yaml.
// The UI edits those files through this API; Go nodes are registered in code
// (dag.RegisterNodeType) and are read-only here.

func (s *Server) nodesDir() string { return filepath.Join(s.BuilderDir, "nodes") }

func (s *Server) reloadNodes() { dag.LoadCustomNodes(s.nodesDir()) }

func (s *Server) customNodeGet(w http.ResponseWriter, r *http.Request) {
	s.reloadNodes()
	t := r.PathValue("type")
	def, ok := dag.CustomNodeDefOf(t)
	if !ok {
		fail(w, 404, "no YAML-defined custom node %q (Go-defined nodes cannot be edited here)", t)
		return
	}
	writeJSON(w, 200, map[string]any{"node": def})
}

func (s *Server) customNodePut(w http.ResponseWriter, r *http.Request) {
	t := r.PathValue("type")
	var def dag.CustomNodeDef
	if err := readBody(r, &def); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	def.Type = t
	if err := def.Validate(); err != nil {
		fail(w, 422, "%v", err)
		return
	}
	s.builderMu.Lock()
	defer s.builderMu.Unlock()
	s.reloadNodes()
	for _, n := range dag.CustomNodes() {
		if n.Type == t && n.Source == "go" {
			fail(w, 409, "node type %q is registered from Go code", t)
			return
		}
	}
	data, err := dag.MarshalCustomNodeDef(&def)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	if err := os.MkdirAll(s.nodesDir(), 0o755); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	path := filepath.Join(s.nodesDir(), t+".yaml")
	_, existed := dag.CustomNodeDefOf(t)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	s.reloadNodes()
	s.audit(r, "builder_node_save", "", "", "", t)
	writeJSON(w, 200, map[string]any{"type": t, "created": !existed, "file": path})
}

type nodeUsage struct {
	DagID      string   `json:"dag_id"`
	Nodes      []string `json:"nodes"`
	WillDelete bool     `json:"will_delete"` // the workflow has no other node left
	path       string
	graph      *dag.Graph
}

// customNodeUsage lists the builder workflows that contain a node of type t.
func (s *Server) customNodeUsage(t string) []nodeUsage {
	var out []nodeUsage
	entries, err := os.ReadDir(s.BuilderDir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if e.IsDir() || !isYAMLFile(e.Name()) {
			continue
		}
		p := filepath.Join(s.BuilderDir, e.Name())
		data, err := os.ReadFile(p)
		if err != nil || !strings.Contains(string(data), t) {
			continue
		}
		g, err := dag.ParseGraph(data)
		if err != nil {
			continue
		}
		u := nodeUsage{DagID: g.DagID, path: p, graph: g}
		for _, n := range g.Nodes {
			if n.Type == t {
				u.Nodes = append(u.Nodes, n.ID)
			}
		}
		if len(u.Nodes) > 0 {
			u.WillDelete = len(u.Nodes) == len(g.Nodes)
			out = append(out, u)
		}
	}
	return out
}

// dropNodes removes the nodes with the given ids, their connections and any
// setting of the remaining nodes that pointed at them.
func dropNodes(g *dag.Graph, ids map[string]bool) {
	nodes := g.Nodes[:0:0]
	for _, n := range g.Nodes {
		if !ids[n.ID] {
			nodes = append(nodes, n)
		}
	}
	g.Nodes = nodes
	edges := g.Edges[:0:0]
	for _, e := range g.Edges {
		if !ids[e.From] && !ids[e.To] {
			edges = append(edges, e)
		}
	}
	g.Edges = edges
	for _, n := range g.Nodes {
		for _, k := range []string{"input", "rows_from"} {
			if v, _ := n.Config[k].(string); ids[v] {
				delete(n.Config, k)
			}
		}
		if l, ok := n.Config["inputs"].([]any); ok {
			keep := []any{}
			for _, x := range l {
				if v, _ := x.(string); !ids[v] {
					keep = append(keep, x)
				}
			}
			if len(keep) == 0 {
				delete(n.Config, "inputs")
			} else {
				n.Config["inputs"] = keep
			}
		}
		if ex, _ := n.Config["expand"].(map[string]any); ex != nil {
			if xc, _ := ex["xcom"].(map[string]any); xc != nil {
				if v, _ := xc["task"].(string); ids[v] {
					delete(n.Config, "expand")
				}
			}
		}
	}
}

func (s *Server) customNodeUsageGet(w http.ResponseWriter, r *http.Request) {
	s.reloadNodes()
	t := r.PathValue("type")
	if _, ok := dag.CustomNodeDefOf(t); !ok {
		fail(w, 404, "no YAML-defined custom node %q", t)
		return
	}
	used := s.customNodeUsage(t)
	if used == nil {
		used = []nodeUsage{}
	}
	writeJSON(w, 200, map[string]any{"used_by": used})
}

// customNodeDelete removes a node definition. While workflows still use it the
// call answers 409 with the list; with ?force=1 those nodes are deleted from
// every workflow first (a workflow left without nodes is deleted).
func (s *Server) customNodeDelete(w http.ResponseWriter, r *http.Request) {
	t := r.PathValue("type")
	force := r.URL.Query().Get("force") == "1"
	s.builderMu.Lock()
	defer s.builderMu.Unlock()
	s.reloadNodes()
	if _, ok := dag.CustomNodeDefOf(t); !ok {
		fail(w, 404, "no YAML-defined custom node %q", t)
		return
	}
	used := s.customNodeUsage(t)
	if len(used) > 0 && !force {
		writeJSON(w, 409, map[string]any{"error": "node " + t + " is still used by workflows", "used_by": used})
		return
	}
	a := accessFrom(r)
	for _, u := range used { // check everything before changing anything
		if !a.DAG(engine.ActDagEdit, u.DagID) {
			fail(w, 403, "you cannot edit DAG %s, which uses this node", u.DagID)
			return
		}
	}
	var changed []string
	for _, u := range used {
		if u.WillDelete {
			if err := os.Remove(u.path); err != nil {
				fail(w, 500, "%v", err)
				return
			}
		} else {
			ids := map[string]bool{}
			for _, id := range u.Nodes {
				ids[id] = true
			}
			dropNodes(u.graph, ids)
			y, err := u.graph.ToYAML()
			if err != nil {
				fail(w, 500, "%s: %v", u.DagID, err)
				return
			}
			tmp := u.path + ".tmp"
			if err := os.WriteFile(tmp, y, 0o644); err != nil {
				fail(w, 500, "%v", err)
				return
			}
			if err := os.Rename(tmp, u.path); err != nil {
				_ = os.Remove(tmp)
				fail(w, 500, "%v", err)
				return
			}
		}
		if s.Reload != nil {
			_ = s.Reload(u.path)
		}
		changed = append(changed, u.DagID)
		s.audit(r, "builder_node_cascade", u.DagID, "", "", t)
	}
	_ = os.Remove(filepath.Join(s.nodesDir(), t+".yaml"))
	_ = os.Remove(filepath.Join(s.nodesDir(), t+".yml"))
	s.reloadNodes()
	s.audit(r, "builder_node_delete", "", "", "", t)
	writeJSON(w, 200, map[string]any{"deleted": t, "workflows_changed": changed})
}
