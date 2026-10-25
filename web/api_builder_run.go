package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"dagsflow-go/dag"
	"dagsflow-go/engine"
)

// "Run this node" in the builder: runs one node of the graph being edited (it
// doesn't have to be saved) right away in the webserver, and returns its log
// and output, so a source can be previewed before the workflow runs.
//
// The node's upstream outputs come, in this order, from:
//  1. outputs the UI kept from earlier "Run node" calls (sent back as `outputs`),
//  2. the latest run of the saved workflow,
//  3. running the upstream node now, but only for nodes that only read data
//     (sources, transform, merge, If, no-op). Anything else must be run first.
//
// Nothing is written to the metadata database.

// PreviewTimeout bounds one "Run node" call.
var PreviewTimeout = 2 * time.Minute

const (
	previewMaxRows = 200       // rows sent back for the table view
	previewMaxLog  = 256 << 10 // log bytes sent back
)

type runNodeBody struct {
	Graph   dag.Graph      `json:"graph"`
	Node    string         `json:"node"`
	Outputs map[string]any `json:"outputs"` // node id -> output kept by the UI
}

// safePreview: node types that only read data, so the preview may run them to
// get the input of the node being previewed.
func safePreview(n dag.GNode) bool {
	switch n.Type {
	case "postgres_query", "bigquery_query", "gsheet_read", "file_read", "transform", "merge", "if", "empty":
		return true
	case "http":
		m := strings.ToUpper(fmt.Sprint(n.Config["method"]))
		return m == "" || m == "<NIL>" || m == "GET"
	}
	return false
}

func (s *Server) builderRunNode(w http.ResponseWriter, r *http.Request) {
	s.reloadNodes()
	var b runNodeBody
	if err := readBody(r, &b); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	g := b.Graph
	id := g.DagID
	if !dag.ValidDagID(id) {
		id = "builder_preview"
	}
	a := accessFrom(r)
	if !a.DAG(engine.ActDagEdit, id) || !a.DAG(engine.ActDagTrigger, id) {
		fail(w, 403, "running a node needs the edit and trigger permissions on %s", id)
		return
	}
	byID := map[string]dag.GNode{}
	for _, n := range g.Nodes {
		byID[n.ID] = n
	}
	target, ok := byID[b.Node]
	if !ok {
		fail(w, 400, "node %q is not in the workflow", b.Node)
		return
	}

	// the node and everything before it
	ups := map[string][]string{}
	for _, e := range g.Edges {
		ups[e.To] = append(ups[e.To], e.From)
	}
	need := map[string]bool{}
	var walk func(string)
	walk = func(n string) {
		for _, u := range ups[n] {
			if !need[u] {
				need[u] = true
				walk(u)
			}
		}
	}
	walk(target.ID)
	sub := dag.Graph{DagID: id, Settings: g.Settings}
	for _, n := range g.Nodes {
		if n.ID == target.ID || need[n.ID] {
			sub.Nodes = append(sub.Nodes, n)
		}
	}
	for _, e := range g.Edges {
		if (need[e.From] || e.From == target.ID) && (need[e.To] || e.To == target.ID) {
			sub.Edges = append(sub.Edges, e)
		}
	}
	if target.Type == "if" { // its true/false sides, as placeholders (they don't run)
		for _, e := range g.Edges {
			if e.From == target.ID && !need[e.To] {
				sub.Nodes = append(sub.Nodes, dag.GNode{ID: e.To, Type: "empty", Config: map[string]any{}})
				sub.Edges = append(sub.Edges, e)
			}
		}
	}
	y, err := sub.ToYAML()
	if err != nil {
		fail(w, 400, "%v", err)
		return
	}
	dags, err := dag.ParseYAML(id+".yaml", y)
	if err != nil {
		fail(w, 400, "%s", strings.TrimPrefix(err.Error(), fmt.Sprintf("dag %q: ", id)))
		return
	}
	if err := dags[0].Validate(); err != nil {
		fail(w, 400, "%v", err)
		return
	}

	seed := map[string]map[string]any{}
	used := map[string]string{}
	for nid, v := range b.Outputs {
		if need[nid] {
			seed[nid] = map[string]any{"return_value": v}
			used[nid] = "previous preview"
		}
	}
	if dag.ValidDagID(g.DagID) {
		if last := s.latestXCom(g.DagID); last != nil {
			for slot, kv := range last {
				t, _, _ := strings.Cut(slot, "#")
				if !need[t] || used[t] == "previous preview" {
					continue
				}
				seed[slot] = kv
				used[t] = "last run"
			}
		}
	}
	var safe []string
	for nid := range need {
		if safePreview(byID[nid]) {
			safe = append(safe, nid)
		}
	}
	req := engine.PreviewRequest{DagID: id, YAML: y, Node: target.ID, Seed: seed, Safe: safe,
		TimeoutMS: PreviewTimeout.Milliseconds(), LogMax: previewMaxLog}

	var res engine.PreviewResult
	where := strings.ToLower(os.Getenv("DAGSFLOW_PREVIEW_ON"))
	if where == "worker" || where != "webserver" && s.Store.PreviewWorkers(dag.DefaultQueue) {
		res, err = s.Store.RunPreviewOnWorker(r.Context(), dag.DefaultQueue, req)
		if err != nil {
			fail(w, 502, "preview on a worker: %v", err)
			return
		}
	} else {
		res = engine.RunPreview(r.Context(), req)
		res.Where = "webserver"
	}
	if res.Status == "needs_input" {
		fail(w, 409, "%s", res.Error)
		return
	}
	for _, nid := range res.Ran {
		used[nid] = "run now"
	}
	out := map[string]any{"status": res.Status, "log": res.Log, "duration_ms": res.DurationMS, "ran": res.Ran,
		"inputs": used, "where": res.Where}
	if res.Error != "" {
		out["error"] = res.Error
	}
	if res.Selected != nil || target.Type == "if" && res.Status == "success" {
		sel := res.Selected
		if sel == nil {
			sel = []string{}
		}
		out["selected"] = sel
	}
	if res.ConditionMet != nil {
		out["condition_met"] = *res.ConditionMet
	}
	if res.Note != "" {
		out["note"] = res.Note
	}
	if res.HasOutput {
		out["output"], out["rows_total"] = previewValue(res.Output)
		if b, _ := json.Marshal(res.Output); len(b) <= 4<<20 { // small enough to keep in the browser
			out["full_output"] = res.Output
		}
	}
	if len(res.XCom) > 0 {
		out["xcom"] = res.XCom
	}
	s.audit(r, "builder_run_node", g.DagID, "", target.ID, fmt.Sprint(out["status"]))
	writeJSON(w, 200, out)
}

// previewValue trims rows for the table view.
func previewValue(v any) (any, int) {
	if s, ok := v.(string); ok {
		var parsed any
		if t := strings.TrimSpace(s); t != "" && strings.ContainsAny(t[:1], "[{") && json.Unmarshal([]byte(t), &parsed) == nil {
			v = parsed
		} else if len(s) > 64<<10 {
			return s[:64<<10] + "\n… cut", len(s)
		}
	}
	if rows, ok := v.([]any); ok {
		n := len(rows)
		if n > previewMaxRows {
			rows = rows[:previewMaxRows]
		}
		return rows, n
	}
	return v, -1
}

// latestXCom: the XComs of the most recent run of the workflow.
func (s *Server) latestXCom(dagID string) map[string]map[string]any {
	runs, err := s.Store.ListRuns(dagID)
	if err != nil {
		return nil
	}
	var best *engine.DagRun
	for _, r := range runs {
		if len(r.XCom) == 0 {
			continue
		}
		if best == nil || r.LogicalDate.After(best.LogicalDate) {
			best = r
		}
	}
	if best == nil {
		return nil
	}
	_ = s.Store.ResolveXCom(best)
	return best.XCom
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}
