package web

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"dagsflow-go/dag"
	"dagsflow-go/engine"
)

// audit records an Event Log entry for the signed-in user.
func (s *Server) audit(r *http.Request, event, dagID, runID, taskID, extra string) {
	user := "anonymous"
	if u := userFrom(r); u != nil {
		user = u.Username
	}
	s.Store.Audit(engine.AuditEvent{User: user, Event: event, DagID: dagID, RunID: runID, TaskID: taskID, Extra: extra})
}

// ---- code & reparse ---------------------------------------------------------------

func (s *Server) dagSource(w http.ResponseWriter, r *http.Request) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return
	}
	files, err := d.SourceFiles()
	resp := map[string]any{"dag_id": d.ID, "source": d.Source, "parsed_at": d.ParsedAt,
		"is_file_source": d.IsFileSource(), "files": files}
	if err != nil {
		resp["error"] = err.Error()
		resp["files"] = []dag.SourceFile{}
	}
	writeJSON(w, 200, resp)
}

func (s *Server) reparse(w http.ResponseWriter, r *http.Request) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return
	}
	if s.Reparse == nil {
		fail(w, 501, "reparse is not available in this server")
		return
	}
	if err := s.Reparse(d); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	s.audit(r, "reparse", d.ID, "", "", d.Source)
	resp := map[string]any{"dag_id": d.ID, "source": d.Source}
	if nd, ok := dag.Get(d.ID); ok {
		resp["parsed_at"] = nd.ParsedAt
	} else if err, bad := dag.ImportErrors()[d.Source]; bad {
		fail(w, 422, "the file no longer loads: %v", err)
		return
	} else {
		fail(w, 404, "dag %s is no longer defined in %s", d.ID, d.Source)
		return
	}
	writeJSON(w, 200, resp)
}

// ---- rendered template ----------------------------------------------------------------

func (s *Server) rendered(w http.ResponseWriter, r *http.Request) {
	d, run, ok := s.runLite404(w, r)
	if !ok {
		return
	}
	idx := -1
	if v := r.URL.Query().Get("map_index"); v != "" {
		idx, _ = strconv.Atoi(v)
	}
	v, err := engine.RenderedFields(s.Store, d, run, r.PathValue("task"), idx)
	if err != nil {
		fail(w, 404, "%v", err)
		return
	}
	writeJSON(w, 200, v)
}

// ---- event log -----------------------------------------------------------------------

func (s *Server) eventLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	a := accessFrom(r)
	if d := q.Get("dag_id"); d != "" && !a.DAG(engine.ActDagRead, d) {
		fail(w, 403, "you cannot read DAG %s", d)
		return
	}
	// fetch extra so filtering out unreadable DAGs still fills the page
	evs, err := s.Store.AuditEvents(q.Get("dag_id"), q.Get("run_id"), q.Get("task_id"), limit*4)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	out := evs[:0]
	for _, ev := range evs {
		if ev.DagID == "" || a.DAG(engine.ActDagRead, ev.DagID) {
			out = append(out, ev)
		}
		if len(out) == limit {
			break
		}
	}
	writeJSON(w, 200, map[string]any{"event_logs": out})
}

// ---- mark state ------------------------------------------------------------------------

type markBody struct {
	State      string `json:"new_state"`
	MapIndex   *int   `json:"map_index"`
	Upstream   bool   `json:"include_upstream"`
	Downstream bool   `json:"include_downstream"`
}

func (s *Server) markTask(w http.ResponseWriter, r *http.Request) {
	var b markBody
	if err := readBody(r, &b); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	idx := -1
	if b.MapIndex != nil {
		idx = *b.MapIndex
	}
	s.mark(w, r, engine.MarkRequest{Target: "task", TaskID: r.PathValue("task"), MapIndex: idx,
		State: dag.State(b.State), Upstream: b.Upstream, Downstream: b.Downstream})
}

func (s *Server) markRun(w http.ResponseWriter, r *http.Request) {
	var b markBody
	if err := readBody(r, &b); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	s.mark(w, r, engine.MarkRequest{Target: "run", MapIndex: -1, State: dag.State(b.State)})
}

func (s *Server) mark(w http.ResponseWriter, r *http.Request, m engine.MarkRequest) {
	d, run, ok := s.runOr404(w, r)
	if !ok {
		return
	}
	if u := userFrom(r); u != nil {
		m.User = u.Username
	}
	if err := m.Validate(d); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	what := "run"
	if m.Target == "task" {
		what = m.TaskID
		if m.MapIndex >= 0 {
			what += fmt.Sprintf("[%d]", m.MapIndex)
		}
		var opts []string
		if m.Upstream {
			opts = append(opts, "upstream")
		}
		if m.Downstream {
			opts = append(opts, "downstream")
		}
		if len(opts) > 0 {
			what += " +" + strings.Join(opts, "+")
		}
	}
	extra := fmt.Sprintf("%s -> %s", what, m.State)

	// The scheduler is executing this run: hand the request to it so its
	// in-memory state stays authoritative (it also stops the task).
	if run.State == engine.RunRunning {
		if _, alive := engine.SchedulerInfo(s.Store); alive {
			if err := s.Store.EnqueueMark(d.ID, run.RunID, m); err != nil {
				fail(w, 500, "%v", err)
				return
			}
			s.audit(r, "mark_"+string(m.State), d.ID, run.RunID, m.TaskID, extra)
			writeJSON(w, 202, map[string]any{"queued": true, "message": "The scheduler applies this within a few seconds."})
			return
		}
	}
	changed, err := engine.MarkState(d, run, m)
	if err != nil {
		fail(w, 400, "%v", err)
		return
	}
	if err := s.Store.SaveRun(run); err != nil {
		failStore(w, err)
		return
	}
	s.audit(r, "mark_"+string(m.State), d.ID, run.RunID, m.TaskID, extra)
	writeJSON(w, 200, map[string]any{"changed": changed, "dag_run_state": run.State})
}

// ---- notes (Airflow "Task Instance Notes" / "DAG Run Notes") -------------------------------

type noteBody struct {
	Note     string `json:"note"`
	MapIndex *int   `json:"map_index"`
}

func (s *Server) runNote(w http.ResponseWriter, r *http.Request) {
	var b noteBody
	if err := readBody(r, &b); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	s.setNote(w, r, engine.MarkRequest{Kind: "note", Target: "run", MapIndex: -1, Note: b.Note})
}

func (s *Server) tiNote(w http.ResponseWriter, r *http.Request) {
	var b noteBody
	if err := readBody(r, &b); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	idx := -1
	if b.MapIndex != nil {
		idx = *b.MapIndex
	}
	s.setNote(w, r, engine.MarkRequest{Kind: "note", Target: "task", TaskID: r.PathValue("task"), MapIndex: idx, Note: b.Note})
}

func (s *Server) setNote(w http.ResponseWriter, r *http.Request, m engine.MarkRequest) {
	d, run, ok := s.runOr404(w, r)
	if !ok {
		return
	}
	if u := userFrom(r); u != nil {
		m.User = u.Username
	}
	if err := m.Validate(d); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	extra := "note cleared"
	if m.Note != "" {
		extra = "note set"
	}
	// running: the scheduler owns the run file; it applies the note itself
	if run.State == engine.RunRunning {
		if _, alive := engine.SchedulerInfo(s.Store); alive {
			if err := s.Store.EnqueueMark(d.ID, run.RunID, m); err != nil {
				fail(w, 500, "%v", err)
				return
			}
			s.audit(r, "set_note", d.ID, run.RunID, m.TaskID, extra)
			writeJSON(w, 202, map[string]any{"queued": true, "note": m.Note})
			return
		}
	}
	if err := engine.SetNote(d, run, m); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	if err := s.Store.SaveRun(run); err != nil {
		failStore(w, err)
		return
	}
	s.audit(r, "set_note", d.ID, run.RunID, m.TaskID, extra)
	writeJSON(w, 200, map[string]any{"note": m.Note})
}

// ---- task instance list (Browse → Task Instances / "List All Instances") -------------

func (s *Server) taskInstances(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	type item struct {
		DagID       string     `json:"dag_id"`
		TaskID      string     `json:"task_id"`
		RunID       string     `json:"run_id"`
		MapIndex    int        `json:"map_index"`
		State       dag.State  `json:"state"`
		LogicalDate time.Time  `json:"logical_date"`
		StartDate   *time.Time `json:"start_date,omitempty"`
		EndDate     *time.Time `json:"end_date,omitempty"`
		TryNumber   int        `json:"try_number"`
		Operator    string     `json:"operator"`
		Pool        string     `json:"pool"`
		Note        string     `json:"note,omitempty"`
	}
	out := []item{}
	var dags []*dag.DAG
	if id := q.Get("dag_id"); id != "" {
		if d, ok := dag.Get(id); ok {
			dags = []*dag.DAG{d}
		}
	} else {
		dags = dag.List()
	}
	a := accessFrom(r)
	for _, d := range dags {
		if !a.CanDAG(engine.ActDagRead, d.ID, d.Tags) {
			continue
		}
		runs, _ := s.Store.ListRunsLite(d.ID)
		for i := len(runs) - 1; i >= 0; i-- {
			run := runs[i]
			for _, t := range d.Tasks() {
				if tf := q.Get("task_id"); tf != "" && tf != t.ID {
					continue
				}
				tis := []*engine.TaskInstance{run.Tasks[t.ID]}
				if kids := run.Mapped[t.ID]; len(kids) > 0 {
					tis = kids
				}
				for _, ti := range tis {
					if ti == nil || (q.Get("state") != "" && string(ti.State) != q.Get("state")) {
						continue
					}
					out = append(out, item{d.ID, t.ID, run.RunID, ti.MapIndex, ti.State, run.LogicalDate,
						ti.StartDate, ti.EndDate, ti.TryNumber, t.Operator(), t.Pool(), ti.Note})
				}
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].LogicalDate.After(out[j].LogicalDate) })
	total := len(out)
	if len(out) > limit {
		out = out[:limit]
	}
	writeJSON(w, 200, map[string]any{"task_instances": out, "total_entries": total})
}
