package web

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

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
	d, run, ok := s.runOr404(w, r)
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
	evs, err := s.Store.AuditEvents(q.Get("dag_id"), q.Get("run_id"), q.Get("task_id"), limit)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"event_logs": evs})
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
		fail(w, 500, "%v", err)
		return
	}
	s.audit(r, "mark_"+string(m.State), d.ID, run.RunID, m.TaskID, extra)
	writeJSON(w, 200, map[string]any{"changed": changed, "dag_run_state": run.State})
}
