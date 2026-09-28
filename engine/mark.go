package engine

import (
	"fmt"
	"sort"
	"time"

	"dagsflow-go/dag"
)

// MarkRequest ~ Airflow "Mark state as success/failed" on a task instance or a DagRun.
type MarkRequest struct {
	Kind       string    `json:"kind,omitempty"` // "" = mark state, "note" = set Note
	Note       string    `json:"note,omitempty"`
	Target     string    `json:"target"` // "task" or "run"
	TaskID     string    `json:"task_id,omitempty"`
	MapIndex   int       `json:"map_index"` // -1 = the whole (mapped) task
	State      dag.State `json:"state"`     // success | failed
	Upstream   bool      `json:"include_upstream,omitempty"`
	Downstream bool      `json:"include_downstream,omitempty"`
	User       string    `json:"user,omitempty"`
	At         time.Time `json:"at"`
}

func (m MarkRequest) Validate(d *dag.DAG) error {
	if m.Kind == "note" {
		if m.Target == "task" {
			if _, ok := d.Task(m.TaskID); !ok {
				return fmt.Errorf("task %q not found in dag %s", m.TaskID, d.ID)
			}
		}
		if len(m.Note) > 10000 {
			return fmt.Errorf("note is too long (max 10000 characters)")
		}
		return nil
	}
	if m.State != dag.StateSuccess && m.State != dag.StateFailed {
		return fmt.Errorf("state must be success or failed")
	}
	switch m.Target {
	case "run":
	case "task":
		t, ok := d.Task(m.TaskID)
		if !ok {
			return fmt.Errorf("task %q not found in dag %s", m.TaskID, d.ID)
		}
		if m.MapIndex >= 0 && t.MapSource() == nil {
			return fmt.Errorf("task %s is not mapped", m.TaskID)
		}
	default:
		return fmt.Errorf("target must be task or run")
	}
	return nil
}

// markTargets: the task plus (optionally) all its ancestors / descendants.
func markTargets(d *dag.DAG, m MarkRequest) map[string]bool {
	sel := map[string]bool{}
	t, _ := d.Task(m.TaskID)
	var walk func(x *dag.Task, up bool)
	walk = func(x *dag.Task, up bool) {
		next := x.Downstream()
		if up {
			next = x.Upstream()
		}
		for _, n := range next {
			if !sel[n.ID] {
				sel[n.ID] = true
				walk(n, up)
			}
		}
	}
	sel[t.ID] = true
	if m.Upstream {
		walk(t, true)
	}
	if m.Downstream {
		walk(t, false)
	}
	return sel
}

// descendants of the given tasks that are not themselves selected.
func descendantsOf(d *dag.DAG, sel map[string]bool) []string {
	seen := map[string]bool{}
	var out []string
	var walk func(x *dag.Task)
	walk = func(x *dag.Task) {
		for _, n := range x.Downstream() {
			if !seen[n.ID] && !sel[n.ID] {
				seen[n.ID] = true
				out = append(out, n.ID)
			}
			walk(n)
		}
	}
	for id := range sel {
		t, _ := d.Task(id)
		walk(t)
	}
	return out
}

func markTI(ti *TaskInstance, s dag.State, user string) {
	now := time.Now()
	ti.State = s
	ti.EndDate = &now
	if ti.StartDate == nil {
		ti.StartDate = &now
	}
	ti.Error = fmt.Sprintf("marked %s manually by %s", s, user)
	if s == dag.StateSuccess {
		ti.Error = ""
	}
}

func resetTI(d *dag.DAG, r *DagRun, id string) {
	if t, ok := d.Task(id); ok {
		clearTask(r, t, -1, false)
	}
}

// finalRunState: like Airflow, a finished run fails if any leaf failed.
func finalRunState(d *dag.DAG, r *DagRun) string {
	for _, t := range d.Tasks() {
		if len(t.Downstream()) == 0 {
			switch r.Tasks[t.ID].State {
			case dag.StateFailed, dag.StateUpstreamFailed, dag.StateNone:
				return RunFailed
			}
		}
	}
	return RunSuccess
}

func aggregateState(kids []*TaskInstance) (dag.State, bool) {
	var succ, fail int
	for _, c := range kids {
		if !c.State.Finished() {
			return "", false
		}
		switch c.State {
		case dag.StateSuccess:
			succ++
		case dag.StateFailed, dag.StateUpstreamFailed:
			fail++
		}
	}
	switch {
	case fail > 0:
		return dag.StateFailed, true
	case succ > 0:
		return dag.StateSuccess, true
	}
	return dag.StateSkipped, true
}

// SetNote sets a DagRun or task instance note ("" clears it).
func SetNote(d *dag.DAG, r *DagRun, m MarkRequest) error {
	if err := m.Validate(d); err != nil {
		return err
	}
	if m.Target == "run" {
		r.Note = m.Note
		return nil
	}
	ti := r.Tasks[m.TaskID]
	if m.MapIndex >= 0 {
		kids := r.Mapped[m.TaskID]
		if m.MapIndex >= len(kids) {
			return fmt.Errorf("map_index %d out of range", m.MapIndex)
		}
		ti = kids[m.MapIndex]
	}
	if ti == nil {
		return fmt.Errorf("task instance %s does not exist in this run", m.TaskID)
	}
	ti.Note = m.Note
	return nil
}

// MarkState applies a mark to a run that is NOT being executed right now
// (finished or not yet started). Like Airflow's set_task_instance_state, the
// downstream tasks that failed / upstream_failed are cleared and the run is
// re-queued so they run again. Returns the affected task ids.
func MarkState(d *dag.DAG, r *DagRun, m MarkRequest) ([]string, error) {
	if err := m.Validate(d); err != nil {
		return nil, err
	}
	now := time.Now()
	for _, t := range d.Tasks() {
		if r.Tasks[t.ID] == nil {
			r.Tasks[t.ID] = &TaskInstance{TaskID: t.ID, MapIndex: -1}
		}
	}
	var changed []string
	if m.Target == "run" {
		for _, t := range d.Tasks() {
			tis := append([]*TaskInstance{r.Tasks[t.ID]}, r.Mapped[t.ID]...)
			for _, ti := range tis {
				switch {
				case m.State == dag.StateSuccess && ti.State != dag.StateSuccess && ti.State != dag.StateSkipped:
					markTI(ti, dag.StateSuccess, m.User)
				case m.State == dag.StateFailed && ti.State == dag.StateNone:
					markTI(ti, dag.StateSkipped, m.User)
				case m.State == dag.StateFailed && !ti.State.Finished():
					markTI(ti, dag.StateFailed, m.User)
				default:
					continue
				}
				changed = append(changed, t.ID)
			}
		}
		r.State = string(m.State)
		if r.StartDate == nil {
			r.StartDate = &now
		}
		r.EndDate = &now
		return dedupe(changed), nil
	}

	sel := markTargets(d, m)
	for id := range sel {
		kids := r.Mapped[id]
		switch {
		case id == m.TaskID && m.MapIndex >= 0:
			if m.MapIndex >= len(kids) {
				return nil, fmt.Errorf("map_index %d out of range (%d instances)", m.MapIndex, len(kids))
			}
			markTI(kids[m.MapIndex], m.State, m.User)
			if st, done := aggregateState(kids); done {
				markTI(r.Tasks[id], st, m.User)
			}
		default:
			for _, k := range kids {
				markTI(k, m.State, m.User)
			}
			markTI(r.Tasks[id], m.State, m.User)
		}
		changed = append(changed, id)
	}
	requeue := false
	for _, id := range descendantsOf(d, sel) {
		if s := r.Tasks[id].State; s == dag.StateFailed || s == dag.StateUpstreamFailed {
			resetTI(d, r, id)
			changed = append(changed, id)
			requeue = true
		}
	}
	if !allFinished(r) {
		requeue = true
	}
	if requeue {
		r.State, r.EndDate = RunQueued, nil
	} else if r.State == RunSuccess || r.State == RunFailed {
		r.State = finalRunState(d, r)
		r.EndDate = &now
	}
	sort.Strings(changed)
	return dedupe(changed), nil
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// ---- command queue for runs the scheduler is executing ------------------------

// EnqueueMark hands a mark request to the scheduler that is executing the run.
func (s *Store) EnqueueMark(dagID, runID string, m MarkRequest) error {
	if m.At.IsZero() {
		m.At = time.Now()
	}
	return s.be().enqueueCommand(dagID, runID, m)
}

// TakeMarks returns and removes pending mark requests for a run (oldest first).
func (s *Store) TakeMarks(dagID, runID string) []MarkRequest {
	return s.be().takeCommands(dagID, runID)
}

// DropCommands removes leftovers once a run is done.
func (s *Store) DropCommands(dagID, runID string) { s.be().dropCommands(dagID, runID) }
