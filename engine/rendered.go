package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"dagsflow-go/dag"
)

// RenderedView is what the "Rendered Template" tab shows for a task instance.
type RenderedView struct {
	Fields []dag.RenderedField `json:"fields"`
	// recorded: values captured while the task ran; on_the_fly: rendered now
	// from the stored run (the task has not run yet in this run).
	Source string `json:"source"`
	Note   string `json:"note,omitempty"`
}

// RenderedFields returns the rendered template fields of a task instance:
// the values recorded at execution time, or — like Airflow — rendered on the
// fly from the run's context when the task has not run.
func RenderedFields(s *Store, d *dag.DAG, r *DagRun, taskID string, mapIndex int) (RenderedView, error) {
	t, ok := d.Task(taskID)
	if !ok {
		return RenderedView{}, fmt.Errorf("task %s not found", taskID)
	}
	if t.MapSource() != nil && mapIndex < 0 {
		return RenderedView{Fields: []dag.RenderedField{}, Source: "none",
			Note: "This is a mapped task: select a mapped instance to see its rendered template."}, nil
	}
	slot := taskID
	if mapIndex >= 0 {
		slot = mappedKey(taskID, mapIndex)
	}
	if rec := r.Rendered[slot]; len(rec) > 0 {
		v := RenderedView{Source: "recorded", Fields: []dag.RenderedField{}}
		seen := map[string]bool{}
		for _, f := range t.TemplateFields() {
			if val, ok := rec[f.Name]; ok {
				v.Fields = append(v.Fields, dag.RenderedField{Name: f.Name, Value: val, Lang: t.LangOf(f.Name)})
				seen[f.Name] = true
			}
		}
		var extra []string
		for k := range rec {
			if !seen[k] {
				extra = append(extra, k)
			}
		}
		sort.Strings(extra)
		for _, k := range extra {
			v.Fields = append(v.Fields, dag.RenderedField{Name: k, Value: rec[k], Lang: t.LangOf(k)})
		}
		return v, nil
	}

	sr := &storedRun{s: s, d: d, r: r}
	info := dag.RunInfo{RunID: r.RunID, RunType: r.RunType, LogicalDate: r.LogicalDate,
		DataIntervalStart: r.DataIntervalStart, DataIntervalEnd: r.DataIntervalEnd,
		Conf: r.Conf, TryNumber: 1, MapIndex: mapIndex}
	if ti := r.Tasks[taskID]; ti != nil && ti.TryNumber > 0 {
		info.TryNumber = ti.TryNumber
	}
	note := ""
	if mapIndex >= 0 {
		items, err := itemsOf(sr, t)
		switch {
		case err != nil:
			note = "Could not resolve the mapped item: " + err.Error()
		case mapIndex >= len(items):
			note = fmt.Sprintf("map_index %d is out of range (%d items)", mapIndex, len(items))
		default:
			info.Item = items[mapIndex]
		}
	}
	fields := dag.RenderTemplates(dag.NewPreviewContext(t, info, sr))
	if note == "" {
		note = "This task instance has not rendered its templates in this run yet, so they are rendered now from the run's context."
	}
	return RenderedView{Fields: fields, Source: "on_the_fly", Note: note}, nil
}

func itemsOf(rs dag.RunState, t *dag.Task) ([]any, error) {
	src := t.MapSource()
	if !src.IsXCom() {
		return src.Items, nil
	}
	v, ok := rs.XComPull(src.XComTask, src.XComKey)
	if !ok {
		return nil, fmt.Errorf("xcom %s.%s not available yet", src.XComTask, src.XComKey)
	}
	return toList(v)
}

// storedRun is a read-only dag.RunState over a persisted DagRun.
type storedRun struct {
	s *Store
	d *dag.DAG
	r *DagRun
}

var errPreview = errors.New("not available while previewing templates")

func (sr *storedRun) XComPush(string, int, string, any) error { return errPreview }
func (sr *storedRun) TriggerDagRun(context.Context, string, map[string]any, bool) (string, error) {
	return "", errPreview
}
func (sr *storedRun) SetRendered(string, int, string, string) error { return nil }
func (sr *storedRun) ExternalTaskState(dagID, taskID string, logical time.Time) (dag.State, bool, error) {
	return externalTaskState(sr.s, dagID, taskID, logical)
}

func (sr *storedRun) XComPull(taskID, key string) (any, bool) {
	pull := func(id string) (any, bool) {
		if kids, mapped := sr.r.Mapped[id]; mapped {
			out := make([]any, len(kids))
			found := false
			for i := range kids {
				if v, ok := sr.r.XCom[mappedKey(id, i)][key]; ok {
					out[i], found = v, true
				}
			}
			return out, found
		}
		v, ok := sr.r.XCom[id][key]
		return v, ok
	}
	if taskID != "" {
		return pull(taskID)
	}
	for _, t := range sr.d.Tasks() {
		if v, ok := pull(t.ID); ok {
			return v, true
		}
	}
	return nil, false
}
