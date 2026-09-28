package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// TemplateField ~ an entry of Airflow's template_fields: a templated attribute
// of a task. The built-in operators declare theirs (bash_command, sql, url, ...);
// custom Go tasks can declare their own with WithTemplateField and render them
// with ctx.RenderTemplateField. The UI's "Rendered Template" tab shows them.
type TemplateField struct {
	Name string `json:"name"`
	Raw  string `json:"raw,omitempty"`
	File string `json:"file,omitempty"` // template read from this file at render time (e.g. a .sql file)
	Lang string `json:"lang,omitempty"` // bash | sql | json | text: syntax highlighting in the UI
	// Map: every string value is a template; the result is shown as JSON
	// (e.g. bash env, TriggerDagRun conf).
	Map map[string]any `json:"map,omitempty"`
	// Vars are rendered first and exposed at the top level of the template
	// (BigQuery params: {{ .target_table }}).
	Vars map[string]any `json:"vars,omitempty"`
}

// RenderedField is one rendered template field.
type RenderedField struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Lang  string `json:"lang,omitempty"`
	Error string `json:"error,omitempty"`
}

// WithTemplateField declares a templated field on a custom task so it shows up
// in the Rendered Template view. Render it inside the task with
// ctx.RenderTemplateField(name).
func WithTemplateField(name, raw, lang string) TaskOption {
	return WithTemplateFields(TemplateField{Name: name, Raw: raw, Lang: lang})
}

// WithTemplateFields declares several template fields (used by operators and
// by the Go DAG bundle loader).
func WithTemplateFields(fields ...TemplateField) TaskOption {
	return func(t *Task) {
		for _, f := range fields {
			replaced := false
			for i := range t.templates {
				if t.templates[i].Name == f.Name {
					t.templates[i], replaced = f, true
				}
			}
			if !replaced {
				t.templates = append(t.templates, f)
			}
		}
	}
}

func (t *Task) TemplateFields() []TemplateField { return t.templates }

func (t *Task) templateField(name string) (TemplateField, bool) {
	for _, f := range t.templates {
		if f.Name == name {
			return f, true
		}
	}
	return TemplateField{}, false
}

// render renders one template field for this task instance.
func (c *TaskContext) render(f TemplateField) (string, error) {
	if f.Map != nil {
		out, err := renderMap(c, f.Map)
		if err != nil {
			return "", err
		}
		b, err := json.MarshalIndent(out, "", "  ")
		return string(b), err
	}
	raw := f.Raw
	if f.File != "" {
		b, err := os.ReadFile(f.File)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", f.File, err)
		}
		raw = string(b)
	}
	var extra map[string]any
	if f.Vars != nil {
		v, err := renderMap(c, f.Vars)
		if err != nil {
			return "", err
		}
		extra = v
		// like Airflow, operator params are merged into {{ params }}
		if _, own := v["params"]; !own {
			merged := map[string]any{}
			for k, x := range c.Params {
				merged[k] = x
			}
			for k, x := range v {
				merged[k] = x
			}
			extra = map[string]any{"params": merged}
			for k, x := range v {
				extra[k] = x
			}
		}
	}
	return c.Render(raw, extra)
}

// RecordRendered stores a rendered value for the Rendered Template view.
func (c *TaskContext) RecordRendered(name, value string) {
	if c.run != nil {
		_ = c.run.SetRendered(c.Task.ID, c.MapIndex, name, value)
	}
}

// RenderTemplateField renders a declared template field and records the
// result (what the UI shows as "Rendered Template").
func (c *TaskContext) RenderTemplateField(name string) (string, error) {
	f, ok := c.Task.templateField(name)
	if !ok {
		return "", fmt.Errorf("task %s has no template field %q (declare it with dag.WithTemplateField)", c.Task.ID, name)
	}
	v, err := c.render(f)
	if err != nil {
		return "", fmt.Errorf("render %s: %w", name, err)
	}
	c.RecordRendered(name, v)
	return v, nil
}

// RenderAndRecord renders an ad-hoc template and records it under name, for
// custom tasks that build templates dynamically.
func (c *TaskContext) RenderAndRecord(name, raw string) (string, error) {
	v, err := c.Render(raw, nil)
	if err == nil {
		c.RecordRendered(name, v)
	}
	return v, err
}

// RenderTemplates renders every declared template field without running the
// task (Airflow's "rendered on the fly"). Errors are reported per field.
func RenderTemplates(c *TaskContext) []RenderedField {
	out := []RenderedField{}
	for _, f := range c.Task.templates {
		v, err := c.render(f)
		rf := RenderedField{Name: f.Name, Value: v, Lang: f.Lang}
		if err != nil {
			rf.Error = err.Error()
		}
		out = append(out, rf)
	}
	return out
}

// LangOf returns the highlighting language of a (possibly undeclared) field.
func (t *Task) LangOf(name string) string {
	if f, ok := t.templateField(name); ok && f.Lang != "" {
		return f.Lang
	}
	switch n := strings.ToLower(name); {
	case strings.Contains(n, "sql"), strings.Contains(n, "query"):
		return "sql"
	case strings.Contains(n, "bash"), strings.Contains(n, "command"):
		return "bash"
	}
	return "text"
}

// NewPreviewContext builds a TaskContext for rendering templates outside of a
// real execution (UI preview / `tasks render`).
func NewPreviewContext(t *Task, info RunInfo, rs RunState) *TaskContext {
	return NewTaskContext(context.Background(), t, info, rs, io.Discard)
}
