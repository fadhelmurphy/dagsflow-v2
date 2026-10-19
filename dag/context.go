package dag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"text/template"
	"time"
)

// RunState is implemented by the engine; it gives a task access to its DagRun.
type RunState interface {
	XComPush(taskID string, mapIndex int, key string, value any) error
	// XComPull with taskID "" searches all tasks of the run (in topological order).
	// For a mapped task it returns a []any with one entry per map index.
	XComPull(taskID, key string) (any, bool)
	TriggerDagRun(ctx context.Context, dagID string, conf map[string]any, wait bool) (string, error)
	// ExternalTaskState: state of taskID (or of the whole run when "") in the run
	// of dagID with the given logical date.
	ExternalTaskState(dagID, taskID string, logical time.Time) (State, bool, error)
	// SetRendered records a rendered template field (Rendered Template view).
	SetRendered(taskID string, mapIndex int, field, value string) error
	// SetOutletExtra sets the extra of the dataset event emitted on success.
	SetOutletExtra(taskID string, mapIndex int, uri string, extra map[string]any) error
}

// RunInfo is the per-run metadata exposed to tasks (Airflow's context / macros).
type RunInfo struct {
	RunID             string
	RunType           string
	LogicalDate       time.Time
	DataIntervalStart time.Time
	DataIntervalEnd   time.Time
	Conf              map[string]any
	TryNumber         int
	MapIndex          int // -1 unless the task is mapped
	Item              any // the element for this map index
}

// TaskContext is what every task receives. It embeds context.Context, so it is
// cancelled on timeout or scheduler shutdown: pass it to DB/HTTP calls.
type TaskContext struct {
	context.Context
	RunInfo
	DAG    *DAG
	Task   *Task
	Params map[string]any // dag params overridden by dag_run.conf

	run    RunState
	out    io.Writer
	logger *log.Logger
}

func NewTaskContext(parent context.Context, t *Task, info RunInfo, rs RunState, logOut io.Writer) *TaskContext {
	params := map[string]any{}
	for k, v := range t.dag.Params {
		params[k] = v
	}
	for k, v := range info.Conf {
		params[k] = v
	}
	if info.Conf == nil {
		info.Conf = map[string]any{}
	}
	return &TaskContext{
		Context: parent, RunInfo: info, DAG: t.dag, Task: t, Params: params,
		run: rs, out: logOut, logger: log.New(logOut, "", log.LstdFlags),
	}
}

func (c *TaskContext) Logf(format string, args ...any)   { c.logger.Printf("INFO - "+format, args...) }
func (c *TaskContext) Errorf(format string, args ...any) { c.logger.Printf("ERROR - "+format, args...) }

// LogWriter is the raw task log (e.g. for subprocess stdout).
func (c *TaskContext) LogWriter() io.Writer { return c.out }

// XComPush stores a JSON-serialisable value for this task instance.
func (c *TaskContext) XComPush(key string, value any) error {
	return c.run.XComPush(c.Task.ID, c.MapIndex, key, value)
}

// XComPull returns the raw value pushed by taskID under key ("" = any task).
func (c *TaskContext) XComPull(taskID, key string) any {
	v, _ := c.run.XComPull(taskID, key)
	return v
}

// TriggerDagRun ~ TriggerDagRunOperator. wait=true blocks until the run finishes
// and returns an error if it failed.
func (c *TaskContext) TriggerDagRun(dagID string, conf map[string]any, wait bool) (string, error) {
	return c.run.TriggerDagRun(c, dagID, conf, wait)
}

// XComLookup is XComPull that also reports whether the value exists.
func (c *TaskContext) XComLookup(taskID, key string) (any, bool) { return c.run.XComPull(taskID, key) }

// ExternalTaskState: state of a task (or the whole run when taskID is "") of
// another DAG's run with the given logical date.
func (c *TaskContext) ExternalTaskState(dagID, taskID string, logical time.Time) (State, bool, error) {
	return c.run.ExternalTaskState(dagID, taskID, logical)
}

// XComPullAs decodes an XCom value into T. XComs are stored as JSON (so they
// survive restarts), which means numbers come back as float64 unless decoded.
func XComPullAs[T any](c *TaskContext, taskID, key string) (T, error) {
	var out T
	v, ok := c.run.XComPull(taskID, key)
	if !ok {
		return out, fmt.Errorf("xcom %s.%s not found", taskID, key)
	}
	return convert[T](v)
}

// ParamAs decodes a param (dag params / dag_run.conf) into T.
func ParamAs[T any](c *TaskContext, key string) (T, error) {
	var out T
	v, ok := c.Params[key]
	if !ok {
		return out, fmt.Errorf("param %q not set", key)
	}
	return convert[T](v)
}

// ItemAs decodes the mapped item into T.
func ItemAs[T any](c *TaskContext) (T, error) { return convert[T](c.Item) }

func convert[T any](v any) (T, error) {
	var out T
	if t, ok := v.(T); ok {
		return t, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(b, &out)
	return out, err
}

// Render renders a Go template with Airflow-like macros:
//
//	{{ .ds }} {{ .ds_nodash }} {{ .ts }} {{ .run_id }} {{ .dag_id }} {{ .task_id }}
//	{{ .data_interval_start }} {{ .data_interval_end }} {{ .params.x }} {{ .conf.x }}
//	{{ .item }} {{ .map_index }}  {{ xcom_pull "task_id" "key" }}  {{ xcom "key" }}
//	{{ var "key" }}  {{ ds_add .ds -1 }}
//
// Keys in extra are also available at top level ({{ .target_table }}).
func (c *TaskContext) Render(raw string, extra map[string]any) (string, error) {
	return c.renderDepth(raw, extra, 0)
}

func (c *TaskContext) renderDepth(raw string, extra map[string]any, depth int) (string, error) {
	data := map[string]any{
		"ds":                  c.LogicalDate.Format("2006-01-02"),
		"ds_nodash":           c.LogicalDate.Format("20060102"),
		"ts":                  c.LogicalDate.Format(time.RFC3339),
		"ts_nodash":           c.LogicalDate.Format("20060102T150405"),
		"logical_date":        c.LogicalDate,
		"data_interval_start": c.DataIntervalStart.Format(time.RFC3339),
		"data_interval_end":   c.DataIntervalEnd.Format(time.RFC3339),
		"run_id":              c.RunID,
		"dag_id":              c.DAG.ID,
		"task_id":             c.Task.ID,
		"try_number":          c.TryNumber,
		"map_index":           c.MapIndex,
		"item":                c.Item,
		"params":              c.Params,
		"conf":                c.Conf,
	}
	for k, v := range extra {
		data[k] = v
	}
	funcs := jinjaFuncs(c, depth)
	for k, v := range (template.FuncMap{
		"xcom_pull": func(taskID, key string) any { return c.XComPull(taskID, key) },
		"xcom":      func(key string) any { return c.XComPull("", key) },
		"var":       GetVariable,
		"json": func(v any) (string, error) {
			b, err := json.Marshal(v)
			return string(b), err
		},
		"ds_add": func(ds string, days any) (string, error) {
			t, err := time.Parse("2006-01-02", ds)
			if err != nil {
				return "", err
			}
			return t.AddDate(0, 0, jtoInt(days)).Format("2006-01-02"), nil
		},
	}) {
		funcs[k] = v
	}
	known := make(map[string]bool, len(data))
	for k := range data {
		known[k] = true
	}
	src, err := jinjaTranslate(raw, known)
	if err != nil {
		return "", fmt.Errorf("template: %w", err)
	}
	tmpl, err := template.New(c.Task.ID).Option("missingkey=error").Funcs(funcs).Parse(src)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}
