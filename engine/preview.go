package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"dagsflow-go/dag"
)

// Builder "Run node" previews: run one node of a workflow (given as YAML, not
// registered) with seeded upstream outputs, in memory. Used by the webserver,
// or by a worker when the preview is sent through the task queue.

// PreviewRequest describes one preview.
type PreviewRequest struct {
	DagID string                    `json:"dag_id"`
	YAML  []byte                    `json:"yaml"` // the node and the nodes before it
	Node  string                    `json:"node"`
	Seed  map[string]map[string]any `json:"seed,omitempty"` // XComs known already (slot -> key -> value)
	// Safe lists the upstream nodes that only read data and may run to
	// produce a missing input.
	Safe      []string `json:"safe,omitempty"`
	TimeoutMS int64    `json:"timeout_ms"`
	LogMax    int      `json:"log_max,omitempty"`
}

// PreviewResult is what the builder shows.
type PreviewResult struct {
	Status       string         `json:"status"` // success | failed | needs_input
	Error        string         `json:"error,omitempty"`
	Log          string         `json:"log"`
	DurationMS   int64          `json:"duration_ms"`
	Ran          []string       `json:"ran"`
	Output       any            `json:"output,omitempty"`
	HasOutput    bool           `json:"has_output,omitempty"`
	Selected     []string       `json:"selected,omitempty"`
	ConditionMet *bool          `json:"condition_met,omitempty"`
	Note         string         `json:"note,omitempty"`
	XCom         map[string]any `json:"xcom,omitempty"`
	Where        string         `json:"where,omitempty"` // "webserver" or the worker id
}

// previewRun is the dag.RunState of a preview: XComs in memory.
type previewRun struct {
	mu   sync.Mutex
	xcom map[string]map[string]any
}

func (p *previewRun) XComPush(taskID string, idx int, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("xcom value for %q is not JSON serialisable: %w", key, err)
	}
	var norm any
	_ = json.Unmarshal(b, &norm)
	slot := taskID
	if idx >= 0 {
		slot = fmt.Sprintf("%s#%d", taskID, idx)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.xcom[slot] == nil {
		p.xcom[slot] = map[string]any{}
	}
	p.xcom[slot][key] = norm
	return nil
}

func (p *previewRun) XComPull(taskID, key string) (any, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if m, ok := p.xcom[taskID]; ok {
		v, ok := m[key]
		return v, ok
	}
	// a mapped task: one entry per map index
	var idx []int
	for slot := range p.xcom {
		if t, n, ok := strings.Cut(slot, "#"); ok && t == taskID {
			if i, err := strconv.Atoi(n); err == nil {
				idx = append(idx, i)
			}
		}
	}
	if len(idx) == 0 {
		return nil, false
	}
	sort.Ints(idx)
	out := make([]any, 0, len(idx))
	for _, i := range idx {
		out = append(out, p.xcom[fmt.Sprintf("%s#%d", taskID, i)][key])
	}
	return out, true
}

func (p *previewRun) has(taskID string) bool {
	_, ok := p.XComPull(taskID, "return_value")
	return ok
}

func (p *previewRun) TriggerDagRun(context.Context, string, map[string]any, bool) (string, error) {
	return "", errors.New("Trigger another DAG does not run in a preview")
}

func (p *previewRun) ExternalTaskState(string, string, time.Time) (dag.State, bool, error) {
	return "", false, nil
}
func (p *previewRun) SetRendered(string, int, string, string) error            { return nil }
func (p *previewRun) SetOutletExtra(string, int, string, map[string]any) error { return nil }

// capWriter keeps the first max bytes.
type capWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
	max int
	cut bool
}

func (c *capWriter) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if room := c.max - c.buf.Len(); room > 0 {
		if len(p) > room {
			c.buf.Write(p[:room])
			c.cut = true
		} else {
			c.buf.Write(p)
		}
	} else if len(p) > 0 {
		c.cut = true
	}
	return len(p), nil
}

func (c *capWriter) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.buf.String()
	if c.cut {
		s += "\n… log cut"
	}
	return s
}

// missingUps: upstream nodes of target whose output is still unknown.
func missingUps(d *dag.DAG, target string, rs *previewRun) []string {
	var out []string
	t, _ := d.Task(target)
	seen := map[string]bool{}
	var walk func(*dag.Task)
	walk = func(t *dag.Task) {
		for _, u := range t.Upstream() {
			if seen[u.ID] {
				continue
			}
			seen[u.ID] = true
			if !rs.has(u.ID) {
				out = append(out, u.ID)
				walk(u)
			}
		}
	}
	walk(t)
	return out
}

// neededBy: id is upstream of target through nodes that have no output yet.
func neededBy(d *dag.DAG, id, target string, rs *previewRun) bool {
	for _, m := range missingUps(d, target, rs) {
		if m == id {
			return true
		}
	}
	return false
}

// previewTask runs one task (the first item of a mapped one, one poke of a sensor).
func previewTask(ctx context.Context, t *dag.Task, info dag.RunInfo, rs *previewRun, log *capWriter) (map[string]any, error) {
	extra := map[string]any{}
	if ms := t.MapSource(); ms != nil {
		items := ms.Items
		if ms.IsXCom() {
			v, _ := rs.XComPull(ms.XComTask, firstNonEmptyStr(ms.XComKey, "return_value"))
			if s, ok := v.(string); ok {
				var parsed any
				if json.Unmarshal([]byte(s), &parsed) == nil {
					v = parsed
				}
			}
			items, _ = v.([]any)
		}
		if len(items) == 0 {
			return extra, errors.New("run once per item: there are no items to preview")
		}
		info.MapIndex, info.Item = 0, items[0]
		extra["note"] = fmt.Sprintf("previewing item 1 of %d", len(items))
	}
	tctx := dag.NewTaskContext(ctx, t, info, rs, log)
	var err error
	func() {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic: %v", p)
			}
		}()
		if t.Sensor() != nil {
			var ok bool
			ok, err = t.Poke(tctx)
			extra["condition_met"] = ok
			return
		}
		var sel []string
		sel, err = t.Run(tctx)
		if t.IsBranch() && err == nil {
			if sel == nil {
				sel = []string{}
			}
			extra["selected"] = sel
		}
	}()
	if info.MapIndex >= 0 && err == nil {
		// show the item's output as the node's output
		if v, ok := rs.XComPull(fmt.Sprintf("%s#%d", t.ID, info.MapIndex), "return_value"); ok {
			_ = rs.XComPush(t.ID, -1, "return_value", v)
		}
	}
	return extra, err
}

// RunPreview runs the preview. It never panics; errors are in the result.
func RunPreview(ctx context.Context, req PreviewRequest) (res PreviewResult) {
	res.Status, res.Ran = "success", []string{}
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	logMax := req.LogMax
	if logMax <= 0 {
		logMax = 256 << 10
	}
	log := &capWriter{max: logMax}
	defer func() {
		if p := recover(); p != nil {
			res.Status, res.Error = "failed", fmt.Sprintf("panic: %v", p)
		}
		res.Log = log.String()
	}()
	fail := func(status, msg string) PreviewResult {
		res.Status, res.Error = status, msg
		return res
	}
	dags, err := dag.ParseYAML(req.DagID+".yaml", req.YAML)
	if err != nil {
		return fail("failed", strings.TrimPrefix(err.Error(), fmt.Sprintf("dag %q: ", req.DagID)))
	}
	d := dags[0]
	if err := d.Validate(); err != nil {
		return fail("failed", err.Error())
	}
	target, ok := d.Task(req.Node)
	if !ok {
		return fail("failed", fmt.Sprintf("node %q is not in the workflow", req.Node))
	}
	rs := &previewRun{xcom: map[string]map[string]any{}}
	for slot, kv := range req.Seed {
		rs.xcom[slot] = map[string]any{}
		for k, v := range kv {
			rs.xcom[slot][k] = v
		}
	}
	safe := map[string]bool{}
	for _, id := range req.Safe {
		safe[id] = true
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	now := time.Now()
	info := dag.RunInfo{RunID: "preview__" + now.UTC().Format(time.RFC3339Nano), RunType: "preview",
		LogicalDate: now, DataIntervalStart: now, DataIntervalEnd: now, TryNumber: 1, MapIndex: -1, Conf: map[string]any{}}
	for _, t := range d.Tasks() {
		if t.ID == target.ID || rs.has(t.ID) || !neededBy(d, t.ID, target.ID, rs) {
			continue
		}
		if !safe[t.ID] {
			return fail("needs_input", fmt.Sprintf("%s has no output yet: run it with \"Run node\" first, or run the whole workflow once (it is not run automatically because it may change data)", t.ID))
		}
		fmt.Fprintf(log, "--- %s (input of %s) ---\n", t.ID, target.ID)
		if _, err := previewTask(ctx, t, info, rs, log); err != nil {
			res.Ran = append(res.Ran, t.ID)
			return fail("failed", fmt.Sprintf("%s (needed as input): %v", t.ID, err))
		}
		res.Ran = append(res.Ran, t.ID)
	}
	if len(res.Ran) > 0 {
		fmt.Fprintf(log, "--- %s ---\n", target.ID)
	}
	start := time.Now()
	extra, err := previewTask(ctx, target, info, rs, log)
	res.DurationMS = time.Since(start).Milliseconds()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("stopped after %s (preview limit)", timeout)
	}
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
	}
	if v, ok := extra["selected"].([]string); ok {
		res.Selected = v
	}
	if v, ok := extra["condition_met"].(bool); ok {
		res.ConditionMet = &v
	}
	if v, ok := extra["note"].(string); ok {
		res.Note = v
	}
	if v, ok := rs.XComPull(target.ID, "return_value"); ok {
		res.Output, res.HasOutput = v, true
	}
	rs.mu.Lock()
	for k, v := range rs.xcom[target.ID] {
		if k != "return_value" {
			if res.XCom == nil {
				res.XCom = map[string]any{}
			}
			res.XCom[k] = v
		}
	}
	rs.mu.Unlock()
	return res
}

// PreviewWorkers reports whether an alive worker serves the queue.
func (s *Store) PreviewWorkers(queue string) bool {
	if !s.Distributed() {
		return false
	}
	cs, err := s.Components()
	if err != nil {
		return false
	}
	for _, c := range cs {
		if c.Kind != "worker" || !c.Alive() {
			continue
		}
		if _, oneJob := c.Info["job"]; oneJob { // a Kubernetes pod running one job
			continue
		}
		qs, _ := c.Info["queues"].([]any)
		for _, q := range qs {
			if fmt.Sprint(q) == queue {
				return true
			}
		}
	}
	return false
}

// RunPreviewOnWorker sends the preview through the task queue and waits for a
// worker to run it.
func (s *Store) RunPreviewOnWorker(ctx context.Context, queue string, req PreviewRequest) (PreviewResult, error) {
	id, err := s.be().enqueueJob(&TaskJob{Queue: queue, State: JobQueued, Payload: JobPayload{
		DagID: req.DagID, RunID: "preview__" + strconv.FormatInt(time.Now().UnixNano(), 36), RunType: "preview",
		TaskID: req.Node, MapIndex: -1, Try: 1, Preview: &req}})
	if err != nil {
		return PreviewResult{}, err
	}
	wait := time.Duration(req.TimeoutMS)*time.Millisecond + 30*time.Second
	deadline := time.Now().Add(wait)
	for {
		j, err := s.be().getJob(id)
		if err == nil && j != nil {
			switch j.State {
			case JobSuccess, JobFailed:
				if j.Result != nil && j.Result.Preview != nil {
					r := *j.Result.Preview
					r.Where = firstNonEmptyStr(j.Worker, "worker")
					return r, nil
				}
				msg := "the worker returned no preview (is it running an older dagsflow?)"
				if j.Result != nil && j.Result.Error != "" {
					msg = j.Result.Error
				}
				return PreviewResult{Status: "failed", Error: msg, Ran: []string{}}, nil
			case JobCancelled:
				return PreviewResult{Status: "failed", Error: "the preview was cancelled", Ran: []string{}}, nil
			}
		}
		select {
		case <-ctx.Done():
			_ = s.be().cancelJob(id)
			return PreviewResult{}, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			_ = s.be().cancelJob(id)
			state := "queued"
			if j != nil {
				state = j.State
			}
			return PreviewResult{Status: "failed", Error: fmt.Sprintf("no worker finished the preview in time (job %d is %s)", id, state), Ran: []string{}}, nil
		}
	}
}

func firstNonEmptyStr(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}
