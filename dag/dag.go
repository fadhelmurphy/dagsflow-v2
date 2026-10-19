package dag

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
)

const (
	DefaultPool  = "default_pool"
	DefaultQueue = "default"
)

// DefaultArgs mirrors Airflow's default_args: applied to every task unless overridden.
type DefaultArgs struct {
	Owner          string // shown in the UI (default "dagsflow")
	Retries        int
	RetryDelay     time.Duration
	Timeout        time.Duration // execution_timeout
	Pool           string
	Queue          string // executor queue (worker executor); default "default"
	PriorityWeight int
	SLA            time.Duration
	OnFailure      []Callback
	OnSuccess      []Callback
	OnRetry        []Callback
}

// DAG is a pure definition (like an Airflow DAG file). It holds NO runtime state;
// every execution lives in its own DagRun inside the engine.
type DAG struct {
	ID            string
	Schedule      string // cron expr, @daily, @every 5m, or "" (manual / dataset-driven)
	StartDate     time.Time
	Catchup       bool
	MaxActiveRuns int
	Params        map[string]any
	DefaultArgs   DefaultArgs
	Description   string
	Tags          []string
	Datasets      []string // data-aware schedule: run when all of these were updated

	OnSuccess []Callback // DagRun succeeded
	OnFailure []Callback // DagRun failed
	OnSLAMiss []Callback // a task missed its SLA

	Source   string    // "go" (compiled in), a YAML file, or a Go bundle (file/dir) in the DAG folder
	File     string    // file that defines the DAG (for the Code view)
	ParsedAt time.Time // when this definition was (re)loaded

	// compiledIn: registered with Register (a Go DAG compiled into this
	// binary), so a child process of the binary has the same DAG.
	compiledIn bool

	tasks    []*Task
	taskMap  map[string]*Task
	errs     []error // definition errors, reported by validate()
	topo     []*Task
	schedule cron.Schedule
}

type DAGOption func(*DAG)

func WithStartDate(t time.Time) DAGOption     { return func(d *DAG) { d.StartDate = t } }
func WithCatchup(b bool) DAGOption            { return func(d *DAG) { d.Catchup = b } }
func WithMaxActiveRuns(n int) DAGOption       { return func(d *DAG) { d.MaxActiveRuns = n } }
func WithParams(p map[string]any) DAGOption   { return func(d *DAG) { d.Params = p } }
func WithDefaultArgs(a DefaultArgs) DAGOption { return func(d *DAG) { d.DefaultArgs = a } }
func WithDescription(s string) DAGOption      { return func(d *DAG) { d.Description = s } }
func WithTags(tags ...string) DAGOption       { return func(d *DAG) { d.Tags = tags } }
func WithOnSuccess(cb ...Callback) DAGOption {
	return func(d *DAG) { d.OnSuccess = append(d.OnSuccess, cb...) }
}
func WithOnFailure(cb ...Callback) DAGOption {
	return func(d *DAG) { d.OnFailure = append(d.OnFailure, cb...) }
}
func WithSLAMiss(cb ...Callback) DAGOption {
	return func(d *DAG) { d.OnSLAMiss = append(d.OnSLAMiss, cb...) }
}
func WithDatasetSchedule(uris ...string) DAGOption { return func(d *DAG) { d.Datasets = uris } }

// NewDAG creates a DAG definition. schedule "" means manual trigger only (schedule=None),
// unless WithDatasetSchedule is used.
func NewDAG(id, schedule string, opts ...DAGOption) *DAG {
	d := &DAG{
		ID: id, Schedule: schedule, MaxActiveRuns: 1, Source: "go",
		Params: map[string]any{}, taskMap: map[string]*Task{},
	}
	if _, file, _, ok := runtime.Caller(1); ok {
		d.File = file
	}
	for _, o := range opts {
		o(d)
	}
	if d.Params == nil {
		d.Params = map[string]any{}
	}
	if d.MaxActiveRuns < 1 {
		d.MaxActiveRuns = 1
	}
	return d
}

// ---- Tasks -------------------------------------------------------------------

type TaskFunc func(ctx *TaskContext) error
type BranchFunc func(ctx *TaskContext) ([]string, error)

// MapSource defines what a mapped task expands over (Airflow dynamic task mapping).
type MapSource struct {
	Items    []any
	XComTask string
	XComKey  string
}

func FromItems(items ...any) MapSource      { return MapSource{Items: items} }
func FromXCom(taskID, key string) MapSource { return MapSource{XComTask: taskID, XComKey: key} }
func (m MapSource) IsXCom() bool            { return m.XComTask != "" }

type Task struct {
	ID          string
	dag         *DAG
	execute     TaskFunc
	branch      BranchFunc
	sensor      *SensorSpec
	triggerRule TriggerRule
	retries     int
	retryDelay  time.Duration
	timeout     time.Duration
	pool        string
	poolSlots   int
	queue       string
	podOverride map[string]any
	priority    int
	sla         time.Duration
	outlets     []string
	mapSource   *MapSource
	templates   []TemplateField
	operator    string
	extraLinks  []ExtraLink
	onSuccess   []Callback
	onFailure   []Callback
	onRetry     []Callback
	upstream    []*Task
	downstream  []*Task
}

type TaskOption func(*Task)

func WithTriggerRule(r TriggerRule) TaskOption { return func(t *Task) { t.triggerRule = r } }
func WithTimeout(d time.Duration) TaskOption   { return func(t *Task) { t.timeout = d } }
func WithRetries(n int, delay time.Duration) TaskOption {
	return func(t *Task) { t.retries, t.retryDelay = n, delay }
}
func WithPool(name string) TaskOption { return func(t *Task) { t.pool = name } }
func WithPoolSlots(n int) TaskOption  { return func(t *Task) { t.poolSlots = n } }

// WithQueue routes the task to workers listening on that queue (worker
// executor, ~ Airflow's queue with the CeleryExecutor).
func WithQueue(name string) TaskOption { return func(t *Task) { t.queue = name } }

// WithPodOverride is merged into the pod of this task with the Kubernetes
// executor (~ executor_config={"pod_override": ...}), e.g.
//
//	dag.WithPodOverride(map[string]any{"spec": map[string]any{"containers": []any{
//		map[string]any{"name": "base", "resources": map[string]any{"requests": map[string]any{"memory": "2Gi"}}}}}})
func WithPodOverride(pod map[string]any) TaskOption { return func(t *Task) { t.podOverride = pod } }
func WithPriorityWeight(n int) TaskOption           { return func(t *Task) { t.priority = n } }
func WithSLA(d time.Duration) TaskOption            { return func(t *Task) { t.sla = d } }
func WithOutlets(uris ...string) TaskOption {
	return func(t *Task) { t.outlets = append(t.outlets, uris...) }
}
func WithOnSuccessCallback(cb ...Callback) TaskOption {
	return func(t *Task) { t.onSuccess = append(t.onSuccess, cb...) }
}
func WithOnFailureCallback(cb ...Callback) TaskOption {
	return func(t *Task) { t.onFailure = append(t.onFailure, cb...) }
}
func WithOnRetryCallback(cb ...Callback) TaskOption {
	return func(t *Task) { t.onRetry = append(t.onRetry, cb...) }
}

// Expand turns the task into a mapped task: one task instance per item
// (Airflow's .expand()). Inside the task, use ctx.Item / ctx.MapIndex.
// FromXCom automatically adds the source task as an upstream dependency.
func Expand(src MapSource) TaskOption { return func(t *Task) { s := src; t.mapSource = &s } }

// ExtraLink ~ Airflow operator extra links: a button in the task instance
// details whose URL is read from an XCom of that task instance.
type ExtraLink struct {
	Name    string `json:"name"`
	XComKey string `json:"xcom_key"`
}

// WithOperatorName sets the operator name shown in the UI (e.g. "MyExportOperator").
func WithOperatorName(name string) TaskOption { return func(t *Task) { t.operator = name } }

// WithExtraLink adds an extra link; the task pushes the URL with ctx.XComPush(xcomKey, url).
func WithExtraLink(name, xcomKey string) TaskOption {
	return func(t *Task) {
		for i := range t.extraLinks {
			if t.extraLinks[i].Name == name {
				t.extraLinks[i].XComKey = xcomKey
				return
			}
		}
		t.extraLinks = append(t.extraLinks, ExtraLink{name, xcomKey})
	}
}

func (t *Task) Operator() string        { return t.operator }
func (t *Task) ExtraLinks() []ExtraLink { return t.extraLinks }

func (d *DAG) addTask(id string, fn TaskFunc, br BranchFunc, sn *SensorSpec, opts []TaskOption) *Task {
	da := d.DefaultArgs
	t := &Task{
		ID: id, dag: d, execute: fn, branch: br, sensor: sn,
		triggerRule: AllSuccess,
		retries:     da.Retries, retryDelay: da.RetryDelay, timeout: da.Timeout,
		pool: da.Pool, poolSlots: 1, queue: da.Queue, priority: da.PriorityWeight, sla: da.SLA,
		onSuccess: append([]Callback(nil), da.OnSuccess...),
		onFailure: append([]Callback(nil), da.OnFailure...),
		onRetry:   append([]Callback(nil), da.OnRetry...),
	}
	switch {
	case sn != nil:
		t.operator = "GoSensor"
	case br != nil:
		t.operator = "BranchGoOperator"
	default:
		t.operator = "GoOperator"
	}
	for _, o := range opts {
		o(t)
	}
	if t.pool == "" {
		t.pool = DefaultPool
	}
	if t.poolSlots < 1 {
		t.poolSlots = 1
	}
	if t.queue == "" {
		t.queue = DefaultQueue
	}
	if t.priority == 0 {
		t.priority = 1
	}
	switch _, dup := d.taskMap[id]; {
	case strings.TrimSpace(id) == "":
		d.buildErr(fmt.Errorf("a task has an empty id"))
		return t // detached: linking it is harmless, the DAG will not load
	case dup:
		d.buildErr(fmt.Errorf("duplicate task id %q", id))
		return t
	}
	d.tasks = append(d.tasks, t)
	d.taskMap[id] = t
	return t
}

// buildErr records a definition error. The DAG then shows up as an import
// error instead of crashing the process (DAG files must never take down
// the scheduler or the webserver).
func (d *DAG) buildErr(err error) {
	if d != nil {
		d.errs = append(d.errs, err)
	}
}

// NewTask ~ PythonOperator. Return an error (or panic) to fail the task.
func (d *DAG) NewTask(id string, fn TaskFunc, opts ...TaskOption) *Task {
	return d.addTask(id, fn, nil, nil, opts)
}

// NewBranchTask ~ BranchPythonOperator. Returns the IDs of the direct downstream
// tasks to follow; every other direct downstream task is marked skipped.
func (d *DAG) NewBranchTask(id string, fn BranchFunc, opts ...TaskOption) *Task {
	return d.addTask(id, nil, fn, nil, opts)
}

// ---- dependencies ----------------------------------------------------------
//
//	Airflow                         dagsflow
//	a >> b >> c                     dag.Chain(a, b, c)        or a.Then(b).Then(c)
//	a >> [b, c] >> d                dag.Chain(a, dag.Parallel(b, c), d)
//	[a, b] >> c                     c.After(a, b)
//	a >> [b, c]                     a.ThenAll(b, c)

// Node is anything that can take part in a dependency: a *Task, a Parallel
// group, or a typed output (Out[T]) of the TaskFlow-style API.
type Node interface{ nodeTasks() []*Task }

func (t *Task) nodeTasks() []*Task { return []*Task{t} }

type parallel []*Task

func (p parallel) nodeTasks() []*Task { return p }

// Parallel groups nodes that run side by side (Airflow's [a, b] list).
func Parallel(nodes ...Node) Node {
	var out parallel
	for _, n := range nodes {
		if n != nil {
			out = append(out, n.nodeTasks()...)
		}
	}
	return out
}

// Chain links nodes in order: every task of a node runs before every task of
// the next one. Chain(a, Parallel(b, c), d) == a >> [b, c] >> d.
// It returns the last node so chains can be continued.
func Chain(nodes ...Node) Node {
	for i := 1; i < len(nodes); i++ {
		link(nodes[i-1], nodes[i])
	}
	if len(nodes) == 0 {
		return parallel(nil)
	}
	return nodes[len(nodes)-1]
}

// Then sets t >> next and returns next, so calls can be chained: a.Then(b).Then(c).
func (t *Task) Then(next *Task) *Task {
	link(t, next)
	return next
}

// ThenAll sets t >> [nexts...].
func (t *Task) ThenAll(nexts ...Node) *Task {
	link(t, Parallel(nexts...))
	return t
}

// After declares upstream tasks: c.After(a, b) == [a, b] >> c. Returns t.
func (t *Task) After(ups ...Node) *Task {
	link(Parallel(ups...), t)
	return t
}

func link(from, to Node) {
	if from == nil || to == nil {
		return
	}
	for _, a := range from.nodeTasks() {
		for _, b := range to.nodeTasks() {
			a.link(b)
		}
	}
}

func (t *Task) link(next *Task) {
	switch {
	case t == nil || next == nil:
		for _, x := range []*Task{t, next} {
			if x != nil {
				x.dag.buildErr(fmt.Errorf("dependency with a nil task (next to %q)", x.ID))
			}
		}
		return
	case t == next:
		t.dag.buildErr(fmt.Errorf("task %q cannot depend on itself", t.ID))
		return
	case t.dag != next.dag:
		t.dag.buildErr(fmt.Errorf("cannot link %s.%s to %s.%s: tasks belong to different DAGs (use an ExternalTaskSensor or datasets)", t.dag.ID, t.ID, next.dag.ID, next.ID))
		return
	}
	for _, u := range next.upstream {
		if u == t {
			return
		}
	}
	next.upstream = append(next.upstream, t)
	t.downstream = append(t.downstream, next)
}

func (t *Task) DAG() *DAG                      { return t.dag }
func (t *Task) Upstream() []*Task              { return t.upstream }
func (t *Task) Downstream() []*Task            { return t.downstream }
func (t *Task) TriggerRule() TriggerRule       { return t.triggerRule }
func (t *Task) Retries() int                   { return t.retries }
func (t *Task) RetryDelay() time.Duration      { return t.retryDelay }
func (t *Task) Timeout() time.Duration         { return t.timeout }
func (t *Task) IsBranch() bool                 { return t.branch != nil }
func (t *Task) Pool() string                   { return t.pool }
func (t *Task) PoolSlots() int                 { return t.poolSlots }
func (t *Task) Queue() string                  { return t.queue }
func (t *Task) PodOverride() map[string]any    { return t.podOverride }
func (t *Task) PriorityWeight() int            { return t.priority }
func (t *Task) SLA() time.Duration             { return t.sla }
func (t *Task) Outlets() []string              { return t.outlets }
func (t *Task) Sensor() *SensorSpec            { return t.sensor }
func (t *Task) MapSource() *MapSource          { return t.mapSource }
func (t *Task) OnSuccessCallbacks() []Callback { return t.onSuccess }
func (t *Task) OnFailureCallbacks() []Callback { return t.onFailure }
func (t *Task) OnRetryCallbacks() []Callback   { return t.onRetry }

// Run executes the task body. For branch tasks it returns the selected task IDs.
func (t *Task) Run(ctx *TaskContext) ([]string, error) {
	if r := t.isolated(); r != nil {
		sel, _, err := r(ctx, t, "task")
		if err != nil || t.branch == nil {
			return nil, err
		}
		return t.checkBranch(sel)
	}
	if t.branch != nil {
		sel, err := t.branch(ctx)
		if err != nil {
			return nil, err
		}
		return t.checkBranch(sel)
	}
	if t.execute == nil {
		return nil, nil
	}
	return nil, t.execute(ctx)
}

// Poke runs one poke of a sensor task (in a child process when the task's
// Go code is isolated, see SetIsolatedRunner).
func (t *Task) Poke(ctx *TaskContext) (bool, error) {
	if t.sensor == nil {
		return false, fmt.Errorf("task %s is not a sensor", t.ID)
	}
	if r := t.isolated(); r != nil {
		_, ok, err := r(ctx, t, "poke")
		return ok, err
	}
	return t.sensor.Poke(ctx)
}

func (t *Task) checkBranch(sel []string) ([]string, error) {
	for _, id := range sel {
		ok := false
		for _, dn := range t.downstream {
			if dn.ID == id {
				ok = true
			}
		}
		if !ok {
			return nil, fmt.Errorf("branch returned %q which is not a direct downstream of %s", id, t.ID)
		}
	}
	return sel, nil
}

// IsolatedRunner runs one action ("task" or "poke") of a task in a separate
// process and reports the branch selection / poke result.
type IsolatedRunner func(ctx *TaskContext, t *Task, action string) (selected []string, pokeOK bool, err error)

var isolatedRunner atomic.Pointer[IsolatedRunner]

// SetIsolatedRunner makes the tasks of compiled-in Go DAGs (added with
// Register, so a child process of this binary has them too) run through r, normally in a child process of this binary, so a timeout or a
// cancel can kill them even when their code ignores ctx. nil turns it off.
func SetIsolatedRunner(r IsolatedRunner) {
	if r == nil {
		isolatedRunner.Store(nil)
		return
	}
	isolatedRunner.Store(&r)
}

func (t *Task) isolated() IsolatedRunner {
	p := isolatedRunner.Load()
	if p == nil || t.dag == nil || !t.dag.compiledIn {
		return nil
	}
	if t.execute == nil && t.branch == nil && t.sensor == nil {
		return nil
	}
	return *p
}

// ---- DAG accessors -----------------------------------------------------------

func (d *DAG) Tasks() []*Task { return d.topo }

// Owner ~ default_args["owner"].
func (d *DAG) Owner() string {
	if d.DefaultArgs.Owner != "" {
		return d.DefaultArgs.Owner
	}
	return "dagsflow"
}
func (d *DAG) Task(id string) (*Task, bool) {
	t, ok := d.taskMap[id]
	return t, ok
}
func (d *DAG) CronSchedule() cron.Schedule { return d.schedule }
func (d *DAG) IsManualOnly() bool          { return d.schedule == nil && len(d.Datasets) == 0 }
func (d *DAG) IsDatasetScheduled() bool    { return d.schedule == nil && len(d.Datasets) > 0 }

// validate parses the schedule, wires mapping dependencies and checks the graph
// is acyclic (topological sort).
// Validate checks the DAG (ids, links, cycles) and orders its tasks, as
// Register does. Needed before Tasks() for a DAG that isn't registered.
func (d *DAG) Validate() error { return d.validate() }

func (d *DAG) validate() error {
	if len(d.errs) > 0 {
		return errors.Join(d.errs...)
	}
	s := strings.TrimSpace(d.Schedule)
	if s != "" && s != "@once" && s != "None" {
		sch, err := cron.ParseStandard(s)
		if err != nil {
			return fmt.Errorf("invalid schedule %q: %w", s, err)
		}
		d.schedule = sch
	}
	if d.schedule != nil && len(d.Datasets) > 0 {
		return fmt.Errorf("a DAG can use either a time schedule or a dataset schedule, not both")
	}
	for _, t := range d.tasks {
		if t.mapSource != nil && t.mapSource.IsXCom() {
			src, ok := d.taskMap[t.mapSource.XComTask]
			if !ok {
				return fmt.Errorf("task %s expands over unknown task %q", t.ID, t.mapSource.XComTask)
			}
			src.link(t)
		}
		if t.mapSource != nil && (t.branch != nil || t.sensor != nil) {
			return fmt.Errorf("task %s: branch and sensor tasks cannot be mapped", t.ID)
		}
	}
	indeg := map[*Task]int{}
	for _, t := range d.tasks {
		indeg[t] = len(t.upstream)
	}
	var queue, order []*Task
	for _, t := range d.tasks {
		if indeg[t] == 0 {
			queue = append(queue, t)
		}
	}
	for len(queue) > 0 {
		t := queue[0]
		queue = queue[1:]
		order = append(order, t)
		for _, dn := range t.downstream {
			indeg[dn]--
			if indeg[dn] == 0 {
				queue = append(queue, dn)
			}
		}
	}
	if len(order) != len(d.tasks) {
		return fmt.Errorf("cycle detected in dag %s", d.ID)
	}
	d.topo = order
	return nil
}

// ---- Registry (the "DagBag") -------------------------------------------------

var (
	registry     = map[string]*DAG{}
	importErrors = map[string]error{} // key: dag id (Go) or file path (YAML)
	regMu        sync.RWMutex
)

// Register validates and adds a Go-defined DAG. Invalid DAGs are recorded as
// import errors instead of crashing the whole binary.
func Register(d *DAG) {
	regMu.Lock()
	defer regMu.Unlock()
	if err := d.validate(); err != nil {
		importErrors[d.ID] = err
		return
	}
	d.ParsedAt = time.Now()
	d.compiledIn = d.Source == "go"
	registry[d.ID] = d
}

// ReplaceSource atomically swaps every DAG loaded from `source` (a file) with
// the new set. Used by the DAG-folder loader for hot reload.
func ReplaceSource(source string, dags []*DAG, loadErr error) {
	regMu.Lock()
	defer regMu.Unlock()
	for id, d := range registry {
		if d.Source == source {
			delete(registry, id)
		}
	}
	delete(importErrors, source)
	if loadErr != nil {
		importErrors[source] = loadErr
		return
	}
	for _, d := range dags {
		if err := d.validate(); err != nil {
			importErrors[source] = fmt.Errorf("%s: %w", d.ID, err)
			continue
		}
		if old, dup := registry[d.ID]; dup {
			importErrors[source] = fmt.Errorf("dag_id %q already defined in %s", d.ID, old.Source)
			continue
		}
		d.Source = source
		d.ParsedAt = time.Now()
		registry[d.ID] = d
	}
}

// Sources lists the file sources currently registered (for detecting deleted files).
func Sources() map[string]bool {
	regMu.RLock()
	defer regMu.RUnlock()
	out := map[string]bool{}
	for _, d := range registry {
		if d.Source != "go" {
			out[d.Source] = true
		}
	}
	for k := range importErrors {
		if strings.ContainsAny(k, "/\\") {
			out[k] = true
		}
	}
	return out
}

func Get(id string) (*DAG, bool) {
	regMu.RLock()
	defer regMu.RUnlock()
	d, ok := registry[id]
	return d, ok
}

func List() []*DAG {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]*DAG, 0, len(registry))
	for _, d := range registry {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func ImportErrors() map[string]error {
	regMu.RLock()
	defer regMu.RUnlock()
	out := map[string]error{}
	for k, v := range importErrors {
		out[k] = v
	}
	return out
}
