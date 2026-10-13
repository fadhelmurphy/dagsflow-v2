package dag

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// The visual builder edits a workflow as a graph (nodes + edges + positions)
// and stores it as an ordinary YAML DAG, so the scheduler, executors, RBAC and
// every other view treat it like any hand-written YAML DAG.

// ---- node catalog ----------------------------------------------------------------

// NodeField describes one input of a node in the builder's side panel. Name may
// be a dotted path into the task ("params.project_id").
type NodeField struct {
	Name        string   `json:"name"`
	Label       string   `json:"label"`
	Kind        string   `json:"kind"` // text, code, number, bool, select, list, intlist, kv, json, steps, conn, upstream
	Required    bool     `json:"required,omitempty"`
	Help        string   `json:"help,omitempty"`
	Placeholder string   `json:"placeholder,omitempty"`
	Options     []string `json:"options,omitempty"`
	Lang        string   `json:"lang,omitempty"`
}

// NodeType is one entry of the palette.
type NodeType struct {
	Type        string      `json:"type"`
	Operator    string      `json:"operator"`
	Label       string      `json:"label"`
	Category    string      `json:"category"`
	Description string      `json:"description"`
	Icon        string      `json:"icon"`
	Fields      []NodeField `json:"fields"`
	// NeedsInput: the node reads the rows of an upstream node.
	NeedsInput bool `json:"needs_input,omitempty"`
}

var sensorFields = []NodeField{
	{Name: "poke_interval", Label: "Check every", Kind: "text", Placeholder: "30s", Help: "Go duration, e.g. 30s, 5m"},
	{Name: "sensor_timeout", Label: "Give up after", Kind: "text", Placeholder: "1h"},
	{Name: "mode", Label: "Mode", Kind: "select", Options: []string{"", "poke", "reschedule"}, Help: "reschedule frees the worker slot between checks"},
	{Name: "soft_fail", Label: "Skip instead of fail on timeout", Kind: "bool"},
}

var httpFields = []NodeField{
	{Name: "method", Label: "Method", Kind: "select", Options: []string{"GET", "POST", "PUT", "PATCH", "DELETE"}},
	{Name: "url", Label: "URL", Kind: "text", Placeholder: "https://api.example.com/orders?date={{ .ds }}", Help: "Templates work: {{ .ds }}, {{ var \"key\" }}, {{ xcom_pull \"node_id\" \"return_value\" }}"},
	{Name: "conn_id", Label: "Connection", Kind: "conn", Help: "Optional. Its host is prepended to Endpoint and its token is sent as a Bearer header."},
	{Name: "endpoint", Label: "Endpoint", Kind: "text", Help: "Used with a connection instead of URL"},
	{Name: "headers", Label: "Headers", Kind: "kv"},
	{Name: "body", Label: "Body", Kind: "code", Lang: "json"},
	{Name: "expect_status", Label: "Accepted status codes", Kind: "intlist", Help: "Default: any 2xx"},
}

// Nodes is the palette. Order = display order.
var Nodes = []NodeType{
	{Type: "http", Operator: "http", Label: "HTTP request", Category: "Sources", Icon: "globe",
		Description: "Call an API. The response body (up to 64KB) is the node's output.", Fields: httpFields},
	{Type: "postgres_query", Operator: "postgres", Label: "Postgres query", Category: "Sources", Icon: "db",
		Description: "Run SQL on Postgres. Returned rows are the node's output (JSON array).",
		Fields: []NodeField{
			{Name: "conn_id", Label: "Connection", Kind: "conn", Required: true},
			{Name: "sql", Label: "SQL", Kind: "code", Lang: "sql", Required: true, Help: "Templates work, e.g. WHERE d = '{{ .ds }}'"},
			{Name: "limit", Label: "Max rows", Kind: "number", Placeholder: "10000"},
		}},
	{Type: "bigquery_query", Operator: "bigquery_query", Label: "BigQuery read rows", Category: "Sources", Icon: "bq",
		Description: "Run a SELECT on BigQuery. The result rows are the node's output (JSON array).",
		Fields: []NodeField{
			{Name: "sql", Label: "SQL", Kind: "code", Lang: "sql", Required: true, Help: "Templates work, e.g. WHERE d = '{{ .ds }}'"},
			{Name: "project_id", Label: "Project", Kind: "text", Help: "Default: from the connection / credentials"},
			{Name: "location", Label: "Location", Kind: "text", Placeholder: "US"},
			{Name: "limit", Label: "Max rows", Kind: "number", Placeholder: "10000"},
			{Name: "conn_id", Label: "Connection", Kind: "conn", Help: "Default: google_cloud_default"},
		}},
	{Type: "gsheet_read", Operator: "gsheet_read", Label: "Google Sheets read", Category: "Sources", Icon: "table",
		Description: "Read a sheet range. With a header row, each row becomes an object.",
		Fields: []NodeField{
			{Name: "spreadsheet_id", Label: "Spreadsheet", Kind: "text", Required: true, Help: "ID or full URL. Share the sheet with the service account."},
			{Name: "range", Label: "Range", Kind: "text", Required: true, Placeholder: "Sheet1!A1:F"},
			{Name: "no_header", Label: "First row is data (no header)", Kind: "bool"},
			{Name: "limit", Label: "Max rows", Kind: "number", Placeholder: "10000"},
			{Name: "conn_id", Label: "Connection", Kind: "conn", Help: "Default: google_cloud_default"},
		}},
	{Type: "transform", Operator: "transform", Label: "Transform rows", Category: "Transform", Icon: "shuffle", NeedsInput: true,
		Description: "No-code reshaping of the rows from the node before it: pick fields, rename, filter, add columns, sort…",
		Fields: []NodeField{
			{Name: "input", Label: "Read rows from", Kind: "upstream", Help: "Default: the only connected node before this one"},
			{Name: "steps", Label: "Steps (run top to bottom)", Kind: "steps"},
		}},
	{Type: "bigquery", Operator: "bigquery", Label: "BigQuery SQL (run)", Category: "Transform", Icon: "bolt",
		Description: "Run a BigQuery job (query, MERGE, CREATE TABLE AS…).",
		Fields: []NodeField{
			{Name: "sql", Label: "SQL", Kind: "code", Lang: "sql", Required: true},
			{Name: "params.project_id", Label: "Project", Kind: "text"},
			{Name: "params.location", Label: "Location", Kind: "text", Placeholder: "US"},
			{Name: "params.gcp_conn_id", Label: "Connection", Kind: "conn", Help: "Default: google_cloud_default"},
		}},
	{Type: "bash", Operator: "bash", Label: "Shell command", Category: "Transform", Icon: "terminal",
		Description: "Run a shell command. The last stdout line is the node's output.",
		Fields: []NodeField{
			{Name: "command", Label: "Command", Kind: "code", Lang: "bash", Required: true},
			{Name: "env", Label: "Environment", Kind: "kv"},
		}},
	{Type: "postgres_insert", Operator: "postgres", Label: "Postgres insert", Category: "Destinations", Icon: "dbin", NeedsInput: true,
		Description: "Insert the rows from the node before it into a table (one transaction).",
		Fields: []NodeField{
			{Name: "conn_id", Label: "Connection", Kind: "conn", Required: true},
			{Name: "table", Label: "Table", Kind: "text", Required: true, Placeholder: "public.orders", Help: "Columns are matched by field name"},
			{Name: "rows_from", Label: "Read rows from", Kind: "upstream", Help: "Default: the only connected node before this one"},
			{Name: "on_conflict", Label: "On conflict", Kind: "select", Options: []string{"", "do_nothing"}, Help: "do_nothing skips rows that already exist"},
		}},
	{Type: "bigquery_insert", Operator: "bigquery_insert", Label: "BigQuery insert rows", Category: "Destinations", Icon: "bq", NeedsInput: true,
		Description: "Stream the rows from the node before it into an existing table.",
		Fields: []NodeField{
			{Name: "dataset", Label: "Dataset", Kind: "text", Required: true},
			{Name: "table", Label: "Table", Kind: "text", Required: true, Help: "Must exist; columns are matched by field name"},
			{Name: "project_id", Label: "Project", Kind: "text"},
			{Name: "rows_from", Label: "Read rows from", Kind: "upstream", Help: "Default: the only connected node before this one"},
			{Name: "conn_id", Label: "Connection", Kind: "conn", Help: "Default: google_cloud_default"},
		}},
	{Type: "gsheet_write", Operator: "gsheet_write", Label: "Google Sheets write", Category: "Destinations", Icon: "table", NeedsInput: true,
		Description: "Write the rows from the node before it into a sheet.",
		Fields: []NodeField{
			{Name: "spreadsheet_id", Label: "Spreadsheet", Kind: "text", Required: true, Help: "ID or full URL. Share the sheet with the service account (editor)."},
			{Name: "range", Label: "Range / tab", Kind: "text", Required: true, Placeholder: "Sheet1"},
			{Name: "write_mode", Label: "Mode", Kind: "select", Options: []string{"", "append", "overwrite"}, Help: "append (default) adds below existing data; overwrite clears the range first"},
			{Name: "columns", Label: "Columns", Kind: "list", Help: "Optional order; default: all fields"},
			{Name: "user_entered", Label: "Parse values like typing (dates, formulas)", Kind: "bool"},
			{Name: "rows_from", Label: "Read rows from", Kind: "upstream"},
			{Name: "conn_id", Label: "Connection", Kind: "conn", Help: "Default: google_cloud_default"},
		}},
	{Type: "trigger_dagrun", Operator: "trigger_dagrun", Label: "Trigger another DAG", Category: "Control", Icon: "play",
		Description: "Start a run of another DAG, optionally waiting for it.",
		Fields: []NodeField{
			{Name: "trigger_dag_id", Label: "DAG id", Kind: "text", Required: true},
			{Name: "conf", Label: "Conf", Kind: "json"},
			{Name: "wait_for_completion", Label: "Wait until it finishes", Kind: "bool"},
		}},
	{Type: "empty", Operator: "empty", Label: "No-op / join", Category: "Control", Icon: "dot",
		Description: "Does nothing. Handy as a start, end or join point.", Fields: []NodeField{}},
	{Type: "file_sensor", Operator: "file_sensor", Label: "Wait for file", Category: "Sensors", Icon: "clock",
		Description: "Wait until a file exists.",
		Fields:      append([]NodeField{{Name: "path", Label: "Path", Kind: "text", Required: true}}, sensorFields...)},
	{Type: "time_delta_sensor", Operator: "time_delta_sensor", Label: "Wait for a delay", Category: "Sensors", Icon: "clock",
		Description: "Wait until the data interval end plus a delay.",
		Fields:      append([]NodeField{{Name: "delta", Label: "Delay", Kind: "text", Required: true, Placeholder: "10m"}}, sensorFields...)},
	{Type: "external_task_sensor", Operator: "external_task_sensor", Label: "Wait for another DAG", Category: "Sensors", Icon: "clock",
		Description: "Wait until a task of another DAG succeeded for the same logical date.",
		Fields: append([]NodeField{{Name: "external_dag_id", Label: "DAG id", Kind: "text", Required: true},
			{Name: "external_task_id", Label: "Task id", Kind: "text", Help: "Empty = the whole DAG run"}}, sensorFields...)},
	{Type: "http_sensor", Operator: "http_sensor", Label: "Wait for HTTP", Category: "Sensors", Icon: "clock",
		Description: "Poll a URL until it answers with an accepted status.",
		Fields:      append(append([]NodeField{}, httpFields...), sensorFields...)},
}

// CommonFields are the per-node settings every node has (collapsed "Settings").
var CommonFields = []NodeField{
	{Name: "retries", Label: "Retries", Kind: "number"},
	{Name: "retry_delay", Label: "Retry delay", Kind: "text", Placeholder: "5m"},
	{Name: "timeout", Label: "Timeout", Kind: "text", Placeholder: "30m"},
	{Name: "trigger_rule", Label: "Run when", Kind: "select", Options: []string{"", "all_success", "all_failed", "all_done", "one_success", "one_failed", "none_failed", "none_failed_min_one_success", "none_skipped", "always"}, Help: "Default all_success"},
	{Name: "pool", Label: "Pool", Kind: "text"},
	{Name: "priority_weight", Label: "Priority", Kind: "number"},
	{Name: "outlets", Label: "Updates datasets", Kind: "list", Help: "Dataset URIs this node produces"},
	{Name: "on_failure", Label: "Alert on failure", Kind: "list", Help: "Connection ids of type google_chat"},
	{Name: "expand", Label: "Run once per item", Kind: "expand"},
}

func nodeByType(t string) (NodeType, bool) {
	for _, n := range Nodes {
		if n.Type == t {
			return n, true
		}
	}
	return NodeType{}, false
}

// ---- graph model --------------------------------------------------------------------

type GNode struct {
	ID     string         `json:"id"`
	Type   string         `json:"type"`
	X      float64        `json:"x"`
	Y      float64        `json:"y"`
	Config map[string]any `json:"config"`
}

type GEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Graph is the builder's view of a YAML DAG. Settings holds the DAG-level keys
// (schedule, tags, params, default_args, ...) untouched.
type Graph struct {
	DagID    string         `json:"dag_id"`
	Settings map[string]any `json:"settings"`
	Nodes    []GNode        `json:"nodes"`
	Edges    []GEdge        `json:"edges"`
	// NeedsLayout: no positions were stored, the UI should lay the graph out.
	NeedsLayout bool `json:"needs_layout,omitempty"`
}

// GraphIssue is a validation problem, attached to a node when Node is set.
type GraphIssue struct {
	Node    string `json:"node,omitempty"`
	Message string `json:"message"`
	Warning bool   `json:"warning,omitempty"`
}

var (
	dagIDRe  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$`)
	nodeIDRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,99}$`)
)

// ValidDagID reports whether id can be used as DAG id and file name.
func ValidDagID(id string) bool { return dagIDRe.MatchString(id) }

var dagKeys = []string{"dag_id", "description", "schedule", "schedule_datasets", "start_date", "catchup",
	"max_active_runs", "tags", "params", "default_args", "on_success", "on_failure", "on_sla_miss"}

// clean drops empty values so the YAML only holds what was set.
func clean(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			if c := clean(e); c != nil {
				out[k] = c
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	case []any:
		out := make([]any, 0, len(x))
		for _, e := range x {
			if c := clean(e); c != nil {
				out = append(out, c)
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	case string:
		if strings.TrimSpace(x) == "" {
			return nil
		}
		return x
	case bool:
		if !x {
			return nil
		}
		return x
	case nil:
		return nil
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1e15 {
			return int64(x)
		}
		return x
	}
	return v
}

// ordered is a map that marshals its keys in a fixed order.
type ordered struct {
	keys []string
	vals map[string]any
}

func (o *ordered) set(k string, v any) {
	if v == nil {
		return
	}
	if o.vals == nil {
		o.vals = map[string]any{}
	}
	if _, ok := o.vals[k]; !ok {
		o.keys = append(o.keys, k)
	}
	o.vals[k] = v
}

func (o ordered) MarshalYAML() (any, error) {
	n := &yaml.Node{Kind: yaml.MappingNode}
	for _, k := range o.keys {
		kn := &yaml.Node{Kind: yaml.ScalarNode, Value: k}
		vn := &yaml.Node{}
		if err := vn.Encode(o.vals[k]); err != nil {
			return nil, err
		}
		if vn.Kind == yaml.ScalarNode && strings.Contains(vn.Value, "\n") {
			vn.Style = yaml.LiteralStyle
		}
		n.Content = append(n.Content, kn, vn)
	}
	return n, nil
}

// flowInts marshals as [1, 2] on one line.
type flowInts []int

func (f flowInts) MarshalYAML() (any, error) {
	n := &yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
	for _, v := range f {
		n.Content = append(n.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: fmt.Sprint(v)})
	}
	return n, nil
}

// stepKeyOrder: how the keys of a transform step are written.
var stepKeyOrder = []string{"op", "path", "fields", "map", "field", "cmp", "value", "n", "desc"}

func orderSteps(v any) any {
	list, ok := v.([]any)
	if !ok {
		return v
	}
	out := make([]any, len(list))
	for i, it := range list {
		m, ok := it.(map[string]any)
		if !ok {
			out[i] = it
			continue
		}
		o := &ordered{}
		for _, k := range stepKeyOrder {
			if x, ok := m[k]; ok {
				o.set(k, x)
			}
		}
		for _, k := range sortedKeys(m) {
			if _, ok := o.vals[k]; !ok {
				o.set(k, m[k])
			}
		}
		out[i] = o
	}
	return out
}

func sortedKeys(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// taskMap builds the YAML task of a node (without upstream).
func (g *Graph) taskMap(n GNode, nt NodeType) *ordered {
	t := &ordered{}
	t.set("id", n.ID)
	t.set("operator", nt.Operator)
	cfg, _ := clean(n.Config).(map[string]any)
	for _, k := range sortedKeys(cfg) {
		if k == "id" || k == "operator" || k == "upstream" {
			continue
		}
		if k == "steps" {
			t.set(k, orderSteps(cfg[k]))
			continue
		}
		t.set(k, cfg[k])
	}
	return t
}

// ToYAML renders the graph as a YAML DAG document.
func (g *Graph) ToYAML() ([]byte, error) {
	doc := &ordered{}
	doc.set("dag_id", g.DagID)
	settings, _ := clean(g.Settings).(map[string]any)
	for _, k := range dagKeys[1:] {
		if v, ok := settings[k]; ok {
			doc.set(k, v)
		}
	}
	for _, k := range sortedKeys(settings) { // keep unknown keys so the YAML loader reports them
		if _, ok := doc.vals[k]; !ok && k != "tasks" && k != "layout" && k != "dag_id" {
			doc.set(k, settings[k])
		}
	}
	ups := map[string][]string{}
	for _, e := range g.Edges {
		ups[e.To] = append(ups[e.To], e.From)
	}
	var tasks []any
	layout := &ordered{}
	for _, n := range g.Nodes {
		nt, ok := nodeByType(n.Type)
		if !ok {
			return nil, fmt.Errorf("node %s: unknown type %q", n.ID, n.Type)
		}
		t := g.taskMap(n, nt)
		if u := ups[n.ID]; len(u) > 0 {
			ul := make([]any, len(u))
			for i, x := range u {
				ul[i] = x
			}
			t.set("upstream", ul)
		}
		tasks = append(tasks, t)
		layout.set(n.ID, flowInts{int(math.Round(n.X)), int(math.Round(n.Y))})
	}
	doc.set("tasks", tasks)
	doc.set("layout", layout)
	var buf bytes.Buffer
	buf.WriteString("# Created with the dagsflow visual builder. Edit it there, or by hand: both work.\n")
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	_ = enc.Close()
	return buf.Bytes(), nil
}

// Etag identifies a version of a file.
func Etag(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:8])
}

// ---- validation ------------------------------------------------------------------------

func hasCycle(g *Graph) []string {
	indeg := map[string]int{}
	next := map[string][]string{}
	for _, n := range g.Nodes {
		indeg[n.ID] = 0
	}
	for _, e := range g.Edges {
		indeg[e.To]++
		next[e.From] = append(next[e.From], e.To)
	}
	var q []string
	for id, d := range indeg {
		if d == 0 {
			q = append(q, id)
		}
	}
	done := 0
	for len(q) > 0 {
		id := q[0]
		q = q[1:]
		done++
		for _, to := range next[id] {
			if indeg[to]--; indeg[to] == 0 {
				q = append(q, to)
			}
		}
	}
	if done == len(g.Nodes) {
		return nil
	}
	var bad []string
	for id, d := range indeg {
		if d > 0 {
			bad = append(bad, id)
		}
	}
	sort.Strings(bad)
	return bad
}

// Validate checks the graph the way the loader would and returns the YAML it
// would save. Issues with Warning=false block saving.
func (g *Graph) Validate() ([]byte, []GraphIssue) {
	var iss []GraphIssue
	add := func(node, msg string) { iss = append(iss, GraphIssue{Node: node, Message: msg}) }
	if !ValidDagID(g.DagID) {
		add("", "DAG id must start with a letter or digit and use only letters, digits, _ . - (max 100)")
	}
	if len(g.Nodes) == 0 {
		add("", "Add at least one node")
	}
	ids := map[string]bool{}
	for _, n := range g.Nodes {
		switch {
		case !nodeIDRe.MatchString(n.ID):
			add(n.ID, fmt.Sprintf("node id %q must use letters, digits, _ . - only", n.ID))
		case ids[n.ID]:
			add(n.ID, fmt.Sprintf("duplicate node id %q", n.ID))
		}
		ids[n.ID] = true
		if _, ok := nodeByType(n.Type); !ok {
			add(n.ID, fmt.Sprintf("unknown node type %q", n.Type))
		}
	}
	seen := map[GEdge]bool{}
	edges := g.Edges[:0:0]
	for _, e := range g.Edges {
		switch {
		case !ids[e.From] || !ids[e.To]:
			add("", fmt.Sprintf("connection %s → %s points to a missing node", e.From, e.To))
		case e.From == e.To:
			add(e.From, "a node cannot be connected to itself")
		case !seen[e]:
			seen[e] = true
			edges = append(edges, e)
		}
	}
	g.Edges = edges
	if len(iss) > 0 {
		return nil, iss
	}
	if cyc := hasCycle(g); cyc != nil {
		for _, id := range cyc {
			add(id, "this node is part of a loop: a node cannot depend on itself through other nodes")
		}
		return nil, iss
	}
	out, err := g.ToYAML()
	if err != nil {
		add("", err.Error())
		return nil, iss
	}

	// per node: strict decode + operator checks
	var doc struct {
		Tasks []yaml.Node `yaml:"tasks"`
	}
	_ = yaml.Unmarshal(out, &doc)
	incoming := map[string]int{}
	for _, e := range g.Edges {
		incoming[e.To]++
	}
	for i, n := range g.Nodes {
		nt, _ := nodeByType(n.Type)
		var raw bytes.Buffer
		enc := yaml.NewEncoder(&raw)
		_ = enc.Encode(&doc.Tasks[i])
		_ = enc.Close()
		dec := yaml.NewDecoder(&raw)
		dec.KnownFields(true)
		var yt yamlTask
		if err := dec.Decode(&yt); err != nil {
			add(n.ID, cleanYAMLErr(err))
			continue
		}
		for _, f := range nt.Fields {
			if f.Required && !hasValue(n.Config, f.Name) && !(n.Type == "bigquery" && hasValue(n.Config, "sql_file")) {
				add(n.ID, f.Label+" is required")
			}
		}
		if nt.Type == "http" || nt.Type == "http_sensor" {
			if !hasValue(n.Config, "url") && !(hasValue(n.Config, "conn_id") && hasValue(n.Config, "endpoint")) {
				add(n.ID, "URL is required (or a connection plus an endpoint)")
			}
		}
		if nt.NeedsInput && incoming[n.ID] == 0 && !hasValue(n.Config, "input") && !hasValue(n.Config, "rows_from") {
			add(n.ID, "connect this node to the node that provides the rows")
		}
		if nt.NeedsInput && incoming[n.ID] > 1 && !hasValue(n.Config, "input") && !hasValue(n.Config, "rows_from") {
			add(n.ID, "several nodes are connected here: choose which one provides the rows")
		}
		if err := yt.checkBuild(); err != nil {
			add(n.ID, err.Error())
		}
		for _, cid := range []string{str(n.Config["conn_id"])} {
			if cid != "" && !strings.Contains(cid, "{{") {
				if _, ok := GetConnection(cid); !ok {
					iss = append(iss, GraphIssue{Node: n.ID, Warning: true,
						Message: fmt.Sprintf("connection %q does not exist yet (Admin → Connections)", cid)})
				}
			}
		}
	}
	if hardErrors(iss) {
		return nil, iss
	}

	// whole DAG: parse like the loader, then validate the schedule and the graph
	dags, err := ParseYAML("builder.yaml", out)
	if err != nil {
		add("", cleanYAMLErr(err))
		return nil, iss
	}
	for _, d := range dags {
		if err := d.validate(); err != nil {
			add("", err.Error())
		}
	}
	if hardErrors(iss) {
		return nil, iss
	}
	return out, iss
}

func hardErrors(iss []GraphIssue) bool {
	for _, i := range iss {
		if !i.Warning {
			return true
		}
	}
	return false
}

func hasValue(cfg map[string]any, path string) bool {
	var cur any = cfg
	for _, p := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return false
		}
		cur = m[p]
	}
	return clean(cur) != nil
}

func cleanYAMLErr(err error) string {
	s := strings.TrimPrefix(err.Error(), "yaml: unmarshal errors:\n")
	s = strings.ReplaceAll(s, "  line ", "line ")
	if i := strings.Index(s, "field "); i >= 0 && strings.Contains(s, "not found in type") {
		f := strings.Fields(s[i+6:])[0]
		return fmt.Sprintf("unknown setting %q", f)
	}
	return strings.TrimSpace(s)
}

// checkBuild builds the single task in a scratch DAG to surface option errors
// (bad durations, missing fields) for this node alone.
func (yt yamlTask) checkBuild() error {
	d := NewDAG("_check", "")
	opts, err := yt.options()
	if err != nil {
		return err
	}
	if yt.TriggerRule != "" {
		opts = append(opts, WithTriggerRule(TriggerRule(yt.TriggerRule)))
	}
	if err := yt.add(d, "", opts); err != nil {
		return err
	}
	return errors.Join(d.errs...)
}

// ---- YAML -> graph ---------------------------------------------------------------------

// ParseGraph reads a single-document YAML DAG into a graph. It fails for files
// the builder cannot represent (several documents, Go-only features).
func ParseGraph(data []byte) (*Graph, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	var extra map[string]any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("this file holds several DAGs: the builder edits one DAG per file")
	}
	g := &Graph{Settings: map[string]any{}, Nodes: []GNode{}, Edges: []GEdge{}}
	g.DagID, _ = raw["dag_id"].(string)
	layout, _ := raw["layout"].(map[string]any)
	for k, v := range raw {
		switch k {
		case "dag_id", "tasks", "layout":
		default:
			g.Settings[k] = v
		}
	}
	tasks, _ := raw["tasks"].([]any)
	for i, tv := range tasks {
		t, ok := tv.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("task %d is not a mapping", i+1)
		}
		id, _ := t["id"].(string)
		op, _ := t["operator"].(string)
		if op == "" {
			op = "empty"
		}
		typ := ""
		for _, nt := range Nodes {
			if nt.Operator == op {
				typ = nt.Type
				if op == "postgres" && nt.Type == "postgres_query" && str(t["table"]) != "" {
					continue
				}
				break
			}
		}
		if typ == "" {
			return nil, fmt.Errorf("task %q uses operator %q, which the builder does not support", id, op)
		}
		cfg := map[string]any{}
		for k, v := range t {
			switch k {
			case "id", "operator", "upstream":
			default:
				cfg[k] = v
			}
		}
		n := GNode{ID: id, Type: typ, Config: cfg}
		switch p := layout[id].(type) {
		case []any: // [x, y]
			if len(p) == 2 {
				n.X, _ = toFloat(p[0])
				n.Y, _ = toFloat(p[1])
			}
		case map[string]any: // {x: , y: }
			n.X, _ = toFloat(p["x"])
			n.Y, _ = toFloat(p["y"])
		default:
			g.NeedsLayout = true
		}
		g.Nodes = append(g.Nodes, n)
		if ups, ok := t["upstream"].([]any); ok {
			for _, u := range ups {
				if us, ok := u.(string); ok {
					g.Edges = append(g.Edges, GEdge{From: us, To: id})
				}
			}
		}
	}
	// "run once per item of X" depends on X without an upstream entry: draw it
	for _, n := range g.Nodes {
		exp, _ := n.Config["expand"].(map[string]any)
		xc, _ := exp["xcom"].(map[string]any)
		from := str(xc["task"])
		if from == "" {
			continue
		}
		have := false
		for _, e := range g.Edges {
			have = have || (e.From == from && e.To == n.ID)
		}
		for _, o := range g.Nodes {
			if o.ID == from && !have {
				g.Edges = append(g.Edges, GEdge{From: from, To: n.ID})
			}
		}
	}
	return g, nil
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
