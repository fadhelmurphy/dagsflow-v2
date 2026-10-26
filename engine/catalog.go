package engine

import (
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"dagsflow-go/dag"
)

// Data catalog and lineage: the tables (and files, sheets) pipelines read and
// write, their columns and schema changes, and which columns feed which.
//
// Two sources fill it:
//   - dagsflow's own nodes, every time they run (lineage they know exactly),
//   - the *_catalog_sync nodes, which read a warehouse's metadata
//     (BigQuery INFORMATION_SCHEMA, Redshift SVV / SYS views): tables made
//     outside dagsflow, and lineage parsed from the queries people ran.
//
// Dataset ids: "<engine>:<name>", e.g. bigquery:proj.ds.orders,
// redshift:dev.public.orders, gsheet:<id>!Sheet1, gcs:gs://b/x/{date}.ndjson,
// task:<dag>.<task> (rows passed between two dagsflow nodes).

type CatalogColumn struct {
	Name      string     `json:"name"`
	Type      string     `json:"type,omitempty"`
	FirstSeen time.Time  `json:"first_seen"`
	LastSeen  time.Time  `json:"last_seen"`
	Removed   *time.Time `json:"removed,omitempty"`
}

type SchemaChange struct {
	At      time.Time         `json:"at"`
	Added   []string          `json:"added,omitempty"`
	Removed []string          `json:"removed,omitempty"`
	Retyped map[string]string `json:"retyped,omitempty"` // column -> "OLD -> NEW"
}

type CatalogTable struct {
	ID        string          `json:"id"`
	Engine    string          `json:"engine"`
	Name      string          `json:"name"`
	Kind      string          `json:"kind,omitempty"` // table, view, external, file, sheet, task
	Columns   []CatalogColumn `json:"columns"`
	CreatedAt *time.Time      `json:"created_at,omitempty"`
	Creator   string          `json:"creator,omitempty"`
	Writers   []string        `json:"writers,omitempty"` // dag.task or user that wrote it
	Source    string          `json:"source"`            // dagsflow | scan
	Scope     string          `json:"scope,omitempty"`   // the sync that found it (bigquery:proj:region)
	SeenAt    time.Time       `json:"seen_at"`
	Dropped   *time.Time      `json:"dropped,omitempty"`
	Changes   []SchemaChange  `json:"changes,omitempty"`
}

type LineageEdge struct {
	From     string    `json:"from"`
	FromCol  string    `json:"from_col,omitempty"`
	To       string    `json:"to"`
	ToCol    string    `json:"to_col,omitempty"`
	Expr     string    `json:"expr,omitempty"` // how the column is computed, when it is not a plain copy
	Via      string    `json:"via,omitempty"`  // dag.task, or job / query id and user
	Kind     string    `json:"kind"`           // dagsflow | query
	LastSeen time.Time `json:"last_seen"`
}

func (e LineageEdge) key() string {
	return e.From + "\x1f" + e.FromCol + "\x1f" + e.To + "\x1f" + e.ToCol
}

var (
	datePart    = regexp.MustCompile(`\d{4}/\d{2}/\d{2}|\d{4}-\d{2}-\d{2}(T\d{2}[:\-]?\d{2}[:\-]?\d{2}(\.\d+)?Z?)?`)
	compactDate = regexp.MustCompile(`(^|[^0-9])(?:19|20)\d{6}(?:\d{4}|\d{6})?([^0-9]|$)`)
)

// NormalizeDatasetID folds dated file names into one dataset:
// gcs:gs://b/x/2026-10-26/a.ndjson -> gcs:gs://b/x/{date}/a.ndjson.
func NormalizeDatasetID(id string) string {
	if strings.HasPrefix(id, "gcs:") || strings.HasPrefix(id, "s3:") || strings.HasPrefix(id, "file:") {
		return compactDate.ReplaceAllString(datePart.ReplaceAllString(id, "{date}"), "${1}{date}${2}")
	}
	return id
}

func engineOf(id string) (string, string) {
	if i := strings.Index(id, ":"); i > 0 {
		return id[:i], id[i+1:]
	}
	return "", id
}

// CatalogUpdate is one batch of findings.
type CatalogUpdate struct {
	Tables []CatalogTable
	Edges  []LineageEdge
	// FullScope: Tables is everything in this scope (a full metadata scan):
	// tables of the scope that are missing are marked dropped.
	FullScope string
	// ReplaceVia: Edges is everything this dag.task produces now; its other
	// edges (from earlier versions of the task) are removed.
	ReplaceVia string
	// Redefined: tables a CREATE [OR REPLACE] … AS made at that time; query
	// edges into them seen before that are removed.
	Redefined map[string]time.Time
}

// UpdateCatalog merges tables (columns, schema changes) and lineage edges.
func (s *Store) UpdateCatalog(u CatalogUpdate) error {
	now := time.Now().UTC()
	var ids []string
	seen := map[string]bool{}
	scanned := map[string]bool{}
	for i := range u.Tables {
		u.Tables[i].ID = NormalizeDatasetID(u.Tables[i].ID)
		scanned[u.Tables[i].ID] = true
		if !seen[u.Tables[i].ID] {
			seen[u.Tables[i].ID] = true
			ids = append(ids, u.Tables[i].ID)
		}
	}
	var edges []LineageEdge
	stubs := map[string]string{} // datasets only known through an edge -> who found them
	for _, e := range u.Edges {
		e.From, e.To = NormalizeDatasetID(e.From), NormalizeDatasetID(e.To)
		if e.From == e.To && strings.EqualFold(e.FromCol, e.ToCol) {
			continue
		}
		if e.LastSeen.IsZero() {
			e.LastSeen = now
		}
		edges = append(edges, e)
		for _, id := range []string{e.From, e.To} {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
				stubs[id] = e.Kind
			}
		}
	}
	cs := s.catalog()
	defer s.dropCatalogCache()
	if len(u.Redefined) > 0 {
		kept := edges[:0]
		for _, e := range edges { // edges of this batch from before the table was redefined
			if at, ok := u.Redefined[e.To]; !ok || e.Kind != "query" || !e.LastSeen.Before(at) {
				kept = append(kept, e)
			}
		}
		edges = kept
		for to, at := range u.Redefined {
			if err := cs.dropEdgesInto(NormalizeDatasetID(to), "query", at); err != nil {
				return err
			}
		}
	}
	if u.ReplaceVia != "" {
		keep := map[string]bool{}
		for _, e := range edges {
			keep[e.key()] = true
		}
		if err := cs.dropEdgesVia(u.ReplaceVia, keep); err != nil {
			return err
		}
	}
	if len(ids) > 0 {
		err := cs.updateTables(ids, func(m map[string]*CatalogTable) error {
			for _, t := range u.Tables {
				mergeTable(m, t, now)
			}
			for id, kind := range stubs {
				if m[id] == nil {
					eng, name := engineOf(id)
					m[id] = &CatalogTable{ID: id, Engine: eng, Name: name, Kind: kindOf(eng), Source: kind, SeenAt: now, Columns: []CatalogColumn{}}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if u.FullScope != "" {
		if err := cs.markDropped(u.FullScope, scanned, now); err != nil {
			return err
		}
	}
	if len(edges) > 0 {
		return cs.putEdges(edges)
	}
	return nil
}

func kindOf(engine string) string {
	switch engine {
	case "gcs", "s3", "file":
		return "file"
	case "gsheet":
		return "sheet"
	case "task":
		return "task"
	}
	return "table"
}

func mergeTable(all map[string]*CatalogTable, t CatalogTable, now time.Time) {
	if t.Engine == "" {
		t.Engine, t.Name = engineOf(t.ID)
		if i := strings.Index(t.ID, ":"); i > 0 {
			t.Name = t.ID[i+1:]
		}
	}
	cur := all[t.ID]
	if cur == nil {
		cur = &CatalogTable{ID: t.ID, Engine: t.Engine, Name: t.Name, Kind: firstNonEmptyStr(t.Kind, kindOf(t.Engine)), Source: t.Source, Scope: t.Scope, Columns: []CatalogColumn{}}
		all[t.ID] = cur
	}
	cur.SeenAt, cur.Dropped = now, nil
	if t.Kind != "" {
		cur.Kind = t.Kind
	}
	if t.Scope != "" {
		cur.Scope = t.Scope
	}
	if cur.Source != "scan" && t.Source == "scan" {
		cur.Source = "scan"
	}
	if t.CreatedAt != nil {
		cur.CreatedAt = t.CreatedAt
	}
	if t.Creator != "" {
		cur.Creator = t.Creator
	}
	for _, w := range t.Writers {
		if !containsStr(cur.Writers, w) {
			cur.Writers = append(cur.Writers, w)
			if len(cur.Writers) > 20 {
				cur.Writers = cur.Writers[len(cur.Writers)-20:]
			}
		}
	}
	if t.Columns == nil { // nothing known about the columns this time
		return
	}
	byName := map[string]*CatalogColumn{}
	for i := range cur.Columns {
		byName[strings.ToLower(cur.Columns[i].Name)] = &cur.Columns[i]
	}
	var ch SchemaChange
	present := map[string]bool{}
	first := len(cur.Columns) == 0
	for _, c := range t.Columns {
		k := strings.ToLower(c.Name)
		present[k] = true
		if old := byName[k]; old != nil {
			if old.Removed != nil {
				old.Removed = nil
				ch.Added = append(ch.Added, c.Name)
			}
			if c.Type != "" && old.Type != "" && !strings.EqualFold(c.Type, old.Type) {
				if ch.Retyped == nil {
					ch.Retyped = map[string]string{}
				}
				ch.Retyped[c.Name] = old.Type + " -> " + c.Type
			}
			if c.Type != "" {
				old.Type = c.Type
			}
			old.LastSeen = now
			continue
		}
		cur.Columns = append(cur.Columns, CatalogColumn{Name: c.Name, Type: c.Type, FirstSeen: now, LastSeen: now})
		byName[k] = &cur.Columns[len(cur.Columns)-1]
		for i := range cur.Columns { // the slice may have moved
			byName[strings.ToLower(cur.Columns[i].Name)] = &cur.Columns[i]
		}
		if !first {
			ch.Added = append(ch.Added, c.Name)
		}
	}
	for i := range cur.Columns {
		c := &cur.Columns[i]
		if !present[strings.ToLower(c.Name)] && c.Removed == nil {
			d := now
			c.Removed = &d
			ch.Removed = append(ch.Removed, c.Name)
		}
	}
	if len(ch.Added)+len(ch.Removed)+len(ch.Retyped) > 0 {
		ch.At = now
		cur.Changes = append(cur.Changes, ch)
		if len(cur.Changes) > 100 {
			cur.Changes = cur.Changes[len(cur.Changes)-100:]
		}
	}
}

func containsStr(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// CatalogSummary is one row of the catalog list.
type CatalogSummary struct {
	ID         string     `json:"id"`
	Engine     string     `json:"engine"`
	Name       string     `json:"name"`
	Kind       string     `json:"kind"`
	Columns    int        `json:"columns"`
	Source     string     `json:"source"`
	Creator    string     `json:"creator,omitempty"`
	CreatedAt  *time.Time `json:"created_at,omitempty"`
	SeenAt     time.Time  `json:"seen_at"`
	Dropped    *time.Time `json:"dropped,omitempty"`
	Changes    int        `json:"changes"`
	LastChange *time.Time `json:"last_change,omitempty"`
	Writers    []string   `json:"writers,omitempty"`
}

func summarize(t *CatalogTable) CatalogSummary {
	it := CatalogSummary{ID: t.ID, Engine: t.Engine, Name: t.Name, Kind: t.Kind, Source: t.Source, Creator: t.Creator,
		CreatedAt: t.CreatedAt, SeenAt: t.SeenAt, Dropped: t.Dropped, Changes: len(t.Changes), Writers: t.Writers}
	for _, c := range t.Columns {
		if c.Removed == nil {
			it.Columns++
		}
	}
	if n := len(t.Changes); n > 0 {
		at := t.Changes[n-1].At
		it.LastChange = &at
	}
	return it
}

// liveColumnNames: lower-case names of the current columns, one per line (search).
func liveColumnNames(t *CatalogTable) string {
	var b strings.Builder
	for _, c := range t.Columns {
		if c.Removed == nil {
			b.WriteString(strings.ToLower(c.Name))
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// CatalogSummaries lists the datasets whose id or a column name contains q
// (case-insensitive; "" = all), sorted by id.
func (s *Store) CatalogSummaries(q string) ([]CatalogSummary, error) {
	return s.catalog().summaries(strings.ToLower(strings.TrimSpace(q)))
}

// CatalogTable returns one dataset and the edges into and out of it.
func (s *Store) CatalogTable(id string) (*CatalogTable, []LineageEdge, []LineageEdge, error) {
	cs := s.catalog()
	m, err := cs.tables([]string{id})
	if err != nil {
		return nil, nil, nil, err
	}
	up, err := cs.edges(id, true)
	if err != nil {
		return nil, nil, nil, err
	}
	down, err := cs.edges(id, false)
	if err != nil {
		return nil, nil, nil, err
	}
	up, down = fresh(up), fresh(down)
	sortEdges(up)
	sortEdges(down)
	return m[id], up, down, nil
}

// CatalogEdgeTTL: lineage not seen for this long is hidden, and removed by
// `db clean` (DAGSFLOW_CATALOG_EDGE_TTL_DAYS, default 90; 0 keeps it forever).
func CatalogEdgeTTL() time.Duration {
	days := 90
	if v := os.Getenv("DAGSFLOW_CATALOG_EDGE_TTL_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			days = n
		}
	}
	return time.Duration(days) * 24 * time.Hour
}

// fresh drops edges older than the TTL.
func fresh(l []LineageEdge) []LineageEdge {
	ttl := CatalogEdgeTTL()
	if ttl == 0 {
		return l
	}
	cut := time.Now().Add(-ttl)
	out := l[:0:0]
	for _, e := range l {
		if !e.LastSeen.Before(cut) {
			out = append(out, e)
		}
	}
	return out
}

// CleanCatalog removes lineage not seen within edgeTTL and datasets dropped
// longer than droppedTTL ago (0 skips that part).
func (s *Store) CleanCatalog(edgeTTL, droppedTTL time.Duration) (edges, tables int, err error) {
	cs := s.catalog()
	defer s.dropCatalogCache()
	if edgeTTL > 0 {
		if edges, err = cs.expireEdges(time.Now().Add(-edgeTTL)); err != nil {
			return
		}
	}
	if droppedTTL > 0 {
		tables, err = cs.purgeDropped(time.Now().Add(-droppedTTL))
	}
	return
}

func sortEdges(l []LineageEdge) {
	sort.Slice(l, func(i, j int) bool { return l[i].key() < l[j].key() })
}

// LineageNode is one step of a column's lineage.
type LineageNode struct {
	Dataset  string    `json:"dataset"`
	Column   string    `json:"column,omitempty"`
	Expr     string    `json:"expr,omitempty"`
	Via      string    `json:"via,omitempty"`
	LastSeen time.Time `json:"last_seen"`
	Depth    int       `json:"depth"`
}

// ColumnLineage walks the column (or table, when column is "") up or down,
// passing through task datasets, up to depth steps.
func (s *Store) ColumnLineage(id, column string, upstream bool, depth int) ([]LineageNode, error) {
	cs := s.catalog()
	if depth <= 0 {
		depth = 10
	}
	byDS := map[string][]LineageEdge{}
	edgesOf := func(ds string) ([]LineageEdge, error) {
		if e, ok := byDS[ds]; ok {
			return e, nil
		}
		e, err := cs.edges(ds, upstream)
		if err != nil {
			return nil, err
		}
		e = fresh(e)
		sortEdges(e)
		byDS[ds] = e
		return e, nil
	}
	type key struct{ ds, col string }
	seen := map[key]bool{{id, strings.ToLower(column)}: true}
	var out []LineageNode
	frontier := []key{{id, column}}
	for d := 1; d <= depth && len(frontier) > 0 && len(out) < 5000; d++ {
		var next []key
		for _, k := range frontier {
			list, err := edgesOf(k.ds)
			if err != nil {
				return nil, err
			}
			for _, e := range list {
				// a column follows column edges; a table ("") follows every edge
				var match bool
				var nk key
				if upstream {
					match = k.col == "" || strings.EqualFold(e.ToCol, k.col)
					nk = key{e.From, e.FromCol}
				} else {
					match = k.col == "" || strings.EqualFold(e.FromCol, k.col)
					nk = key{e.To, e.ToCol}
				}
				if !match {
					continue
				}
				if k.col == "" {
					nk.col = ""
				}
				lk := key{nk.ds, strings.ToLower(nk.col)}
				if seen[lk] {
					continue
				}
				seen[lk] = true
				n := LineageNode{Dataset: nk.ds, Column: nk.col, Expr: e.Expr, Via: e.Via, LastSeen: e.LastSeen, Depth: d}
				if k.col == "" {
					n.Expr = ""
				}
				out = append(out, n)
				next = append(next, nk)
			}
		}
		frontier = next
	}
	return out, nil
}

// SyncState is where an incremental sync stopped (per scope).
func (s *Store) SyncState(scope string) (time.Time, error) {
	var m map[string]time.Time
	if err := s.readTable("catalog_sync", &m); err != nil {
		return time.Time{}, err
	}
	return m[scope], nil
}

func (s *Store) SetSyncState(scope string, at time.Time) error {
	var m map[string]time.Time
	return s.updateTable("catalog_sync", &m, func() error {
		if m == nil {
			m = map[string]time.Time{}
		}
		m[scope] = at
		return nil
	})
}

// RecordTaskLineage stores what a task reported in its "dagsflow_lineage" XCom.
//
// replace: this is everything the task produces (not one instance of a
// mapped task), so edges it reported before and not now are removed.
func (s *Store) RecordTaskLineage(dagID, taskID string, v any, replace bool) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var rep struct {
		Datasets []struct {
			ID      string `json:"id"`
			Kind    string `json:"kind"`
			Columns []struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"columns"`
		} `json:"datasets"`
		Edges []LineageEdge `json:"edges"`
	}
	if err := json.Unmarshal(b, &rep); err != nil {
		return err
	}
	via := dagID + "." + taskID
	var u CatalogUpdate
	for _, d := range rep.Datasets {
		t := CatalogTable{ID: d.ID, Kind: d.Kind, Source: "dagsflow", Writers: nil}
		if d.Columns != nil {
			t.Columns = []CatalogColumn{}
			for _, c := range d.Columns {
				t.Columns = append(t.Columns, CatalogColumn{Name: c.Name, Type: c.Type})
			}
		}
		u.Tables = append(u.Tables, t)
	}
	if replace {
		u.ReplaceVia = via
	}
	writes := map[string]bool{}
	for _, e := range rep.Edges {
		e.Via, e.Kind = via, "dagsflow"
		u.Edges = append(u.Edges, e)
		writes[NormalizeDatasetID(e.To)] = true
	}
	for i := range u.Tables {
		if writes[NormalizeDatasetID(u.Tables[i].ID)] {
			u.Tables[i].Writers = []string{via}
		}
	}
	return s.UpdateCatalog(u)
}

// CatalogColumnNames: the current columns of a dataset (nil when unknown),
// matched without case. Cached for 30 s: SQL nodes ask for every table they read.
func (s *Store) CatalogColumnNames(id string) []string {
	c := s.catCache()
	k := strings.ToLower(NormalizeDatasetID(id))
	c.mu.Lock()
	if e, ok := c.cols[k]; ok && time.Since(e.at) < 30*time.Second {
		c.mu.Unlock()
		return e.names
	}
	c.mu.Unlock()
	t, err := s.catalog().tableFold(k)
	if err != nil {
		return nil
	}
	var names []string
	if t != nil {
		for _, col := range t.Columns {
			if col.Removed == nil {
				names = append(names, col.Name)
			}
		}
	}
	c.mu.Lock()
	if c.cols == nil || len(c.cols) > 10000 {
		c.cols = map[string]colsEntry{}
	}
	c.cols[k] = colsEntry{names, time.Now()}
	c.mu.Unlock()
	return names
}

// ---- dag hooks ------------------------------------------------------------------------

// InstallCatalogHooks lets the nodes of this process use the catalog: column
// lookups for SQL lineage, and the *_catalog_sync nodes.
func (s *Store) InstallCatalogHooks() {
	dag.CatalogColumns = s.CatalogColumnNames
	dag.Catalog = catalogSink{s}
}

type catalogSink struct{ s *Store }

func (c catalogSink) SaveCatalogScan(scan dag.CatalogScan) error {
	u := CatalogUpdate{}
	if scan.FullScope {
		u.FullScope = scan.Scope
	}
	for _, t := range scan.Tables {
		ct := CatalogTable{ID: t.ID, Kind: t.Kind, Source: "scan", Scope: scan.Scope, CreatedAt: t.CreatedAt, Creator: t.Creator}
		if t.Columns != nil {
			ct.Columns = []CatalogColumn{}
			for _, col := range t.Columns {
				ct.Columns = append(ct.Columns, CatalogColumn{Name: col.Name, Type: col.Type})
			}
		}
		u.Tables = append(u.Tables, ct)
	}
	for _, e := range scan.Edges {
		u.Edges = append(u.Edges, LineageEdge{From: e.From, FromCol: e.FromCol, To: e.To, ToCol: e.ToCol, Expr: e.Expr,
			Via: e.Via, Kind: "query", LastSeen: e.At})
	}
	u.Redefined = scan.Redefined
	return c.s.UpdateCatalog(u)
}

func (c catalogSink) CatalogSyncState(scope string) (time.Time, error) { return c.s.SyncState(scope) }
func (c catalogSink) SetCatalogSyncState(scope string, at time.Time) error {
	return c.s.SetSyncState(scope, at)
}
