package dag

import (
	"strings"
)

// Lineage reported by nodes. A task pushes it in the XCom LineageXCom; the
// engine stores it in the catalog when the task succeeds and removes the
// XCom. Dataset ids are "<engine>:<name>" (see engine/catalog.go). Rows passed
// from one node to the next are the dataset task:<dag>.<task>, so a sheet ->
// gcs file -> external table -> table chain links up column by column.

const LineageXCom = "dagsflow_lineage"

// CatalogColumns returns the known columns of a dataset id (set by the
// engine; nil on a remote worker). It resolves * and unqualified names in SQL.
var CatalogColumns func(id string) []string

type LineageColumn struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

type LineageDataset struct {
	ID   string `json:"id"`
	Kind string `json:"kind,omitempty"`
	// Columns: every column of the dataset (nil when not known: a partial
	// write such as an INSERT of some columns says nothing about the others).
	Columns []LineageColumn `json:"columns,omitempty"`
}

type LineageLink struct {
	From    string `json:"from"`
	FromCol string `json:"from_col,omitempty"`
	To      string `json:"to"`
	ToCol   string `json:"to_col,omitempty"`
	Expr    string `json:"expr,omitempty"`
}

type Lineage struct {
	Datasets []LineageDataset `json:"datasets,omitempty"`
	Edges    []LineageLink    `json:"edges,omitempty"`
	Created  []string         `json:"created,omitempty"` // tables a CREATE made (not temp tables)
	// Unread: statements that may write data but could not be read
	// (CALL, dynamic SQL, syntax the reader doesn't know)
	Unread []string `json:"unread,omitempty"`
}

func (l *Lineage) add(o Lineage) {
	for _, d := range o.Datasets {
		l.setDataset(d)
	}
	l.Edges = append(l.Edges, o.Edges...)
}

func (l *Lineage) setDataset(d LineageDataset) {
	for i := range l.Datasets {
		if l.Datasets[i].ID == d.ID {
			if d.Columns != nil {
				l.Datasets[i].Columns = d.Columns
			}
			if d.Kind != "" {
				l.Datasets[i].Kind = d.Kind
			}
			return
		}
	}
	l.Datasets = append(l.Datasets, d)
}

// ReportLineage adds to what this task reports (several calls add up).
func (c *TaskContext) ReportLineage(l Lineage) {
	if len(l.Datasets)+len(l.Edges) == 0 {
		return
	}
	if c.lineage == nil {
		c.lineage = &Lineage{}
	}
	c.lineage.add(l)
	if err := c.XComPush(LineageXCom, c.lineage); err != nil {
		c.Logf("WARNING: lineage: %v", err)
	}
}

// TaskDatasetID is the dataset of the rows a task passes on.
func TaskDatasetID(dagID, taskID string) string { return "task:" + dagID + "." + taskID }

func (c *TaskContext) taskDataset() string { return TaskDatasetID(c.DAG.ID, c.Task.ID) }

func lineageCols(names []string) []LineageColumn {
	out := make([]LineageColumn, 0, len(names))
	for _, n := range names {
		out = append(out, LineageColumn{Name: n})
	}
	return out
}

// lineageRead: rows read from src (a sheet, a file) become this task's rows,
// columns renamed by rename.
func (c *TaskContext) lineageRead(src, kind string, rows []any, rename map[string]string) {
	cols := rowColumns(rows)
	if len(cols) == 0 {
		return
	}
	task := c.taskDataset()
	l := Lineage{Datasets: []LineageDataset{{ID: src, Kind: kind, Columns: lineageCols(cols)}}}
	var out []string
	for _, col := range cols {
		to := col
		if r := strings.TrimSpace(rename[col]); r != "" {
			to = r
		}
		out = append(out, to)
		l.Edges = append(l.Edges, LineageLink{From: src, FromCol: col, To: task, ToCol: to})
	}
	l.Datasets = append(l.Datasets, LineageDataset{ID: task, Kind: "task", Columns: lineageCols(out)})
	c.ReportLineage(l)
}

// lineageWrite: the rows of task up were written to dst, column for column.
// full: dst holds only these columns (a file, an overwritten sheet).
func (c *TaskContext) lineageWrite(up string, rows []any, cols []string, dst, kind string, full bool) {
	if cols == nil {
		cols = rowColumns(rows)
	}
	from := TaskDatasetID(c.DAG.ID, up)
	d := LineageDataset{ID: dst, Kind: kind}
	if full {
		d.Columns = lineageCols(cols)
	}
	l := Lineage{Datasets: []LineageDataset{d}}
	for _, col := range cols {
		l.Edges = append(l.Edges, LineageLink{From: from, FromCol: col, To: dst, ToCol: col})
	}
	if len(cols) == 0 {
		l.Edges = append(l.Edges, LineageLink{From: from, To: dst})
	}
	c.ReportLineage(l)
}

// SQLDialect says how a SQL engine names tables.
type SQLDialect struct {
	Engine   string // dataset id prefix: bigquery, redshift, athena, postgres
	BigQuery bool   // BigQuery lexing ("…" is a string)
	// Qualify turns a table name as written into the full name; "" for a name
	// that is not a table (a temp table, a CTE of the script).
	Qualify func(name string) string
}

// SQLLineageOf turns the statements of sql into lineage. resultToTask: the
// last SELECT is the task's output (a query node returning rows).
func SQLLineageOf(sql string, d SQLDialect, resultToTask string, extra ColumnsOf) Lineage {
	cols := func(t string) []string {
		if extra != nil {
			if c := extra(t); c != nil {
				return c
			}
		}
		if CatalogColumns != nil {
			if q := d.Qualify(t); q != "" {
				return CatalogColumns(d.Engine + ":" + q)
			}
		}
		return nil
	}
	var l Lineage
	type temp struct {
		cols    map[string][]SQLRef
		exprs   map[string]string // how a column was computed
		order   []string
		sources []string
	}
	temps := map[string]*temp{}
	cols = func(next ColumnsOf) ColumnsOf { // tables made earlier in the script first
		return func(t string) []string {
			if tt := temps[strings.ToLower(t)]; tt != nil {
				return tt.order
			}
			return next(t)
		}
	}(cols)
	id := func(t string) string {
		if temps[strings.ToLower(t)] != nil {
			return ""
		}
		if i := strings.Index(t, "://"); i > 0 { // COPY … FROM 's3://…'
			eng := strings.ToLower(t[:i])
			if eng == "gs" {
				eng = "gcs"
			}
			return eng + ":" + t
		}
		if q := d.Qualify(t); q != "" {
			return d.Engine + ":" + q
		}
		return ""
	}
	// refs through temp tables down to real tables
	var expand func(refs []SQLRef, depth int) []SQLRef
	expand = func(refs []SQLRef, depth int) []SQLRef {
		var out []SQLRef
		for _, r := range refs {
			t := temps[strings.ToLower(r.Table)]
			if t == nil || depth > 10 {
				out = mergeRefs(out, []SQLRef{r})
				continue
			}
			if c, ok := t.cols[strings.ToLower(r.Column)]; ok {
				out = mergeRefs(out, expand(c, depth+1))
			}
		}
		return out
	}
	var sources func(names []string, depth int) []string
	sources = func(names []string, depth int) []string {
		var out []string
		for _, n := range names {
			if t := temps[strings.ToLower(n)]; t != nil && depth < 10 {
				out = append(out, sources(t.sources, depth+1)...)
			} else {
				out = append(out, n)
			}
		}
		return out
	}
	// the expression of a plain copy of a temp table's column
	var viaExpr func(refs []SQLRef, depth int) string
	viaExpr = func(refs []SQLRef, depth int) string {
		if len(refs) != 1 || depth > 10 {
			return ""
		}
		t := temps[strings.ToLower(refs[0].Table)]
		if t == nil {
			return ""
		}
		k := strings.ToLower(refs[0].Column)
		return firstNonEmpty(t.exprs[k], viaExpr(t.cols[k], depth+1))
	}
	stmts := splitStatements(sql, d.BigQuery)
	var parsed []SQLLineage
	lastSelect := -1
	for i, st := range stmts {
		s, ok := st.parse(cols)
		if !ok {
			if writesData(st) {
				l.Unread = append(l.Unread, truncate(rawText(st), 200))
			}
			parsed = append(parsed, SQLLineage{})
			continue
		}
		if s.Kind == "copy" { // every column of the table comes from the file's column of that name
			names := []string{}
			for _, c := range s.Columns {
				names = append(names, c.Name)
			}
			if len(names) == 0 {
				names = cols(s.Target)
			}
			s.Columns = nil
			for _, n := range names {
				s.Columns = append(s.Columns, SQLColumn{Name: n, From: []SQLRef{{s.Sources[0], n}}})
			}
		}
		if s.Kind == "select" && len(s.Columns) > 0 {
			lastSelect = i
		}
		if s.Kind != "select" && (s.Temp || temps[strings.ToLower(s.Target)] != nil || d.Qualify(s.Target) == "") && !strings.Contains(s.Target, "://") {
			t := temps[strings.ToLower(s.Target)]
			if t == nil || s.Kind == "create" {
				t = &temp{cols: map[string][]SQLRef{}, exprs: map[string]string{}}
				temps[strings.ToLower(s.Target)] = t
			}
			t.sources = append(t.sources, s.Sources...)
			for _, c := range s.Columns {
				k := strings.ToLower(c.Name)
				if _, ok := t.cols[k]; !ok {
					t.order = append(t.order, c.Name)
				}
				t.cols[k] = mergeRefs(t.cols[k], expand(c.From, 0))
				if e := firstNonEmpty(c.Expr, viaExpr(c.From, 0)); e != "" {
					t.exprs[k] = e
				}
			}
			s.Kind = "temp"
		}
		parsed = append(parsed, s)
	}
	for i, s := range parsed {
		if s.Kind == "" || s.Kind == "temp" || s.Kind == "select" && (i != lastSelect || resultToTask == "") {
			continue
		}
		to := resultToTask
		if s.Kind != "select" {
			to = id(s.Target)
		}
		if to == "" {
			continue
		}
		ds := LineageDataset{ID: to, Kind: "table"}
		if s.View {
			ds.Kind = "view"
		}
		if s.Kind == "select" {
			ds.Kind = "task"
		}
		if (s.Kind == "create" || s.Kind == "select") && len(s.Columns) > 0 {
			var names []string
			for _, c := range s.Columns {
				names = append(names, c.Name)
			}
			ds.Columns = lineageCols(names)
		}
		l.setDataset(ds)
		if s.Kind == "create" {
			l.Created = append(l.Created, to)
		}
		linked := map[string]bool{}
		for _, c := range s.Columns {
			for _, r := range expand(c.From, 0) {
				from := id(r.Table)
				if from == "" {
					continue
				}
				linked[from] = true
				l.Edges = append(l.Edges, LineageLink{From: from, FromCol: r.Column, To: to, ToCol: c.Name, Expr: firstNonEmpty(c.Expr, viaExpr(c.From, 0))})
			}
		}
		for _, src := range sources(s.Sources, 0) { // tables read only in joins / filters
			if from := id(src); from != "" && !linked[from] && from != to {
				l.Edges = append(l.Edges, LineageLink{From: from, To: to})
			}
		}
	}
	return l
}

// splitName splits a.b.c, `p.d.t` (already unquoted by the parser).
func splitName(n string) []string { return strings.Split(n, ".") }

// BigQueryDialect: project.dataset.table; dataset.table uses project.
func BigQueryDialect(project string) SQLDialect {
	return SQLDialect{Engine: "bigquery", BigQuery: true, Qualify: func(n string) string {
		p := splitName(n)
		switch len(p) {
		case 3:
			if strings.HasPrefix(strings.ToLower(p[1]), "information_schema") || strings.HasPrefix(strings.ToLower(p[0]), "region-") {
				return ""
			}
			return n
		case 2:
			if project == "" || strings.EqualFold(p[0], "information_schema") {
				return ""
			}
			return project + "." + n
		}
		return ""
	}}
}

// RedshiftDialect: database.schema.table, lower case; schema.table uses
// database; a bare name is in public.
func RedshiftDialect(engine, database, defaultSchema string) SQLDialect {
	return SQLDialect{Engine: engine, Qualify: func(n string) string {
		n = strings.ToLower(n)
		p := splitName(n)
		if len(p) > 0 && (strings.HasPrefix(p[0], "pg_") || strings.HasPrefix(p[0], "svv_") || strings.HasPrefix(p[0], "stl_") ||
			strings.HasPrefix(p[0], "sys_") || p[0] == "information_schema") {
			return ""
		}
		switch len(p) {
		case 3:
			return n
		case 2:
			if database == "" {
				return n
			}
			return strings.ToLower(database) + "." + n
		case 1:
			if defaultSchema == "" {
				return ""
			}
			q := strings.ToLower(defaultSchema) + "." + n
			if database != "" {
				q = strings.ToLower(database) + "." + q
			}
			return q
		}
		return ""
	}}
}

// AthenaDialect: database.table (lower case); a bare name uses database.
func AthenaDialect(database string) SQLDialect {
	return SQLDialect{Engine: "athena", Qualify: func(n string) string {
		n = strings.ToLower(n)
		p := splitName(n)
		switch len(p) {
		case 3: // awsdatacatalog.db.t
			return p[1] + "." + p[2]
		case 2:
			if p[0] == "information_schema" {
				return ""
			}
			return n
		case 1:
			if database == "" {
				return ""
			}
			return strings.ToLower(database) + "." + n
		}
		return ""
	}}
}

// lineageSQL reports the lineage of SQL a node ran.
func (c *TaskContext) lineageSQL(sql string, d SQLDialect, rowsOut bool, extra ColumnsOf) {
	defer func() { // lineage must never fail a task
		if r := recover(); r != nil {
			c.Logf("WARNING: lineage: could not read the SQL (%v)", r)
		}
	}()
	task := ""
	if rowsOut {
		task = c.taskDataset()
	}
	l := SQLLineageOf(sql, d, task, extra)
	for _, u := range l.Unread {
		c.Logf("lineage: could not read this statement, its lineage is missing: %s", u)
	}
	l.Unread = nil
	c.ReportLineage(l)
}

// lineageExternal: an external table over files, column for column.
func (c *TaskContext) lineageExternal(table, fileEngine string, files []string, cols []bqCol) {
	c.lineageExternalMapped(table, fileEngine, files, cols, cols)
}

// lineageSchema reports a table's columns with their types, as the
// warehouse has them after the load (SQL alone gives names, not types).
func (c *TaskContext) lineageSchema(id string, cols []bqCol) { c.lineageSchemaKind(id, "table", cols) }

func (c *TaskContext) lineageSchemaKind(id, kind string, cols []bqCol) {
	if len(cols) == 0 {
		return
	}
	lc := make([]LineageColumn, len(cols))
	for i, col := range cols {
		lc[i] = LineageColumn{Name: col.Name, Type: col.Type}
	}
	c.ReportLineage(Lineage{Datasets: []LineageDataset{{ID: id, Kind: kind, Columns: lc}}})
}

// tableCols: known columns by table name (as the SQL writes it).
func tableCols(m map[string][]string) ColumnsOf {
	return func(t string) []string {
		for k, v := range m {
			if strings.EqualFold(k, t) && len(v) > 0 {
				return v
			}
		}
		return nil
	}
}

func names2(cols []bqCol) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

// lineagePass: rows of the inputs become this task's rows; a column comes
// from every input that has a column of the same name.
func (c *TaskContext) lineagePass(inputs []string, out []any) {
	cols := rowColumns(out)
	if len(cols) == 0 {
		return
	}
	task := c.taskDataset()
	l := Lineage{Datasets: []LineageDataset{{ID: task, Kind: "task", Columns: lineageCols(cols)}}}
	for _, in := range inputs {
		v, _ := c.XComLookup(in, "return_value")
		rows, _ := asRows(v)
		have := map[string]bool{}
		for _, k := range rowColumns(rows) {
			have[k] = true
		}
		linked := false
		for _, col := range cols {
			if have[col] {
				linked = true
				l.Edges = append(l.Edges, LineageLink{From: TaskDatasetID(c.DAG.ID, in), FromCol: col, To: task, ToCol: col})
			}
		}
		if !linked {
			l.Edges = append(l.Edges, LineageLink{From: TaskDatasetID(c.DAG.ID, in), To: task})
		}
	}
	c.ReportLineage(l)
}

// lineageExternalMapped: like lineageExternal when the table renames the
// file's columns (Athena lower-cases them); src and cols are in the same order.
func (c *TaskContext) lineageExternalMapped(table, fileEngine string, files []string, src, cols []bqCol) {
	l := Lineage{}
	tc := make([]LineageColumn, len(cols))
	for i, col := range cols {
		tc[i] = LineageColumn{Name: col.Name, Type: col.Type}
	}
	l.Datasets = append(l.Datasets, LineageDataset{ID: table, Kind: "external", Columns: tc})
	for _, f := range files {
		for i, col := range cols {
			from := col.Name
			if len(src) == len(cols) {
				from = src[i].Name
			}
			l.Edges = append(l.Edges, LineageLink{From: fileEngine + ":" + f, FromCol: from, To: table, ToCol: col.Name})
		}
	}
	c.ReportLineage(l)
}

// writesData: a statement the reader gave up on that may move data (so
// lineage may be missing), as opposed to DECLARE, DROP, GRANT and the like.
func writesData(st sqlStatement) bool {
	if len(st) == 0 || st[0].kind != tIdent {
		return false
	}
	switch strings.ToUpper(st[0].text) {
	case "DECLARE", "SET", "DROP", "TRUNCATE", "ALTER", "GRANT", "REVOKE", "DELETE", "ASSERT", "RAISE", "RETURN", "COMMIT",
		"ROLLBACK", "ABORT", "ANALYZE", "VACUUM", "LOCK", "SHOW", "DESCRIBE", "EXPLAIN", "COMMENT", "LEAVE", "BREAK", "CONTINUE",
		"ITERATE", "UNLOAD", "EXPORT", "SELECT":
		return false
	case "CREATE":
		for _, t := range st[1:min(len(st), 6)] {
			if isWord(t, "SCHEMA") || isWord(t, "FUNCTION") || isWord(t, "PROCEDURE") || isWord(t, "ROW") || isWord(t, "USER") ||
				isWord(t, "DATABASE") || isWord(t, "INDEX") || isWord(t, "MODEL") || isWord(t, "SNAPSHOT") {
				return false
			}
		}
	}
	return true
}
