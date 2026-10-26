package dag

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// "BigQuery table (load / merge)": copies a BigQuery table (usually the
// external table of the node before it) into a physical table, all in
// BigQuery (no rows pass through dagsflow):
//
//	append  - historical: every run adds its rows (CREATE on the first run)
//	replace - only the latest load (CREATE OR REPLACE TABLE ... AS SELECT)
//	merge   - update the rows whose keys match, insert the others
//
// Rows can be filtered with conditions on any column, and metadata columns are
// added: the GCS file of the row, the load time, a job id (yyyymmddHHMMSS) and
// the load date. Columns that appear in the source later are added to the table.

// BigQueryLoadConfig configures bigquery_load_table.
type BigQueryLoadConfig struct {
	ConnID, ProjectID, Location string
	Table                       string // target: dataset.table or project.dataset.table
	SourceTable                 string // default: the table of the node before it
	RowsFrom                    string // that node (default: the only upstream)
	Mode                        string // append (default) | replace | merge
	MergeKeys                   []string
	WhenMatched                 string // update (default) | skip
	Conditions                  []LoadCondition
	ConditionMode               string // all (default) | any
	PartitionBy                 string
	ClusterBy                   []string
	ColumnMap                   map[string]ColumnSpec // source column -> new name and/or cast
	PartitionExpirationDays     float64               // drop partitions older than this (needs partition_by)
	CastMode                    string                // safe (default: SAFE_CAST, bad values become NULL) | strict (CAST, fails)
	Expiration                  string                // table expiration: 30d, 12h, 2026-12-31; a duration counts from the last load (see bq_expire.go)

	Metadata       []string          // which metadata columns: path, inserted_at, job_id, job_date (default all; ["none"] = none)
	MetadataPrefix string            // default "metadata_"
	MetadataNames  map[string]string // path -> ingest_path, inserted_at -> ingest_at, ...
	InsertedAt     string            // utc (default, TIMESTAMP) | local (DATETIME)
	JobDate        string            // local (default) | utc
	Timezone       string            // the "local" time zone, default: the server's (TZ)
}

// ColumnSpec renames and/or converts one source column. Cast is a BigQuery
// type (INT64, NUMERIC, DATE…) or an SQL expression, where {col} is the column:
// "my_project.utils.to_date({col})". A bare function name gets the column as
// its only argument: "my_project.utils.to_date".
type ColumnSpec struct {
	Name string `yaml:"name" json:"name,omitempty"`
	Cast string `yaml:"cast" json:"cast,omitempty"`
}

var fnNameRe = regexp.MustCompile("^[A-Za-z_][A-Za-z0-9_-]*([.][A-Za-z_][A-Za-z0-9_-]*)*$|^`[^`]+`$")

// castExpr returns the SQL for a column and the result type when it is known.
func castExpr(col, cast, mode string) (string, string) {
	c := strings.TrimSpace(cast)
	id := bqIdent(col)
	switch {
	case c == "":
		return id, ""
	case bqTypeRe.MatchString(c):
		t := strings.ToUpper(c)
		if strings.EqualFold(mode, "strict") {
			return "CAST(" + id + " AS " + t + ")", baseType(t)
		}
		return "SAFE_CAST(" + id + " AS " + t + ")", baseType(t)
	case strings.Contains(c, "{col}"):
		return "(" + strings.ReplaceAll(c, "{col}", id) + ")", ""
	case fnNameRe.MatchString(c):
		return c + "(" + id + ")", ""
	default:
		return "(" + c + ")", "" // an expression that names the column itself
	}
}

// baseType: NUMERIC(10,2) -> NUMERIC; INTEGER -> INT64…
func baseType(t string) string {
	if i := strings.Index(t, "("); i > 0 {
		t = t[:i]
	}
	switch t {
	case "INTEGER":
		return "INT64"
	case "FLOAT":
		return "FLOAT64"
	case "BOOLEAN":
		return "BOOL"
	case "DECIMAL":
		return "NUMERIC"
	case "BIGDECIMAL":
		return "BIGNUMERIC"
	}
	return t
}

// LoadCondition keeps the rows where column op value.
type LoadCondition struct {
	Column string `yaml:"column" json:"column"`
	Op     string `yaml:"op" json:"op"`
	Value  string `yaml:"value" json:"value"`
}

var (
	loadOps   = map[string]bool{"=": true, "!=": true, ">": true, ">=": true, "<": true, "<=": true, "contains": true, "not_contains": true, "in": true, "not_in": true, "is_null": true, "not_null": true, "is_empty": true, "not_empty": true}
	metaKinds = []string{"path", "inserted_at", "job_id", "job_date"}
	numRe     = regexp.MustCompile(`^-?\d+(\.\d+)?$`)
)

func (cfg BigQueryLoadConfig) check(d *DAG, id string) {
	if cfg.PartitionExpirationDays < 0 {
		d.buildErr(fmt.Errorf("task %s: partition_expiration_days must be positive", id))
	}
	if cfg.PartitionExpirationDays > 0 && strings.TrimSpace(cfg.PartitionBy) == "" {
		d.buildErr(fmt.Errorf("task %s: partition_expiration_days needs partition_by", id))
	}
	checkExpiration(d, id, cfg.Expiration)
	if !bqTableRe.MatchString(strings.TrimSpace(cfg.Table)) {
		d.buildErr(fmt.Errorf("task %s: table must be dataset.table or project.dataset.table, got %q", id, cfg.Table))
	}
	if cfg.SourceTable != "" && !bqTableRe.MatchString(strings.TrimSpace(cfg.SourceTable)) {
		d.buildErr(fmt.Errorf("task %s: source_table must be dataset.table or project.dataset.table", id))
	}
	switch cfg.mode() {
	case "append", "replace":
	case "merge":
		if len(cfg.MergeKeys) == 0 {
			d.buildErr(fmt.Errorf("task %s: merge needs merge_keys (the columns that identify a row)", id))
		}
	default:
		d.buildErr(fmt.Errorf("task %s: mode must be append, replace or merge", id))
	}
	if w := strings.ToLower(cfg.WhenMatched); w != "" && w != "update" && w != "skip" {
		d.buildErr(fmt.Errorf("task %s: when_matched must be update or skip", id))
	}
	if m := strings.ToLower(cfg.ConditionMode); m != "" && m != "all" && m != "any" {
		d.buildErr(fmt.Errorf("task %s: condition_mode must be all or any", id))
	}
	for i, c := range cfg.Conditions {
		if strings.TrimSpace(c.Column) == "" || !loadOps[c.Op] {
			d.buildErr(fmt.Errorf("task %s: condition %d needs a column and an op (= != > >= < <= contains not_contains in not_in is_null not_null is_empty not_empty)", id, i+1))
		}
	}
	for _, m := range cfg.Metadata {
		if m != "none" && !contains(metaKinds, m) {
			d.buildErr(fmt.Errorf("task %s: metadata: %q is not one of path, inserted_at, job_id, job_date, none", id, m))
		}
	}
	for k := range cfg.MetadataNames {
		if !contains(metaKinds, k) {
			d.buildErr(fmt.Errorf("task %s: metadata_names: %q is not one of path, inserted_at, job_id, job_date", id, k))
		}
	}
	if m := strings.ToLower(cfg.CastMode); m != "" && m != "safe" && m != "strict" {
		d.buildErr(fmt.Errorf("task %s: cast_mode must be safe or strict", id))
	}
	seenName := map[string]string{}
	for c, sp := range cfg.ColumnMap {
		if n := strings.TrimSpace(sp.Name); n != "" {
			if prev, dup := seenName[n]; dup {
				d.buildErr(fmt.Errorf("task %s: column_map: %q and %q are both renamed to %q", id, prev, c, n))
			}
			seenName[n] = c
		}
		if strings.Contains(sp.Cast, ";") {
			d.buildErr(fmt.Errorf("task %s: column_map: the cast of %q can't contain ;", id, c))
		}
	}
	if v := strings.ToLower(cfg.InsertedAt); v != "" && v != "utc" && v != "local" {
		d.buildErr(fmt.Errorf("task %s: inserted_at must be utc or local", id))
	}
	if v := strings.ToLower(cfg.JobDate); v != "" && v != "utc" && v != "local" {
		d.buildErr(fmt.Errorf("task %s: job_date must be local or utc", id))
	}
	if cfg.Timezone != "" {
		if _, err := time.LoadLocation(cfg.Timezone); err != nil {
			d.buildErr(fmt.Errorf("task %s: timezone %q: %v", id, cfg.Timezone, err))
		}
	}
}

func (cfg BigQueryLoadConfig) mode() string {
	return strings.ToLower(firstNonEmpty(cfg.Mode, "append"))
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// metaColumn is one generated column: its name, SQL value and type.
type metaColumn struct{ Kind, Name, Expr, Type string }

func (cfg BigQueryLoadConfig) metaColumns(now time.Time, external bool, uri string) ([]metaColumn, error) {
	want := cfg.Metadata
	if len(want) == 0 {
		want = metaKinds
	}
	if contains(want, "none") {
		return nil, nil
	}
	loc := time.Local
	if cfg.Timezone != "" {
		l, err := time.LoadLocation(cfg.Timezone)
		if err != nil {
			return nil, err
		}
		loc = l
	}
	local, utc := now.In(loc), now.UTC()
	prefix := cfg.MetadataPrefix
	if prefix == "" {
		prefix = "metadata_"
	}
	var out []metaColumn
	for _, k := range metaKinds {
		if !contains(want, k) {
			continue
		}
		name := strings.TrimSpace(cfg.MetadataNames[k])
		if name == "" {
			name = prefix + k
		}
		var expr, typ string
		switch k {
		case "path":
			switch {
			case external:
				expr = "_FILE_NAME" // the gs:// file each row was read from
			case uri != "":
				expr = bqStr(uri)
			default:
				expr = "CAST(NULL AS STRING)"
			}
			typ = "STRING"
		case "inserted_at":
			if strings.EqualFold(cfg.InsertedAt, "local") {
				expr, typ = "DATETIME "+bqStr(local.Format("2006-01-02 15:04:05.000000")), "DATETIME"
			} else {
				expr, typ = "TIMESTAMP "+bqStr(utc.Format("2006-01-02 15:04:05.000000")+" UTC"), "TIMESTAMP"
			}
		case "job_id":
			expr, typ = local.Format("20060102150405"), "INT64"
		case "job_date":
			d := local
			if strings.EqualFold(cfg.JobDate, "utc") {
				d = utc
			}
			expr, typ = "DATE "+bqStr(d.Format("2006-01-02")), "DATE"
		}
		out = append(out, metaColumn{k, name, expr, typ})
	}
	return out, nil
}

// condSQL turns the conditions into a WHERE clause. Values are templated.
func condSQL(ctx *TaskContext, conds []LoadCondition, any bool) (string, error) {
	var parts []string
	for _, c := range conds {
		v, err := ctx.Render(c.Value, nil)
		if err != nil {
			return "", fmt.Errorf("condition on %s: %w", c.Column, err)
		}
		col := bqIdent(c.Column)
		asStr := "CAST(" + col + " AS STRING)"
		cmp := func(op string) string {
			if numRe.MatchString(strings.TrimSpace(v)) {
				return fmt.Sprintf("SAFE_CAST(%s AS FLOAT64) %s %s", col, op, strings.TrimSpace(v))
			}
			return fmt.Sprintf("%s %s %s", asStr, op, bqStr(v))
		}
		list := func() string {
			var items []string
			for _, x := range strings.Split(v, ",") {
				if x = strings.TrimSpace(x); x != "" {
					items = append(items, bqStr(x))
				}
			}
			if len(items) == 0 {
				items = []string{"''"}
			}
			return "(" + strings.Join(items, ", ") + ")"
		}
		var p string
		switch c.Op {
		case "=":
			p = asStr + " = " + bqStr(v)
		case "!=":
			p = "(" + col + " IS NULL OR " + asStr + " != " + bqStr(v) + ")"
		case ">", ">=", "<", "<=":
			p = cmp(c.Op)
		case "contains":
			p = "STRPOS(" + asStr + ", " + bqStr(v) + ") > 0"
		case "not_contains":
			p = "COALESCE(STRPOS(" + asStr + ", " + bqStr(v) + "), 0) = 0"
		case "in":
			p = asStr + " IN " + list()
		case "not_in":
			p = "(" + col + " IS NULL OR " + asStr + " NOT IN " + list() + ")"
		case "is_null":
			p = col + " IS NULL"
		case "not_null":
			p = col + " IS NOT NULL"
		case "is_empty":
			p = "(" + col + " IS NULL OR TRIM(" + asStr + ") = '')"
		case "not_empty":
			p = "TRIM(" + asStr + ") != ''"
		}
		parts = append(parts, p)
	}
	if len(parts) == 0 {
		return "", nil
	}
	j := " AND "
	if any {
		j = " OR "
	}
	return "WHERE " + strings.Join(parts, j), nil
}

type tableInfo struct {
	exists   bool
	external bool
	cols     []bqCol
}

// bqTableInfo reads the columns (in order) and the type of a table.
func bqTableInfo(ctx *TaskContext, conn Connection, project, location, table string) (tableInfo, error) {
	m := bqTableRe.FindStringSubmatch(table)
	ds := bqIdent(m[1] + "." + m[2])
	q := fmt.Sprintf("SELECT c.column_name, c.data_type, t.table_type FROM %s.INFORMATION_SCHEMA.COLUMNS c JOIN %s.INFORMATION_SCHEMA.TABLES t USING (table_name) WHERE c.table_name = %s ORDER BY c.ordinal_position",
		ds, ds, bqStr(m[3]))
	rows, err := bqQuery(ctx, conn, project, location, q, 100000, ctx.Logf)
	if err != nil {
		if strings.Contains(err.Error(), "Not found: Dataset") {
			return tableInfo{}, nil
		}
		return tableInfo{}, fmt.Errorf("read the columns of %s: %w", table, err)
	}
	var ti tableInfo
	for _, r := range rows {
		rm, _ := r.(map[string]any)
		n, _ := rm["column_name"].(string)
		t, _ := rm["data_type"].(string)
		tt, _ := rm["table_type"].(string)
		ti.exists = true
		ti.external = ti.external || tt == "EXTERNAL"
		ti.cols = append(ti.cols, bqCol{n, t})
	}
	return ti, nil
}

// NewBigQueryLoadTableTask loads / merges the source table into the target.
func (d *DAG) NewBigQueryLoadTableTask(id string, cfg BigQueryLoadConfig, opts ...TaskOption) *Task {
	cfg.check(d, id)
	opts = append([]TaskOption{WithOperatorName("BigQueryLoadTableOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		conn, _ := googleConn(cfg.ConnID)
		project := firstNonEmpty(cfg.ProjectID, str(ctx.Params["project_id"]), conn.String("project_id"))
		if k, _ := googleCreds(conn); project == "" && k != nil {
			project = k.ProjectID
		}
		location := firstNonEmpty(cfg.Location, conn.String("location"))
		full := func(t string) (string, error) {
			m := bqTableRe.FindStringSubmatch(strings.TrimSpace(t))
			if m == nil {
				return "", fmt.Errorf("%q is not dataset.table or project.dataset.table", t)
			}
			p := firstNonEmpty(m[1], project)
			if p == "" {
				return "", errBQProject
			}
			return p + "." + m[2] + "." + m[3], nil
		}
		// source: the table of the node before it, or source_table
		srcName, uri := cfg.SourceTable, ""
		if srcName == "" {
			up, err := upstreamInput(ctx, cfg.RowsFrom)
			if err != nil {
				return fmt.Errorf("source: %w (or set source_table)", err)
			}
			v, _ := ctx.XComLookup(up, "return_value")
			out, _ := v.(map[string]any)
			srcName, _ = out["table"].(string)
			if srcName == "" {
				return fmt.Errorf("%q has no table in its output: connect a BigQuery external table / load node, or set source_table", up)
			}
			if us, _ := out["uris"].([]any); len(us) == 1 {
				uri, _ = us[0].(string)
			}
		}
		src, err := full(srcName)
		if err != nil {
			return err
		}
		dst, err := full(cfg.Table)
		if err != nil {
			return err
		}
		if project == "" {
			project = strings.SplitN(dst, ".", 2)[0]
		}
		si, err := bqTableInfo(ctx, conn, project, location, src)
		if err != nil {
			return err
		}
		if !si.exists {
			return fmt.Errorf("source table %s not found", src)
		}
		ti, err := bqTableInfo(ctx, conn, project, location, dst)
		if err != nil {
			return err
		}
		meta, err := cfg.metaColumns(time.Now(), si.external, uri)
		if err != nil {
			return err
		}
		dry := func(q string) (map[string]string, error) { return bqDryRunTypes(ctx, conn, project, location, q) }
		script, err := loadScript(ctx, cfg, src, dst, si, ti, meta, dry)
		if err != nil {
			return err
		}
		ctx.Logf("BigQuery script on %s:\n%s", project, script)
		rows, err := bqQuery(ctx, conn, project, location, script, 10, ctx.Logf)
		if err != nil {
			return err
		}
		ctx.lineageSQL(script, BigQueryDialect(project), false, tableCols(map[string][]string{src: names2(si.cols), dst: names2(ti.cols)}))
		n := int64(-1)
		if len(rows) > 0 {
			if rm, ok := rows[len(rows)-1].(map[string]any); ok {
				switch x := rm["n"].(type) {
				case float64:
					n = int64(x)
				case int64:
					n = x
				case string:
					n, _ = strconv.ParseInt(x, 10, 64)
				}
			}
		}
		if n >= 0 {
			ctx.Logf("%d rows %s into %s", n, map[string]string{"append": "added", "replace": "loaded", "merge": "merged"}[cfg.mode()], dst)
			_ = ctx.XComPush("rows_affected", n)
		}
		mc := make([]any, len(meta))
		for i, m := range meta {
			mc[i] = map[string]any{"name": m.Name, "type": m.Type}
		}
		return ctx.XComPush("return_value", map[string]any{"table": dst, "source": src, "mode": cfg.mode(), "rows": n, "metadata_columns": mc})
	}, opts...)
}

// loadScript builds the BigQuery script for the mode.
//
// dryRun, when set, returns the column types of a query (a BigQuery dry run):
// it gives the type of columns converted with an expression and checks the SQL.
func loadScript(ctx *TaskContext, cfg BigQueryLoadConfig, src, dst string, si, ti tableInfo, meta []metaColumn, dryRun func(string) (map[string]string, error)) (string, error) {
	srcSet := map[string]bool{}
	for _, c := range si.cols {
		srcSet[c.Name] = true
	}
	var unknown []string
	for c := range cfg.ColumnMap {
		if !srcSet[c] {
			unknown = append(unknown, c)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return "", fmt.Errorf("column_map: %s not in %s (columns: %s)", strings.Join(unknown, ", "), src, names(si.cols))
	}
	var sel, cols []string
	types := map[string]string{}
	outSet := map[string]string{} // output name -> source column
	needDry := false
	for _, c := range si.cols {
		sp := cfg.ColumnMap[c.Name]
		out := firstNonEmpty(strings.TrimSpace(sp.Name), c.Name)
		if prev, dup := outSet[out]; dup {
			return "", fmt.Errorf("column_map: %q and %q would both be named %q", prev, c.Name, out)
		}
		outSet[out] = c.Name
		expr, t := castExpr(c.Name, sp.Cast, cfg.CastMode)
		if strings.TrimSpace(sp.Cast) == "" {
			t = c.Type
		} else if t == "" {
			needDry = true
		}
		if expr == bqIdent(out) {
			sel = append(sel, expr)
		} else {
			sel = append(sel, expr+" AS "+bqIdent(out))
		}
		cols = append(cols, out)
		types[out] = t
	}
	for _, m := range meta {
		if _, dup := outSet[m.Name]; dup {
			return "", fmt.Errorf("metadata column %q is already a column of %s: rename it (metadata_prefix / metadata_names)", m.Name, src)
		}
		sel = append(sel, m.Expr+" AS "+bqIdent(m.Name))
		cols = append(cols, m.Name)
		types[m.Name] = m.Type
	}
	for _, c := range cfg.Conditions {
		if !srcSet[c.Column] {
			return "", fmt.Errorf("condition: %s has no column %q (columns: %s)", src, c.Column, names(si.cols))
		}
	}
	where, err := condSQL(ctx, cfg.Conditions, strings.EqualFold(cfg.ConditionMode, "any"))
	if err != nil {
		return "", err
	}
	query := "SELECT " + strings.Join(sel, ", ") + "\nFROM " + bqIdent(src)
	if where != "" {
		query += "\n" + where
	}
	if needDry && dryRun != nil {
		got, err := dryRun(query)
		if err != nil {
			return "", fmt.Errorf("check the column conversions: %w", err)
		}
		for c, t := range got {
			types[c] = t
		}
	}
	mode := cfg.mode()
	if mode == "merge" {
		var keys []string
		for _, k := range cfg.MergeKeys {
			if _, ok := outSet[k]; !ok {
				return "", fmt.Errorf("merge key %q is not a column (columns: %s)", k, strings.Join(cols, ", "))
			}
			keys = append(keys, bqIdent(k))
		}
		// one row per key, else MERGE fails on duplicates
		query += "\nQUALIFY ROW_NUMBER() OVER (PARTITION BY " + strings.Join(keys, ", ") + ") = 1"
	}
	tableOpts := ""
	if p := strings.TrimSpace(cfg.PartitionBy); p != "" {
		t, ok := types[p]
		if !ok {
			return "", fmt.Errorf("partition_by: %q is not a column (columns: %s)", p, strings.Join(cols, ", "))
		}
		switch {
		case t == "":
			return "", fmt.Errorf("partition_by: the type of %q is unknown: set a type in its cast", p)
		case t == "DATE":
			tableOpts += "\nPARTITION BY " + bqIdent(p)
		case t == "TIMESTAMP" || t == "DATETIME":
			tableOpts += "\nPARTITION BY DATE(" + bqIdent(p) + ")"
		default:
			return "", fmt.Errorf("partition_by: %q is %s; use a DATE, DATETIME or TIMESTAMP column (e.g. the job date)", p, t)
		}
	}
	if len(cfg.ClusterBy) > 0 {
		var cl []string
		for _, c := range cfg.ClusterBy {
			if _, ok := types[c]; !ok {
				return "", fmt.Errorf("cluster_by: %q is not a column", c)
			}
			cl = append(cl, bqIdent(c))
		}
		tableOpts += "\nCLUSTER BY " + strings.Join(cl, ", ")
	}
	T := bqIdent(dst)
	count := ";\nSELECT @@row_count AS n"
	exp, err := renderExpiration(ctx, cfg.Expiration)
	if err != nil {
		return "", err
	}
	var opt []string
	if exp != "" {
		opt = append(opt, "expiration_timestamp = "+exp)
	}
	if cfg.PartitionExpirationDays > 0 {
		opt = append(opt, "partition_expiration_days = "+strconv.FormatFloat(cfg.PartitionExpirationDays, 'f', -1, 64))
	}
	if mode == "replace" || !ti.exists {
		create := "CREATE TABLE "
		if mode == "replace" {
			create = "CREATE OR REPLACE TABLE "
		}
		if len(opt) > 0 {
			tableOpts += "\nOPTIONS (" + strings.Join(opt, ", ") + ")"
		}
		return create + T + tableOpts + " AS\n" + query + ";\nSELECT COUNT(*) AS n FROM " + T, nil
	}
	var b strings.Builder
	if len(opt) > 0 { // keep the expirations current on an existing table (a duration counts from this load)
		b.WriteString("ALTER TABLE " + T + " SET OPTIONS (" + strings.Join(opt, ", ") + ");\n")
	}
	have := map[string]bool{}
	for _, c := range ti.cols {
		have[c.Name] = true
	}
	for _, c := range cols { // columns that are new in the source
		if !have[c] {
			if types[c] == "" {
				return "", fmt.Errorf("column %q is new in the table but its type is unknown: set a type in its cast", c)
			}
			fmt.Fprintf(&b, "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;\n", T, bqIdent(c), types[c])
		}
	}
	qc := make([]string, len(cols))
	for i, c := range cols {
		qc[i] = bqIdent(c)
	}
	if mode == "append" {
		b.WriteString("INSERT INTO " + T + " (" + strings.Join(qc, ", ") + ")\n" + query + count)
		return b.String(), nil
	}
	var on, set, vals []string
	keySet := map[string]bool{}
	for _, k := range cfg.MergeKeys {
		keySet[k] = true
		on = append(on, "T."+bqIdent(k)+" = S."+bqIdent(k))
	}
	for _, c := range cols {
		vals = append(vals, "S."+bqIdent(c))
		if !keySet[c] {
			set = append(set, bqIdent(c)+" = S."+bqIdent(c))
		}
	}
	b.WriteString("MERGE " + T + " T\nUSING (\n" + query + "\n) S\nON " + strings.Join(on, " AND "))
	if !strings.EqualFold(cfg.WhenMatched, "skip") && len(set) > 0 {
		b.WriteString("\nWHEN MATCHED THEN UPDATE SET " + strings.Join(set, ", "))
	}
	b.WriteString("\nWHEN NOT MATCHED THEN INSERT (" + strings.Join(qc, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")" + count)
	return b.String(), nil
}

// bqDryRunTypes returns the result columns of a query without running it.
func bqDryRunTypes(ctx *TaskContext, conn Connection, project, location, sql string) (map[string]string, error) {
	body := map[string]any{"query": sql, "useLegacySql": false, "dryRun": true}
	if location != "" {
		body["location"] = location
	}
	var res bqResult
	if err := googleCall(ctx, conn, scopeBigQuery, "POST", fmt.Sprintf("%s/projects/%s/queries", bqBase, url.PathEscape(project)), body, &res); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, f := range res.Schema.Fields {
		out[f.Name] = baseType(strings.ToUpper(f.Type))
	}
	return out, nil
}
