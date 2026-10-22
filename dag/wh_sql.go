package dag

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Shared pieces of the Athena and Redshift load nodes: the same options as the
// BigQuery load node (modes, conditions, column map, metadata columns), written
// in their SQL (Trino for Athena, PostgreSQL-like for Redshift).

// LoadOptions are the load-table options shared by Athena and Redshift.
type LoadOptions struct {
	Mode          string // append (default) | replace | merge
	MergeKeys     []string
	WhenMatched   string // update (default) | skip
	Conditions    []LoadCondition
	ConditionMode string // all (default) | any
	ColumnMap     map[string]ColumnSpec
	CastMode      string // safe (default, Athena: TRY_CAST) | strict (CAST)

	Metadata       []string
	MetadataPrefix string
	MetadataNames  map[string]string
	InsertedAt     string // utc (default) | local
	JobDate        string // local (default) | utc
	Timezone       string
}

func (o LoadOptions) mode() string { return strings.ToLower(firstNonEmpty(o.Mode, "append")) }

// asBQ reuses the checks and metadata logic of the BigQuery load node.
func (o LoadOptions) asBQ() BigQueryLoadConfig {
	return BigQueryLoadConfig{Table: "d.t", Mode: o.Mode, MergeKeys: o.MergeKeys, WhenMatched: o.WhenMatched, Conditions: o.Conditions,
		ConditionMode: o.ConditionMode, ColumnMap: o.ColumnMap, CastMode: o.CastMode, Metadata: o.Metadata, MetadataPrefix: o.MetadataPrefix,
		MetadataNames: o.MetadataNames, InsertedAt: o.InsertedAt, JobDate: o.JobDate, Timezone: o.Timezone}
}

func (o LoadOptions) check(d *DAG, id string) { o.asBQ().check(d, id) }

// whSQL is the SQL dialect: athena (Trino) or redshift.
type whSQL struct{ athena bool }

func (w whSQL) ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func whLit(s string) string           { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func (w whSQL) asStr(col string) string {
	return "CAST(" + col + " AS VARCHAR)"
}

// num reads a column as a number, NULL when it isn't one.
func (w whSQL) num(col string) string {
	if w.athena {
		return "TRY_CAST(" + col + " AS DOUBLE)"
	}
	s := "TRIM(" + w.asStr(col) + ")"
	return "CASE WHEN " + s + " ~ '^-?[0-9]+([.][0-9]+)?$' THEN CAST(" + s + " AS FLOAT8) END"
}

// table quotes schema.table (or catalog.schema.table).
func (w whSQL) table(name string) string {
	parts := strings.Split(strings.Trim(strings.TrimSpace(name), `"`), ".")
	for i, p := range parts {
		parts[i] = w.ident(strings.Trim(p, `"`))
	}
	return strings.Join(parts, ".")
}

// cond builds the WHERE clause of the conditions (values are templated).
func (w whSQL) cond(ctx *TaskContext, conds []LoadCondition, any bool) (string, error) {
	var parts []string
	for _, c := range conds {
		v, err := ctx.Render(c.Value, nil)
		if err != nil {
			return "", fmt.Errorf("condition on %s: %w", c.Column, err)
		}
		col := w.ident(c.Column)
		s := w.asStr(col)
		list := func() string {
			var items []string
			for _, x := range strings.Split(v, ",") {
				if x = strings.TrimSpace(x); x != "" {
					items = append(items, whLit(x))
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
			p = s + " = " + whLit(v)
		case "!=":
			p = "(" + col + " IS NULL OR " + s + " <> " + whLit(v) + ")"
		case ">", ">=", "<", "<=":
			if numRe.MatchString(strings.TrimSpace(v)) {
				p = w.num(col) + " " + c.Op + " " + strings.TrimSpace(v)
			} else {
				p = s + " " + c.Op + " " + whLit(v)
			}
		case "contains":
			p = "STRPOS(" + s + ", " + whLit(v) + ") > 0"
		case "not_contains":
			p = "COALESCE(STRPOS(" + s + ", " + whLit(v) + "), 0) = 0"
		case "in":
			p = s + " IN " + list()
		case "not_in":
			p = "(" + col + " IS NULL OR " + s + " NOT IN " + list() + ")"
		case "is_null":
			p = col + " IS NULL"
		case "not_null":
			p = col + " IS NOT NULL"
		case "is_empty":
			p = "(" + col + " IS NULL OR TRIM(" + s + ") = '')"
		case "not_empty":
			p = "TRIM(" + s + ") <> ''"
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

var (
	whTypeRe = regexp.MustCompile(`(?i)^(string|varchar|char|text|bigint|int|integer|smallint|tinyint|int64|int4|int8|double|double precision|float|float4|float8|float64|real|decimal|numeric|bignumeric|boolean|bool|date|timestamp|timestamptz|timestamp with time zone|time|json|bytes|varbinary|varbyte|super)(\s*\(\s*\d+\s*(,\s*\d+\s*)?\))?$`)
	castTail = regexp.MustCompile(`(?i)::\s*([a-z][a-z0-9_ ]*(\(\s*\d+\s*(,\s*\d+\s*)?\))?)\s*$`)
)

// typ maps a type name (BigQuery or the engine's own) to the engine's.
func (w whSQL) typ(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	args := ""
	if i := strings.Index(t, "("); i > 0 {
		t, args = strings.TrimSpace(t[:i]), t[i:]
	}
	m := map[string][2]string{ // athena (Trino), redshift
		"string": {"varchar", "varchar(65535)"}, "varchar": {"varchar", "varchar"}, "char": {"char", "char"}, "text": {"varchar", "varchar(65535)"},
		"int64": {"bigint", "bigint"}, "bigint": {"bigint", "bigint"}, "int": {"integer", "integer"}, "integer": {"integer", "integer"},
		"int4": {"integer", "integer"}, "int8": {"bigint", "bigint"}, "smallint": {"smallint", "smallint"}, "tinyint": {"tinyint", "smallint"},
		"float64": {"double", "float8"}, "double": {"double", "float8"}, "double precision": {"double", "float8"}, "float": {"double", "float8"},
		"float8": {"double", "float8"}, "float4": {"real", "float4"}, "real": {"real", "float4"},
		"numeric": {"decimal(38,9)", "decimal(38,9)"}, "decimal": {"decimal(38,9)", "decimal(38,9)"}, "bignumeric": {"decimal(38,9)", "decimal(38,9)"},
		"bool": {"boolean", "boolean"}, "boolean": {"boolean", "boolean"}, "date": {"date", "date"}, "time": {"time", "time"},
		"timestamp": {"timestamp(6)", "timestamp"}, "datetime": {"timestamp(6)", "timestamp"}, "timestamptz": {"timestamp(6)", "timestamptz"},
		"timestamp with time zone": {"timestamp(6)", "timestamptz"}, "json": {"varchar", "super"}, "super": {"varchar", "super"},
		"bytes": {"varbinary", "varbyte"}, "varbinary": {"varbinary", "varbyte"}, "varbyte": {"varbinary", "varbyte"},
	}
	i := 1
	if w.athena {
		i = 0
	}
	v, ok := m[t]
	if !ok {
		return t + args
	}
	out := v[i]
	if args != "" {
		if j := strings.Index(out, "("); j > 0 {
			out = out[:j]
		}
		out += args
	}
	return out
}

// cast converts a column: a type (TRY_CAST in Athena with cast_mode safe),
// a function name, or an expression with {col}. Redshift expressions can end
// with ::type to tell the type of a new column. The type is "" when unknown.
func (w whSQL) cast(col, spec, mode string) (string, string) {
	c := strings.TrimSpace(spec)
	id := w.ident(col)
	switch {
	case c == "":
		return id, ""
	case whTypeRe.MatchString(c) || bqTypeRe.MatchString(c):
		t := w.typ(c)
		if w.athena && !strings.EqualFold(mode, "strict") {
			return "TRY_CAST(" + id + " AS " + t + ")", t
		}
		return "CAST(" + id + " AS " + t + ")", t
	}
	typ := ""
	if m := castTail.FindStringSubmatch(c); m != nil && whTypeRe.MatchString(strings.TrimSpace(m[1])) {
		typ = w.typ(m[1])
	}
	switch {
	case strings.Contains(c, "{col}"):
		return "(" + strings.ReplaceAll(c, "{col}", id) + ")", typ
	case fnNameRe.MatchString(c):
		return c + "(" + id + ")", typ
	default:
		return "(" + c + ")", typ
	}
}

// meta: the metadata columns as SQL values for this engine. pathExpr is the
// file of each row ("$path" in Athena) or a literal; "" = unknown (NULL).
func (w whSQL) meta(o LoadOptions, now time.Time, pathExpr string) ([]metaColumn, error) {
	mc, err := o.asBQ().metaColumns(now, false, "")
	if err != nil {
		return nil, err
	}
	loc := time.Local
	if o.Timezone != "" {
		if l, err := time.LoadLocation(o.Timezone); err == nil {
			loc = l
		}
	}
	local, utc := now.In(loc), now.UTC()
	for i, m := range mc {
		switch m.Kind {
		case "path":
			if pathExpr == "" {
				pathExpr = "CAST(NULL AS VARCHAR)"
			}
			mc[i].Expr, mc[i].Type = pathExpr, w.typ("string")
		case "inserted_at":
			t := utc
			if strings.EqualFold(o.InsertedAt, "local") {
				t = local
			}
			tt := w.typ("timestamp")
			mc[i].Expr, mc[i].Type = "CAST("+whLit(t.Format("2006-01-02 15:04:05.000000"))+" AS "+strings.ToUpper(tt)+")", tt
		case "job_id":
			mc[i].Expr, mc[i].Type = "CAST("+local.Format("20060102150405")+" AS BIGINT)", "bigint"
		case "job_date":
			d := local
			if strings.EqualFold(o.JobDate, "utc") {
				d = utc
			}
			mc[i].Expr, mc[i].Type = "CAST("+whLit(d.Format("2006-01-02"))+" AS DATE)", "date"
		}
	}
	return mc, nil
}

// whSelect is the SELECT over the source: renamed / converted columns, the
// metadata columns and the conditions; for a merge, one row per key.
type whSelect struct {
	sql   string            // the query
	cols  []string          // output columns, in order
	types map[string]string // known output types ("" = unknown)
}

func (w whSQL) buildSelect(ctx *TaskContext, o LoadOptions, srcTable string, src []bqCol, meta []metaColumn) (whSelect, error) {
	srcSet := map[string]bool{}
	for _, c := range src {
		srcSet[c.Name] = true
	}
	for c := range o.ColumnMap {
		if !srcSet[c] {
			return whSelect{}, fmt.Errorf("column_map: %q is not a column of %s (columns: %s)", c, srcTable, names(src))
		}
	}
	for _, c := range o.Conditions {
		if !srcSet[c.Column] {
			return whSelect{}, fmt.Errorf("condition: %s has no column %q (columns: %s)", srcTable, c.Column, names(src))
		}
	}
	out := whSelect{types: map[string]string{}}
	var sel []string
	outSet := map[string]string{}
	for _, c := range src {
		sp := o.ColumnMap[c.Name]
		name := firstNonEmpty(strings.TrimSpace(sp.Name), c.Name)
		if prev, dup := outSet[name]; dup {
			return whSelect{}, fmt.Errorf("column_map: %q and %q would both be named %q", prev, c.Name, name)
		}
		outSet[name] = c.Name
		expr, t := w.cast(c.Name, sp.Cast, o.CastMode)
		if strings.TrimSpace(sp.Cast) == "" {
			t = c.Type
		}
		sel = append(sel, expr+" AS "+w.ident(name))
		out.cols = append(out.cols, name)
		out.types[name] = t
	}
	for _, m := range meta {
		if _, dup := outSet[m.Name]; dup {
			return whSelect{}, fmt.Errorf("metadata column %q is already a column: rename it (metadata_prefix / metadata_names)", m.Name)
		}
		outSet[m.Name] = ""
		sel = append(sel, m.Expr+" AS "+w.ident(m.Name))
		out.cols = append(out.cols, m.Name)
		out.types[m.Name] = m.Type
	}
	where, err := w.cond(ctx, o.Conditions, strings.EqualFold(o.ConditionMode, "any"))
	if err != nil {
		return whSelect{}, err
	}
	q := "SELECT " + strings.Join(sel, ", ") + "\nFROM " + srcTable
	if where != "" {
		q += "\n" + where
	}
	if o.mode() == "merge" {
		var keys []string
		for _, k := range o.MergeKeys {
			if _, ok := outSet[k]; !ok {
				return whSelect{}, fmt.Errorf("merge key %q is not a column (columns: %s)", k, strings.Join(out.cols, ", "))
			}
			keys = append(keys, w.ident(k))
		}
		qc := make([]string, len(out.cols))
		for i, c := range out.cols {
			qc[i] = w.ident(c)
		}
		q = "SELECT " + strings.Join(qc, ", ") + " FROM (\n  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY " + strings.Join(keys, ", ") + " ORDER BY " + keys[0] + ") AS dagsflow_rn\n  FROM (\n" + q + "\n  ) s\n) d WHERE dagsflow_rn = 1"
	}
	out.sql = q
	return out, nil
}

// mergeSQL: MERGE INTO target USING (source) ON keys … (both engines).
func (w whSQL) mergeSQL(target, source string, o LoadOptions, cols []string) string {
	key := map[string]bool{}
	var on, set, qc, vals []string
	for _, k := range o.MergeKeys {
		key[k] = true
		on = append(on, "t."+w.ident(k)+" = s."+w.ident(k))
	}
	for _, c := range cols {
		qc = append(qc, w.ident(c))
		vals = append(vals, "s."+w.ident(c))
		if !key[c] {
			set = append(set, w.ident(c)+" = s."+w.ident(c))
		}
	}
	q := "MERGE INTO " + target + " AS t\nUSING " + source + " AS s\nON " + strings.Join(on, " AND ")
	if len(set) > 0 && !strings.EqualFold(o.WhenMatched, "skip") {
		q += "\nWHEN MATCHED THEN UPDATE SET " + strings.Join(set, ", ")
	}
	return q + "\nWHEN NOT MATCHED THEN INSERT (" + strings.Join(qc, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")"
}

// insertMissingSQL: insert only the rows whose keys are not in the target
// (merge with when_matched: skip on engines whose MERGE needs WHEN MATCHED).
func (w whSQL) insertMissingSQL(target, source string, o LoadOptions, cols []string) string {
	var on, qc []string
	for _, k := range o.MergeKeys {
		on = append(on, "t."+w.ident(k)+" = s."+w.ident(k))
	}
	for _, c := range cols {
		qc = append(qc, w.ident(c))
	}
	sc := make([]string, len(qc))
	for i, c := range qc {
		sc[i] = "s." + c
	}
	return "INSERT INTO " + target + " (" + strings.Join(qc, ", ") + ")\nSELECT " + strings.Join(sc, ", ") + " FROM " + source + " AS s\nWHERE NOT EXISTS (SELECT 1 FROM " + target + " AS t WHERE " + strings.Join(on, " AND ") + ")"
}
