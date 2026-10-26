package dag

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
)

// "BigQuery external table": creates (or replaces) a BigQuery external table
// over the file a GCS dump node wrote, with exactly the column names of that
// file. Types: every column STRING (default), guessed from the data ("auto"),
// and/or set per column.

// BigQueryExternalConfig configures bigquery_external_table.
type BigQueryExternalConfig struct {
	ConnID, ProjectID, Location string
	Table                       string            // project.dataset.table or dataset.table
	RowsFrom                    string            // the GCS dump node (default: the only upstream)
	AllFiles                    bool              // uris = the folder of the file + "/*"
	DefaultType                 string            // STRING (default) | auto
	ColumnTypes                 map[string]string // column -> BigQuery type
	IfNotExists                 bool              // keep an existing table
	Expiration                  string            // table expiration: 7d, 12h, 2026-12-31 (see bq_expire.go)
	// Without a GCS node before it: describe the files yourself.
	URIs    []string
	Format  string // csv | tsv | ndjson
	Columns []string
}

var (
	bqTableRe = regexp.MustCompile("^(?:([A-Za-z0-9_.:-]+)\\.)?([A-Za-z0-9_]+)\\.([^.`]+)$")
	bqTypeRe  = regexp.MustCompile(`^(?i)(STRING|BYTES|INT64|INTEGER|FLOAT64|FLOAT|NUMERIC|BIGNUMERIC|DECIMAL|BIGDECIMAL|BOOL|BOOLEAN|DATE|DATETIME|TIME|TIMESTAMP|JSON|GEOGRAPHY)(\(\d+(,\s*\d+)?\))?$`)
)

type bqCol struct{ Name, Type string }

func (cfg BigQueryExternalConfig) check(d *DAG, id string) {
	checkExpiration(d, id, cfg.Expiration)
	if !bqTableRe.MatchString(strings.TrimSpace(cfg.Table)) {
		d.buildErr(fmt.Errorf("task %s: table must be dataset.table or project.dataset.table, got %q", id, cfg.Table))
	}
	switch strings.ToUpper(strings.TrimSpace(cfg.DefaultType)) {
	case "", "STRING", "AUTO":
	default:
		d.buildErr(fmt.Errorf("task %s: default_type must be STRING or auto", id))
	}
	for c, t := range cfg.ColumnTypes {
		if !bqTypeRe.MatchString(strings.TrimSpace(t)) {
			d.buildErr(fmt.Errorf("task %s: column_types: %q is not a BigQuery type (%s)", id, t, c))
		}
	}
	if len(cfg.URIs) > 0 {
		for _, u := range cfg.URIs {
			if !strings.HasPrefix(u, "gs://") {
				d.buildErr(fmt.Errorf("task %s: uris must start with gs://", id))
			}
		}
	}
}

// NewBigQueryExternalTableTask creates the external table.
func (d *DAG) NewBigQueryExternalTableTask(id string, cfg BigQueryExternalConfig, opts ...TaskOption) *Task {
	cfg.check(d, id)
	opts = append([]TaskOption{WithOperatorName("BigQueryCreateExternalTableOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		src, err := externalSource(ctx, cfg)
		if err != nil {
			return err
		}
		conn, _ := googleConn(cfg.ConnID)
		project := firstNonEmpty(cfg.ProjectID, str(ctx.Params["project_id"]), conn.String("project_id"))
		if k, _ := googleCreds(conn); project == "" && k != nil {
			project = k.ProjectID
		}
		m := bqTableRe.FindStringSubmatch(strings.TrimSpace(cfg.Table))
		if m[1] != "" {
			project = m[1]
		}
		if project == "" {
			return errBQProject
		}
		table := project + "." + m[2] + "." + m[3]
		exp, err := renderExpiration(ctx, cfg.Expiration)
		if err != nil {
			return err
		}
		src.expiration = exp
		ddl, cols, err := externalDDL(table, cfg, src)
		if err != nil {
			return err
		}
		ctx.Logf("BigQuery on %s:\n%s", project, ddl)
		if _, err := bqQuery(ctx, conn, project, firstNonEmpty(cfg.Location, conn.String("location")), ddl, 1, ctx.Logf); err != nil {
			return err
		}
		ctx.Logf("external table %s: %d columns over %s", table, len(cols), strings.Join(src.uris, ", "))
		schema := make([]any, len(cols))
		for i, c := range cols {
			schema[i] = map[string]any{"name": c.Name, "type": c.Type}
		}
		ctx.lineageExternal("bigquery:"+table, "gcs", src.origin, cols)
		return ctx.XComPush("return_value", map[string]any{"table": table, "uris": src.uris, "format": src.format, "columns": schema})
	}, opts...)
}

type extSource struct {
	uris       []string
	format     string // csv | tsv | ndjson
	delimiter  string
	gzip       bool
	expiration string   // SQL for expiration_timestamp
	cols       []bqCol  // name + guessed type
	origin     []string // the files as written (before all_files), for lineage
}

// externalSource reads the output of the GCS dump node, or the files listed on the node.
func externalSource(ctx *TaskContext, cfg BigQueryExternalConfig) (extSource, error) {
	var s extSource
	if len(cfg.URIs) > 0 && cfg.RowsFrom == "" && len(ctx.Task.Upstream()) == 0 {
		s.uris, s.format = cfg.URIs, strings.ToLower(firstNonEmpty(cfg.Format, "csv"))
		for _, c := range cfg.Columns {
			s.cols = append(s.cols, bqCol{c, "STRING"})
		}
		if len(s.cols) == 0 {
			return s, errors.New("no GCS node before this one: set columns (and uris, format)")
		}
	} else {
		id, err := upstreamInput(ctx, cfg.RowsFrom)
		if err != nil {
			return s, err
		}
		v, ok := ctx.XComLookup(id, "return_value")
		out, _ := v.(map[string]any)
		uri, _ := out["uri"].(string)
		if !ok || !strings.HasPrefix(uri, "gs://") {
			return s, fmt.Errorf("%q is not a Google Cloud Storage dump node (its output has no gs:// uri)", id)
		}
		s.uris = []string{uri}
		s.format, _ = out["format"].(string)
		s.delimiter, _ = out["delimiter"].(string)
		s.gzip, _ = out["gzip"].(bool)
		cols, _ := out["columns"].([]any)
		for _, c := range cols {
			cm, _ := c.(map[string]any)
			n, _ := cm["name"].(string)
			t, _ := cm["type"].(string)
			if n != "" {
				s.cols = append(s.cols, bqCol{n, firstNonEmpty(t, "STRING")})
			}
		}
		if len(cfg.URIs) > 0 {
			s.uris = cfg.URIs
		}
		if cfg.Format != "" {
			s.format = strings.ToLower(cfg.Format)
		}
		if len(s.cols) == 0 {
			return s, fmt.Errorf("%q wrote no columns (no rows?): nothing to describe", id)
		}
	}
	s.origin = append([]string(nil), s.uris...)
	if cfg.AllFiles {
		for i, u := range s.uris {
			if !strings.HasSuffix(u, "*") {
				s.uris[i] = "gs://" + path.Dir(strings.TrimPrefix(u, "gs://")) + "/*"
			}
		}
	}
	if s.format == "json" {
		return s, errors.New("BigQuery cannot read a JSON array file: set the GCS node's format to ndjson (one object per line) or csv")
	}
	return s, nil
}

func bqIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "\\`") + "`" }
func bqStr(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\t", `\t`, "\n", `\n`).Replace(s) + "'"
}

// externalDDL builds CREATE [OR REPLACE] EXTERNAL TABLE ... OPTIONS(...).
func externalDDL(table string, cfg BigQueryExternalConfig, s extSource) (string, []bqCol, error) {
	auto := strings.EqualFold(strings.TrimSpace(cfg.DefaultType), "auto")
	known := map[string]bool{}
	cols := make([]bqCol, len(s.cols))
	for i, c := range s.cols {
		known[c.Name] = true
		t := "STRING"
		if auto {
			t = c.Type
		}
		if o := strings.TrimSpace(cfg.ColumnTypes[c.Name]); o != "" {
			t = strings.ToUpper(o)
		}
		cols[i] = bqCol{c.Name, t}
	}
	var unknown []string
	for c := range cfg.ColumnTypes {
		if !known[c] {
			unknown = append(unknown, c)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return "", nil, fmt.Errorf("column_types: %s not in the file (columns: %s)", strings.Join(unknown, ", "), names(cols))
	}
	var b strings.Builder
	if cfg.IfNotExists {
		b.WriteString("CREATE EXTERNAL TABLE IF NOT EXISTS ")
	} else {
		b.WriteString("CREATE OR REPLACE EXTERNAL TABLE ")
	}
	b.WriteString(bqIdent(table) + " (\n")
	for i, c := range cols {
		sep := ","
		if i == len(cols)-1 {
			sep = ""
		}
		fmt.Fprintf(&b, "  %s %s%s\n", bqIdent(c.Name), c.Type, sep)
	}
	uris := make([]string, len(s.uris))
	for i, u := range s.uris {
		uris[i] = bqStr(u)
	}
	b.WriteString(")\nOPTIONS (\n")
	o := []string{}
	switch s.format {
	case "", "csv", "tsv":
		d := s.delimiter
		if d == "" {
			d = ","
			if s.format == "tsv" {
				d = "\t"
			}
		}
		o = append(o, "format = 'CSV'", "skip_leading_rows = 1", "field_delimiter = "+bqStr(d), "allow_quoted_newlines = true")
	case "ndjson", "jsonl":
		o = append(o, "format = 'NEWLINE_DELIMITED_JSON'")
	default:
		return "", nil, fmt.Errorf("format %q: use csv, tsv or ndjson", s.format)
	}
	o = append(o, "uris = ["+strings.Join(uris, ", ")+"]")
	if s.expiration != "" {
		o = append(o, "expiration_timestamp = "+s.expiration)
	}
	if s.gzip {
		o = append(o, "compression = 'GZIP'")
	}
	b.WriteString("  " + strings.Join(o, ",\n  ") + "\n)")
	return b.String(), cols, nil
}

func names(cols []bqCol) string {
	n := make([]string, len(cols))
	for i, c := range cols {
		n[i] = c.Name
	}
	return strings.Join(n, ", ")
}
