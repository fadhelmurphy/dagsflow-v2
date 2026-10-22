package dag

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Amazon Athena nodes (Athena REST API, no SDK):
//
//	athena_query          run SQL, return the rows
//	athena_external_table external table over the file an s3_write node wrote
//	athena_load_table     load / merge into an Iceberg table (append | replace | merge)
//	athena_delete_table   drop tables
//
// Credentials and region come from the AWS connection (aws_default), like s3_write.

// AthenaConn says where queries run.
type AthenaConn struct {
	ConnID         string // AWS connection (default aws_default)
	Region         string // default: the connection's
	Workgroup      string // default "primary"
	OutputLocation string // s3://bucket/athena-results/ (needed unless the workgroup sets one)
	Catalog        string // default AwsDataCatalog
	Database       string // default database for table names without one
}

type athenaClient struct {
	cr  awsCreds
	cfg AthenaConn
	log func(string, ...any)
}

func (a AthenaConn) client(ctx context.Context, logf func(string, ...any)) (*athenaClient, error) {
	cr, err := awsCredentialsCtx(ctx, a.ConnID)
	if err != nil {
		return nil, err
	}
	if a.Region != "" {
		cr.Region = a.Region
	}
	return &athenaClient{cr: cr, cfg: a, log: logf}, nil
}

// run executes one statement and waits for it; it returns the execution id.
func (c *athenaClient) run(ctx context.Context, sql string) (string, error) {
	c.log("Athena:\n%s", sql)
	body := map[string]any{"QueryString": sql, "WorkGroup": firstNonEmpty(c.cfg.Workgroup, "primary"),
		"QueryExecutionContext": map[string]any{"Catalog": firstNonEmpty(c.cfg.Catalog, "AwsDataCatalog")}}
	if c.cfg.Database != "" {
		body["QueryExecutionContext"].(map[string]any)["Database"] = c.cfg.Database
	}
	if c.cfg.OutputLocation != "" {
		body["ResultConfiguration"] = map[string]any{"OutputLocation": c.cfg.OutputLocation}
	}
	var st struct{ QueryExecutionId string }
	if err := awsJSON(ctx, c.cr, "athena", "AmazonAthena.StartQueryExecution", body, &st); err != nil {
		return "", err
	}
	err := waitFor(ctx, func() (bool, error) {
		var q struct {
			QueryExecution struct {
				Status struct {
					State, StateChangeReason string
					AthenaError              struct{ ErrorMessage string }
				}
			}
		}
		if err := awsJSON(ctx, c.cr, "athena", "AmazonAthena.GetQueryExecution", map[string]any{"QueryExecutionId": st.QueryExecutionId}, &q); err != nil {
			return false, err
		}
		s := q.QueryExecution.Status
		switch s.State {
		case "SUCCEEDED":
			return true, nil
		case "FAILED", "CANCELLED":
			return false, fmt.Errorf("athena query %s: %s", strings.ToLower(s.State), firstNonEmpty(s.StateChangeReason, s.AthenaError.ErrorMessage))
		}
		return false, nil
	})
	if err != nil && ctx.Err() != nil {
		_ = awsJSON(context.Background(), c.cr, "athena", "AmazonAthena.StopQueryExecution", map[string]any{"QueryExecutionId": st.QueryExecutionId}, nil)
	}
	return st.QueryExecutionId, err
}

type athenaCol struct {
	Name, Type       string
	Precision, Scale int
}

// results reads the rows of a finished query (up to limit), the columns and
// the update count of DML (-1 when not reported).
func (c *athenaClient) results(ctx context.Context, id string, limit int) ([]any, []athenaCol, int64, error) {
	var rows []any
	var cols []athenaCol
	updates := int64(-1)
	token, first := "", true
	for {
		body := map[string]any{"QueryExecutionId": id, "MaxResults": 1000}
		if token != "" {
			body["NextToken"] = token
		}
		var r struct {
			UpdateCount *int64
			ResultSet   struct {
				Rows []struct {
					Data []struct{ VarCharValue *string }
				}
				ResultSetMetadata struct{ ColumnInfo []athenaCol }
			}
			NextToken string
		}
		if err := awsJSON(ctx, c.cr, "athena", "AmazonAthena.GetQueryResults", body, &r); err != nil {
			return nil, nil, -1, err
		}
		if r.UpdateCount != nil {
			updates = *r.UpdateCount
		}
		if cols == nil {
			cols = r.ResultSet.ResultSetMetadata.ColumnInfo
		}
		for i, row := range r.ResultSet.Rows {
			if first && i == 0 && len(cols) > 0 && len(row.Data) == len(cols) && row.Data[0].VarCharValue != nil && *row.Data[0].VarCharValue == cols[0].Name {
				continue // the header row of a SELECT
			}
			m := map[string]any{}
			for j, d := range row.Data {
				if j < len(cols) {
					m[cols[j].Name] = athenaValue(cols[j].Type, d.VarCharValue)
				}
			}
			rows = append(rows, m)
			if limit > 0 && len(rows) >= limit {
				return rows, cols, updates, nil
			}
		}
		first = false
		if r.NextToken == "" {
			return rows, cols, updates, nil
		}
		token = r.NextToken
	}
}

func athenaValue(typ string, v *string) any {
	if v == nil {
		return nil
	}
	switch strings.ToLower(typ) {
	case "tinyint", "smallint", "integer", "bigint":
		if n, err := strconv.ParseInt(*v, 10, 64); err == nil {
			return n
		}
	case "double", "float", "real":
		if f, err := strconv.ParseFloat(*v, 64); err == nil {
			return f
		}
	case "boolean":
		return *v == "true"
	}
	return *v
}

// query runs a statement and returns its rows.
func (c *athenaClient) query(ctx context.Context, sql string, limit int) ([]any, []athenaCol, int64, error) {
	id, err := c.run(ctx, sql)
	if err != nil {
		return nil, nil, -1, err
	}
	return c.results(ctx, id, limit)
}

// columns of db.table from information_schema (in order); none when it doesn't exist.
func (c *athenaClient) columns(ctx context.Context, db, table string) ([]bqCol, error) {
	q := fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
		whLit(strings.ToLower(db)), whLit(strings.ToLower(table)))
	rows, _, _, err := c.query(ctx, q, 0)
	if err != nil {
		return nil, fmt.Errorf("read the columns of %s.%s: %w", db, table, err)
	}
	var out []bqCol
	for _, r := range rows {
		m := r.(map[string]any)
		out = append(out, bqCol{str(m["column_name"]), str(m["data_type"])})
	}
	return out, nil
}

var athenaNameRe = regexp.MustCompile(`^(?:([A-Za-z0-9_-]+)\.)?([A-Za-z0-9_]+)$`)

// split "db.table" (or "table" with the default database).
func (a AthenaConn) split(name string) (string, string, error) {
	m := athenaNameRe.FindStringSubmatch(strings.Trim(strings.TrimSpace(name), "`\""))
	if m == nil {
		return "", "", fmt.Errorf("%q must be database.table (letters, digits and _)", name)
	}
	db := firstNonEmpty(m[1], a.Database)
	if db == "" {
		return "", "", fmt.Errorf("%q: set the database (database.table, or the node's database)", name)
	}
	return strings.ToLower(db), strings.ToLower(m[2]), nil
}

func athenaDDLName(db, t string) string { return "`" + db + "`.`" + t + "`" }

// ---- athena_query -------------------------------------------------------------------

// AthenaQueryConfig runs SQL; the rows are the output.
type AthenaQueryConfig struct {
	AthenaConn
	SQL   string
	Limit int
}

func (d *DAG) NewAthenaQueryTask(id string, cfg AthenaQueryConfig, opts ...TaskOption) *Task {
	if strings.TrimSpace(cfg.SQL) == "" {
		d.buildErr(fmt.Errorf("task %s: athena_query needs sql", id))
	}
	opts = append([]TaskOption{WithOperatorName("AthenaOperator"), WithTemplateFields(TemplateField{Name: "sql", Raw: cfg.SQL, Lang: "sql"})}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		c, err := cfg.client(ctx, ctx.Logf)
		if err != nil {
			return err
		}
		q, err := ctx.RenderTemplateField("sql")
		if err != nil {
			return err
		}
		rows, _, n, err := c.query(ctx, q, cfg.Limit)
		if err != nil {
			return err
		}
		if rows == nil {
			rows = []any{}
		}
		if n >= 0 {
			_ = ctx.XComPush("rows_affected", n)
		}
		ctx.Logf("%d rows returned", len(rows))
		return ctx.XComPush("return_value", rows)
	}, opts...)
}

// ---- athena_external_table ------------------------------------------------------------

// AthenaExternalConfig: an external table over the folder of the file an
// s3_write node wrote (Athena reads every file of a folder).
type AthenaExternalConfig struct {
	AthenaConn
	Table       string            // database.table
	RowsFrom    string            // the S3 node (default: the only upstream)
	DefaultType string            // STRING (default) | auto
	ColumnTypes map[string]string // column -> type
	IfNotExists bool
	// Without an S3 node: describe the files yourself.
	Location string // s3://bucket/folder/
	Format   string // csv | tsv | ndjson
	Columns  []string
}

var hiveNameBad = regexp.MustCompile(`[^a-z0-9_]+`)

// hiveName: Glue column names are lowercase letters, digits and _.
func hiveName(s string) string {
	n := strings.Trim(hiveNameBad.ReplaceAllString(strings.ToLower(s), "_"), "_")
	if n == "" {
		n = "col"
	}
	if n[0] >= '0' && n[0] <= '9' {
		n = "c_" + n
	}
	return n
}

// hiveType: the DDL (Hive) name of a type.
func hiveType(t string) string {
	t = whSQL{athena: true}.typ(t)
	switch {
	case strings.HasPrefix(t, "varchar"), t == "char":
		return "string"
	case t == "varbinary":
		return "binary"
	case t == "real":
		return "float"
	case t == "integer":
		return "int"
	case strings.HasPrefix(t, "timestamp"):
		return "timestamp"
	}
	return t
}

func (d *DAG) NewAthenaExternalTableTask(id string, cfg AthenaExternalConfig, opts ...TaskOption) *Task {
	if _, _, err := cfg.split(cfg.Table); err != nil {
		d.buildErr(fmt.Errorf("task %s: table: %v", id, err))
	}
	switch strings.ToUpper(cfg.DefaultType) {
	case "", "STRING", "AUTO":
	default:
		d.buildErr(fmt.Errorf("task %s: default_type must be STRING or auto", id))
	}
	opts = append([]TaskOption{WithOperatorName("AthenaCreateExternalTableOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		db, tb, _ := cfg.split(cfg.Table)
		var s extSource
		if cfg.Location != "" && cfg.RowsFrom == "" && len(ctx.Task.Upstream()) == 0 {
			s = extSource{uris: []string{cfg.Location}, format: strings.ToLower(firstNonEmpty(cfg.Format, "csv"))}
			for _, c := range cfg.Columns {
				s.cols = append(s.cols, bqCol{c, "STRING"})
			}
			if len(s.cols) == 0 {
				return errors.New("no S3 node before this one: set columns (and location, format)")
			}
		} else {
			var err error
			s, err = storeSource(ctx, cfg.RowsFrom, "s3://")
			if err != nil {
				return err
			}
		}
		if s.format == "json" {
			return errors.New("Athena can't read a JSON array file: set the S3 node's format to ndjson or csv")
		}
		loc := s.uris[0]
		if !strings.HasSuffix(loc, "/") {
			loc = "s3://" + path.Dir(strings.TrimPrefix(loc, "s3://")) + "/"
		}
		ddl, cols, err := athenaExternalDDL(db, tb, cfg, s, loc)
		if err != nil {
			return err
		}
		c, err := cfg.client(ctx, ctx.Logf)
		if err != nil {
			return err
		}
		if !cfg.IfNotExists {
			if _, err := c.run(ctx, "DROP TABLE IF EXISTS "+athenaDDLName(db, tb)); err != nil {
				return err
			}
		}
		if _, err := c.run(ctx, ddl); err != nil {
			return err
		}
		ctx.Logf("external table %s.%s: %d columns over %s (every file in that folder)", db, tb, len(cols), loc)
		schema := make([]any, len(cols))
		for i, c := range cols {
			schema[i] = map[string]any{"name": c.Name, "type": c.Type}
		}
		return ctx.XComPush("return_value", map[string]any{"table": db + "." + tb, "uris": []any{loc}, "format": s.format,
			"columns": schema, "engine": "athena", "external": true})
	}, opts...)
}

// storeSource reads the output of a gcs_write / s3_write node.
func storeSource(ctx *TaskContext, from, scheme string) (extSource, error) {
	var s extSource
	id, err := upstreamInput(ctx, from)
	if err != nil {
		return s, err
	}
	v, _ := ctx.XComLookup(id, "return_value")
	out, _ := v.(map[string]any)
	uri, _ := out["uri"].(string)
	if !strings.HasPrefix(uri, scheme) {
		return s, fmt.Errorf("%q is not a %s dump node (its output has no %s uri)", id, strings.TrimSuffix(scheme, "://"), scheme)
	}
	s.uris = []string{uri}
	s.format, _ = out["format"].(string)
	s.delimiter, _ = out["delimiter"].(string)
	s.gzip, _ = out["gzip"].(bool)
	cols, _ := out["columns"].([]any)
	for _, c := range cols {
		cm, _ := c.(map[string]any)
		if n, _ := cm["name"].(string); n != "" {
			t, _ := cm["type"].(string)
			s.cols = append(s.cols, bqCol{n, firstNonEmpty(t, "STRING")})
		}
	}
	if len(s.cols) == 0 {
		return s, fmt.Errorf("%q wrote no columns (no rows?): nothing to describe", id)
	}
	return s, nil
}

func athenaExternalDDL(db, tb string, cfg AthenaExternalConfig, s extSource, loc string) (string, []bqCol, error) {
	auto := strings.EqualFold(cfg.DefaultType, "auto")
	seen := map[string]string{}
	mapping := map[string]string{}
	var cols []bqCol
	var defs []string
	for _, c := range s.cols {
		n := hiveName(c.Name)
		if prev, dup := seen[n]; dup {
			return "", nil, fmt.Errorf("columns %q and %q both become %q in Athena (lowercase letters, digits and _)", prev, c.Name, n)
		}
		seen[n] = c.Name
		if n != c.Name {
			mapping[n] = c.Name
		}
		t := "string"
		if auto {
			t = hiveType(c.Type)
		}
		if o := strings.TrimSpace(cfg.ColumnTypes[c.Name]); o != "" {
			t = hiveType(o)
		}
		cols = append(cols, bqCol{n, t})
		defs = append(defs, "  `"+n+"` "+t)
	}
	for c := range cfg.ColumnTypes {
		if _, ok := seen[hiveName(c)]; !ok {
			return "", nil, fmt.Errorf("column_types: %q is not in the file", c)
		}
	}
	create := "CREATE EXTERNAL TABLE "
	if cfg.IfNotExists {
		create += "IF NOT EXISTS "
	}
	var b strings.Builder
	b.WriteString(create + athenaDDLName(db, tb) + " (\n" + strings.Join(defs, ",\n") + "\n)\n")
	props := []string{}
	switch s.format {
	case "", "csv", "tsv":
		d := s.delimiter
		if d == "" {
			d = ","
			if s.format == "tsv" {
				d = "\t"
			}
		}
		b.WriteString("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\nWITH SERDEPROPERTIES ('separatorChar' = " + hiveStr(d) + ", 'quoteChar' = '\"', 'escapeChar' = '\\\\')\n")
		props = append(props, "'skip.header.line.count' = '1'")
	case "ndjson", "jsonl":
		sp := []string{"'case.insensitive' = 'true'"}
		keys := make([]string, 0, len(mapping))
		for k := range mapping {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			sp = append(sp, "'mapping."+k+"' = "+hiveStr(mapping[k]))
		}
		b.WriteString("ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\nWITH SERDEPROPERTIES (" + strings.Join(sp, ", ") + ")\n")
	default:
		return "", nil, fmt.Errorf("format %q: use csv, tsv or ndjson", s.format)
	}
	b.WriteString("LOCATION " + hiveStr(loc))
	if len(props) > 0 {
		b.WriteString("\nTBLPROPERTIES (" + strings.Join(props, ", ") + ")")
	}
	return b.String(), cols, nil
}

func hiveStr(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\t", `\t`).Replace(s) + "'"
}

// ---- athena_load_table ------------------------------------------------------------------

// AthenaLoadConfig loads the source table into an Iceberg table.
type AthenaLoadConfig struct {
	AthenaConn
	LoadOptions
	Table         string   // database.table
	TableLocation string   // s3://bucket/warehouse/ : the table's data goes to <location>/<table>/
	SourceTable   string   // default: the table of the node before it
	RowsFrom      string   // that node
	PartitionBy   []string // columns, or Iceberg transforms: day(col), month(col), bucket(16, col)
}

func (d *DAG) NewAthenaLoadTableTask(id string, cfg AthenaLoadConfig, opts ...TaskOption) *Task {
	if _, _, err := cfg.split(cfg.Table); err != nil {
		d.buildErr(fmt.Errorf("task %s: table: %v", id, err))
	}
	if cfg.SourceTable != "" {
		if _, _, err := cfg.split(cfg.SourceTable); err != nil {
			d.buildErr(fmt.Errorf("task %s: source_table: %v", id, err))
		}
	}
	if !strings.HasPrefix(cfg.TableLocation, "s3://") {
		d.buildErr(fmt.Errorf("task %s: athena_load_table needs table_location (s3://bucket/warehouse/), where the Iceberg table keeps its data", id))
	}
	cfg.LoadOptions.check(d, id)
	opts = append([]TaskOption{WithOperatorName("AthenaLoadTableOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		c, err := cfg.client(ctx, ctx.Logf)
		if err != nil {
			return err
		}
		srcName, external, pathLit := cfg.SourceTable, false, ""
		if srcName == "" {
			up, err := upstreamInput(ctx, cfg.RowsFrom)
			if err != nil {
				return fmt.Errorf("source: %w (or set source_table)", err)
			}
			v, _ := ctx.XComLookup(up, "return_value")
			out, _ := v.(map[string]any)
			srcName, _ = out["table"].(string)
			if srcName == "" {
				return fmt.Errorf("%q has no table in its output: connect an Athena external table / load node, or set source_table", up)
			}
			external, _ = out["external"].(bool)
			if us, _ := out["uris"].([]any); len(us) == 1 {
				pathLit, _ = us[0].(string)
			}
		}
		sdb, stb, err := cfg.split(srcName)
		if err != nil {
			return err
		}
		ddb, dtb, _ := cfg.split(cfg.Table)
		src, err := c.columns(ctx, sdb, stb)
		if err != nil {
			return err
		}
		if len(src) == 0 {
			return fmt.Errorf("source table %s.%s not found", sdb, stb)
		}
		tgt, err := c.columns(ctx, ddb, dtb)
		if err != nil {
			return err
		}
		stmts, err := athenaLoadStatements(ctx, cfg, sdb+"."+stb, ddb, dtb, src, tgt, external, pathLit, func(q string) (map[string]string, error) {
			_, cols, _, err := c.query(ctx, "SELECT * FROM (\n"+q+"\n) LIMIT 0", 0)
			out := map[string]string{}
			for _, col := range cols {
				t := strings.ToLower(col.Type)
				if t == "decimal" {
					t = fmt.Sprintf("decimal(%d,%d)", col.Precision, col.Scale)
				}
				out[col.Name] = t
			}
			return out, err
		})
		if err != nil {
			return err
		}
		n := int64(-1)
		for _, s := range stmts {
			id, err := c.run(ctx, s)
			if err != nil {
				return err
			}
			if _, _, u, err := c.results(ctx, id, 1); err == nil && u >= 0 {
				n = u
			}
		}
		if n >= 0 {
			ctx.Logf("%d rows written to %s.%s", n, ddb, dtb)
			_ = ctx.XComPush("rows_affected", n)
		}
		return ctx.XComPush("return_value", map[string]any{"table": ddb + "." + dtb, "source": sdb + "." + stb, "mode": cfg.mode(), "rows": n, "engine": "athena"})
	}, opts...)
}

// athenaLoadStatements: the statements to run, in order. typesOf returns the
// output types of a query (for columns that are new in the target).
func athenaLoadStatements(ctx *TaskContext, cfg AthenaLoadConfig, src, db, tb string, srcCols, tgtCols []bqCol, external bool, pathLit string,
	typesOf func(string) (map[string]string, error)) ([]string, error) {
	w := whSQL{athena: true}
	pathExpr := ""
	if external {
		pathExpr = `"$path"`
	} else if pathLit != "" {
		pathExpr = whLit(pathLit)
	}
	meta, err := w.meta(cfg.LoadOptions, time.Now(), pathExpr)
	if err != nil {
		return nil, err
	}
	sel, err := w.buildSelect(ctx, cfg.LoadOptions, w.table(src), srcCols, meta)
	if err != nil {
		return nil, err
	}
	T := w.table(db + "." + tb)
	qc := make([]string, len(sel.cols))
	for i, c := range sel.cols {
		qc[i] = w.ident(c)
	}
	insert := "INSERT INTO " + T + " (" + strings.Join(qc, ", ") + ")\n" + sel.sql
	if len(tgtCols) == 0 { // first load: CTAS creates the Iceberg table
		var parts []string
		for _, p := range cfg.PartitionBy {
			p = strings.TrimSpace(p)
			if t, ok := sel.types[p]; ok && (strings.HasPrefix(t, "timestamp")) {
				p = "day(" + p + ")"
			}
			parts = append(parts, whLit(p))
		}
		with := []string{"table_type = 'ICEBERG'", "is_external = false", "format = 'PARQUET'",
			"location = " + whLit(strings.TrimSuffix(cfg.TableLocation, "/")+"/"+tb+"/")}
		if len(parts) > 0 {
			with = append(with, "partitioning = ARRAY["+strings.Join(parts, ", ")+"]")
		}
		return []string{"CREATE TABLE " + T + "\nWITH (" + strings.Join(with, ", ") + ")\nAS " + sel.sql}, nil
	}
	have := map[string]bool{}
	for _, c := range tgtCols {
		have[c.Name] = true
	}
	var add []string
	var unknown []string
	for _, c := range sel.cols {
		if !have[strings.ToLower(c)] {
			add = append(add, c)
			if sel.types[c] == "" {
				unknown = append(unknown, c)
			}
		}
	}
	var stmts []string
	if len(add) > 0 {
		if len(unknown) > 0 && typesOf != nil {
			got, err := typesOf(sel.sql)
			if err != nil {
				return nil, fmt.Errorf("read the types of the new columns: %w", err)
			}
			for c, t := range got {
				if sel.types[c] == "" {
					sel.types[c] = t
				}
			}
		}
		var defs []string
		for _, c := range add {
			if sel.types[c] == "" {
				return nil, fmt.Errorf("column %q is new in the table but its type is unknown: set a type in its cast", c)
			}
			defs = append(defs, "`"+hiveName(c)+"` "+hiveType(sel.types[c]))
		}
		stmts = append(stmts, "ALTER TABLE "+athenaDDLName(db, tb)+" ADD COLUMNS ("+strings.Join(defs, ", ")+")")
	}
	switch cfg.mode() {
	case "replace":
		stmts = append(stmts, "DELETE FROM "+T, insert)
	case "append":
		stmts = append(stmts, insert)
	case "merge":
		stmts = append(stmts, w.mergeSQL(T, "(\n"+sel.sql+"\n)", cfg.LoadOptions, sel.cols))
	}
	return stmts, nil
}

// ---- athena_delete_table ------------------------------------------------------------------

// AthenaDeleteConfig drops tables (an Iceberg table's data goes too; an
// external table's files stay in S3).
type AthenaDeleteConfig struct {
	AthenaConn
	Tables        []string
	FailIfMissing bool
}

func (d *DAG) NewAthenaDeleteTableTask(id string, cfg AthenaDeleteConfig, opts ...TaskOption) *Task {
	if len(cfg.Tables) == 0 {
		d.buildErr(fmt.Errorf("task %s: athena_delete_table needs tables", id))
	}
	for _, t := range cfg.Tables {
		if !strings.Contains(t, "{{") {
			if _, _, err := cfg.split(t); err != nil {
				d.buildErr(fmt.Errorf("task %s: %v", id, err))
			}
		}
	}
	opts = append([]TaskOption{WithOperatorName("AthenaDeleteTableOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		c, err := cfg.client(ctx, ctx.Logf)
		if err != nil {
			return err
		}
		var done []string
		for _, raw := range cfg.Tables {
			name, err := ctx.Render(raw, nil)
			if err != nil {
				return err
			}
			db, tb, err := cfg.split(name)
			if err != nil {
				return err
			}
			q := "DROP TABLE IF EXISTS " + athenaDDLName(db, tb)
			if cfg.FailIfMissing {
				q = "DROP TABLE " + athenaDDLName(db, tb)
			}
			if _, err := c.run(ctx, q); err != nil {
				return fmt.Errorf("drop %s.%s: %w", db, tb, err)
			}
			done = append(done, db+"."+tb)
		}
		return ctx.XComPush("return_value", map[string]any{"dropped": anyList(done)})
	}, opts...)
}
