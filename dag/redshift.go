package dag

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Amazon Redshift nodes, through the Redshift Data API (no driver, no network
// path to the cluster needed; IAM decides):
//
//	redshift_query        run SQL, return the rows
//	redshift_load_table   COPY the file an s3_write node wrote, then append | replace | merge
//	redshift_delete_table drop tables
//
// Provisioned clusters: cluster_identifier + database + db_user (or secret_arn).
// Serverless: workgroup_name + database (or secret_arn).

// RedshiftConn says where statements run.
type RedshiftConn struct {
	ConnID            string // AWS connection (default aws_default)
	Region            string
	ClusterIdentifier string
	WorkgroupName     string // Redshift Serverless
	Database          string
	DbUser            string
	SecretArn         string
}

type rsClient struct {
	cr  awsCreds
	cfg RedshiftConn
	log func(string, ...any)
}

func (r RedshiftConn) check(d *DAG, id string) {
	if r.Database == "" {
		d.buildErr(fmt.Errorf("task %s: set database", id))
	}
	if (r.ClusterIdentifier == "") == (r.WorkgroupName == "") {
		d.buildErr(fmt.Errorf("task %s: set cluster_identifier (provisioned) or workgroup_name (serverless)", id))
	}
	if r.ClusterIdentifier != "" && r.DbUser == "" && r.SecretArn == "" {
		d.buildErr(fmt.Errorf("task %s: a provisioned cluster needs db_user or secret_arn", id))
	}
}

func (r RedshiftConn) client(ctx context.Context, logf func(string, ...any)) (*rsClient, error) {
	cr, err := awsCredentialsCtx(ctx, r.ConnID)
	if err != nil {
		return nil, err
	}
	if r.Region != "" {
		cr.Region = r.Region
	}
	return &rsClient{cr: cr, cfg: r, log: logf}, nil
}

func (c *rsClient) base() map[string]any {
	b := map[string]any{"Database": c.cfg.Database}
	if c.cfg.WorkgroupName != "" {
		b["WorkgroupName"] = c.cfg.WorkgroupName
	} else {
		b["ClusterIdentifier"] = c.cfg.ClusterIdentifier
	}
	if c.cfg.SecretArn != "" {
		b["SecretArn"] = c.cfg.SecretArn
	} else if c.cfg.DbUser != "" {
		b["DbUser"] = c.cfg.DbUser
	}
	return b
}

type rsStatus struct {
	Status, Error string
	HasResultSet  bool
	ResultRows    int64
	SubStatements []struct {
		Id, Status, Error string
		HasResultSet      bool
		ResultRows        int64
	}
}

// exec runs one statement (or, with several, all of them in one transaction)
// and waits. It returns the id to read results with and the status.
func (c *rsClient) exec(ctx context.Context, sqls ...string) (string, rsStatus, error) {
	body := c.base()
	target := "RedshiftData.ExecuteStatement"
	if len(sqls) == 1 {
		body["Sql"] = sqls[0]
	} else {
		body["Sqls"] = sqls
		target = "RedshiftData.BatchExecuteStatement"
	}
	c.log("Redshift:\n%s", strings.Join(sqls, ";\n"))
	var st struct{ Id string }
	if err := awsJSON(ctx, c.cr, "redshift-data", target, body, &st); err != nil {
		return "", rsStatus{}, err
	}
	var s rsStatus
	err := waitFor(ctx, func() (bool, error) {
		s = rsStatus{}
		if err := awsJSON(ctx, c.cr, "redshift-data", "RedshiftData.DescribeStatement", map[string]any{"Id": st.Id}, &s); err != nil {
			return false, err
		}
		switch s.Status {
		case "FINISHED":
			return true, nil
		case "FAILED", "ABORTED":
			msg := s.Error
			for i, sub := range s.SubStatements {
				if sub.Error != "" {
					msg = fmt.Sprintf("statement %d: %s", i+1, sub.Error)
				}
			}
			return false, fmt.Errorf("redshift: %s", firstNonEmpty(msg, strings.ToLower(s.Status)))
		}
		return false, nil
	})
	if err != nil && ctx.Err() != nil {
		_ = awsJSON(context.Background(), c.cr, "redshift-data", "RedshiftData.CancelStatement", map[string]any{"Id": st.Id}, nil)
	}
	return st.Id, s, err
}

// rows reads the result of a statement (id, or "id:N" for the Nth of a batch).
func (c *rsClient) rows(ctx context.Context, id string, limit int) ([]any, error) {
	var out []any
	token := ""
	for {
		body := map[string]any{"Id": id}
		if token != "" {
			body["NextToken"] = token
		}
		var r struct {
			Records        [][]map[string]any
			ColumnMetadata []struct{ Name string }
			NextToken      string
		}
		if err := awsJSON(ctx, c.cr, "redshift-data", "RedshiftData.GetStatementResult", body, &r); err != nil {
			return nil, err
		}
		for _, rec := range r.Records {
			m := map[string]any{}
			for j, f := range rec {
				if j >= len(r.ColumnMetadata) {
					break
				}
				var v any
				for k, x := range f {
					if k != "isNull" {
						v = x
					}
				}
				m[r.ColumnMetadata[j].Name] = v
			}
			out = append(out, m)
			if limit > 0 && len(out) >= limit {
				return out, nil
			}
		}
		if r.NextToken == "" {
			return out, nil
		}
		token = r.NextToken
	}
}

var rsNameRe = regexp.MustCompile(`^(?:([A-Za-z0-9_$]+)\.)?([A-Za-z0-9_$]+)$`)

// rsSplit: "schema.table" (default schema public), lowercased like Redshift does.
func rsSplit(name string) (string, string, error) {
	m := rsNameRe.FindStringSubmatch(strings.Trim(strings.TrimSpace(name), `"`))
	if m == nil {
		return "", "", fmt.Errorf("%q must be schema.table", name)
	}
	return strings.ToLower(firstNonEmpty(m[1], "public")), strings.ToLower(m[2]), nil
}

func (c *rsClient) columns(ctx context.Context, schema, table string) ([]bqCol, error) {
	q := fmt.Sprintf("SELECT column_name, data_type, character_maximum_length FROM svv_columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
		whLit(schema), whLit(table))
	id, _, err := c.exec(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("read the columns of %s.%s: %w", schema, table, err)
	}
	rows, err := c.rows(ctx, id, 0)
	if err != nil {
		return nil, err
	}
	var out []bqCol
	for _, r := range rows {
		m := r.(map[string]any)
		t := str(m["data_type"])
		if n, ok := m["character_maximum_length"].(float64); ok && n > 0 && strings.Contains(t, "char") {
			t = fmt.Sprintf("varchar(%d)", int64(n))
		}
		out = append(out, bqCol{str(m["column_name"]), t})
	}
	return out, nil
}

// ---- redshift_query -----------------------------------------------------------------

type RedshiftQueryConfig struct {
	RedshiftConn
	SQL   string
	Limit int
}

func (d *DAG) NewRedshiftQueryTask(id string, cfg RedshiftQueryConfig, opts ...TaskOption) *Task {
	if strings.TrimSpace(cfg.SQL) == "" {
		d.buildErr(fmt.Errorf("task %s: redshift_query needs sql", id))
	}
	cfg.RedshiftConn.check(d, id)
	opts = append([]TaskOption{WithOperatorName("RedshiftDataOperator"), WithTemplateFields(TemplateField{Name: "sql", Raw: cfg.SQL, Lang: "sql"})}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		c, err := cfg.client(ctx, ctx.Logf)
		if err != nil {
			return err
		}
		q, err := ctx.RenderTemplateField("sql")
		if err != nil {
			return err
		}
		sid, st, err := c.exec(ctx, q)
		if err != nil {
			return err
		}
		rows := []any{}
		if st.HasResultSet {
			if rows, err = c.rows(ctx, sid, cfg.Limit); err != nil {
				return err
			}
		} else if st.ResultRows >= 0 {
			_ = ctx.XComPush("rows_affected", st.ResultRows)
		}
		ctx.Logf("%d rows returned", len(rows))
		return ctx.XComPush("return_value", rows)
	}, opts...)
}

// ---- redshift_load_table ---------------------------------------------------------------

// RedshiftLoadConfig: COPY the file of an s3_write node (or read a table),
// then append / replace / merge into the target table.
type RedshiftLoadConfig struct {
	RedshiftConn
	LoadOptions
	Table       string   // schema.table
	IAMRole     string   // role Redshift uses to read S3 (arn:aws:iam::…:role/…), or "default"
	RowsFrom    string   // the S3 node (default: the only upstream)
	SourceTable string   // instead of a file: an existing table (schema.table, or a Spectrum table)
	AllFiles    bool     // COPY every file of the folder of the file just written
	S3Region    string   // when the bucket is in another region
	DistKey     string   // first load only
	SortKey     []string // first load only
}

func (d *DAG) NewRedshiftLoadTableTask(id string, cfg RedshiftLoadConfig, opts ...TaskOption) *Task {
	if _, _, err := rsSplit(cfg.Table); err != nil {
		d.buildErr(fmt.Errorf("task %s: table: %v", id, err))
	}
	if cfg.SourceTable != "" {
		if _, _, err := rsSplit(cfg.SourceTable); err != nil {
			d.buildErr(fmt.Errorf("task %s: source_table: %v", id, err))
		}
	} else if strings.TrimSpace(cfg.IAMRole) == "" {
		d.buildErr(fmt.Errorf("task %s: redshift_load_table needs iam_role (a role the cluster can use to read the bucket, or default)", id))
	}
	cfg.RedshiftConn.check(d, id)
	cfg.LoadOptions.check(d, id)
	opts = append([]TaskOption{WithOperatorName("RedshiftLoadTableOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		c, err := cfg.client(ctx, ctx.Logf)
		if err != nil {
			return err
		}
		var s extSource
		if cfg.SourceTable == "" {
			if s, err = storeSource(ctx, cfg.RowsFrom, "s3://"); err != nil {
				return err
			}
		} else {
			ss, st, _ := rsSplit(cfg.SourceTable)
			if s.cols, err = c.columns(ctx, ss, st); err != nil {
				return err
			}
			if len(s.cols) == 0 {
				return fmt.Errorf("source table %s.%s not found", ss, st)
			}
		}
		ts, tt, _ := rsSplit(cfg.Table)
		tgt, err := c.columns(ctx, ts, tt)
		if err != nil {
			return err
		}
		pre, batch, err := redshiftLoadStatements(ctx, cfg, s, ts, tt, tgt, fmt.Sprintf("%d", time.Now().UnixNano()%1e9))
		if err != nil {
			return err
		}
		for _, q := range pre {
			if _, _, err := c.exec(ctx, q); err != nil {
				return err
			}
		}
		sid, st, err := c.exec(ctx, batch...)
		if err != nil {
			return err
		}
		n := int64(-1)
		if len(st.SubStatements) > 0 {
			last := st.SubStatements[len(st.SubStatements)-1]
			if rows, err := c.rows(ctx, firstNonEmpty(last.Id, sid+":"+strconv.Itoa(len(batch))), 1); err == nil && len(rows) == 1 {
				switch x := rows[0].(map[string]any)["n"].(type) {
				case float64:
					n = int64(x)
				}
			}
		}
		if n >= 0 {
			ctx.Logf("%d rows %s into %s.%s", n, map[string]string{"append": "added", "replace": "loaded", "merge": "merged"}[cfg.mode()], ts, tt)
			_ = ctx.XComPush("rows_affected", n)
		}
		return ctx.XComPush("return_value", map[string]any{"table": ts + "." + tt, "mode": cfg.mode(), "rows": n, "engine": "redshift"})
	}, opts...)
}

// redshiftLoadStatements returns the statements to run first, one by one
// (ALTER TABLE ADD COLUMN), and the batch (one transaction): stage the file,
// build the source rows, write them, count them.
func redshiftLoadStatements(ctx *TaskContext, cfg RedshiftLoadConfig, s extSource, ts, tt string, tgt []bqCol, suffix string) ([]string, []string, error) {
	w := whSQL{}
	T := w.table(ts + "." + tt)
	stage, src := "dagsflow_stage_"+suffix, "dagsflow_src_"+suffix
	var batch []string
	from := ""
	pathLit := ""
	srcCols := make([]bqCol, len(s.cols))
	if cfg.SourceTable != "" {
		ss, st, _ := rsSplit(cfg.SourceTable)
		from = w.table(ss + "." + st)
		copy(srcCols, s.cols)
	} else {
		if s.format == "json" {
			return nil, nil, errors.New("Redshift COPY can't read a JSON array file: set the S3 node's format to ndjson or csv")
		}
		uri := s.uris[0]
		if cfg.AllFiles {
			uri = "s3://" + path.Dir(strings.TrimPrefix(uri, "s3://")) + "/"
		}
		pathLit = uri
		var defs, qc []string
		for i, c := range s.cols {
			srcCols[i] = bqCol{c.Name, "varchar(65535)"}
			defs = append(defs, w.ident(c.Name)+" VARCHAR(65535)")
			qc = append(qc, w.ident(c.Name))
		}
		batch = append(batch, "CREATE TEMP TABLE "+stage+" ("+strings.Join(defs, ", ")+")")
		role := strings.TrimSpace(cfg.IAMRole)
		if !strings.EqualFold(role, "default") {
			role = whLit(role)
		}
		cp := "COPY " + stage + " (" + strings.Join(qc, ", ") + ")\nFROM " + whLit(uri) + "\nIAM_ROLE " + role
		switch s.format {
		case "", "csv", "tsv":
			d := s.delimiter
			if d == "" {
				d = ","
				if s.format == "tsv" {
					d = "\t"
				}
			}
			cp += "\nFORMAT AS CSV DELIMITER " + whLit(d) + " IGNOREHEADER 1"
		case "ndjson", "jsonl":
			cp += "\nFORMAT AS JSON 'auto ignorecase'"
		default:
			return nil, nil, fmt.Errorf("format %q: use csv, tsv or ndjson", s.format)
		}
		if s.gzip {
			cp += " GZIP"
		}
		if cfg.S3Region != "" {
			cp += " REGION " + whLit(cfg.S3Region)
		}
		batch = append(batch, cp+" ACCEPTINVCHARS TRUNCATECOLUMNS")
		from = stage
	}
	pathExpr := ""
	if pathLit != "" {
		pathExpr = "CAST(" + whLit(pathLit) + " AS VARCHAR(1024))"
	}
	meta, err := w.meta(cfg.LoadOptions, time.Now(), pathExpr)
	if err != nil {
		return nil, nil, err
	}
	for i := range meta {
		if meta[i].Kind == "path" {
			meta[i].Type = "varchar(1024)"
		}
	}
	sel, err := w.buildSelect(ctx, cfg.LoadOptions, from, srcCols, meta)
	if err != nil {
		return nil, nil, err
	}
	batch = append(batch, "CREATE TEMP TABLE "+src+" AS\n"+sel.sql)
	qc := make([]string, len(sel.cols))
	for i, c := range sel.cols {
		qc[i] = w.ident(c)
	}
	insert := "INSERT INTO " + T + " (" + strings.Join(qc, ", ") + ")\nSELECT " + strings.Join(qc, ", ") + " FROM " + src
	var pre []string
	if len(tgt) == 0 {
		opt := ""
		if k := strings.TrimSpace(cfg.DistKey); k != "" {
			opt += " DISTKEY(" + w.ident(k) + ")"
		}
		if len(cfg.SortKey) > 0 {
			var sk []string
			for _, k := range cfg.SortKey {
				sk = append(sk, w.ident(k))
			}
			opt += " SORTKEY(" + strings.Join(sk, ", ") + ")"
		}
		batch = append(batch, "CREATE TABLE "+T+opt+" AS SELECT * FROM "+src)
	} else {
		have := map[string]bool{}
		for _, c := range tgt {
			have[c.Name] = true
		}
		for _, c := range sel.cols {
			if have[strings.ToLower(c)] {
				continue
			}
			t := sel.types[c]
			if t == "" {
				return nil, nil, fmt.Errorf("column %q is new in the table but its type is unknown: end its cast with ::type (e.g. my_schema.f({col})::date)", c)
			}
			pre = append(pre, "ALTER TABLE "+T+" ADD COLUMN "+w.ident(c)+" "+t)
		}
		switch cfg.mode() {
		case "replace":
			batch = append(batch, "DELETE FROM "+T, insert)
		case "append":
			batch = append(batch, insert)
		case "merge":
			if strings.EqualFold(cfg.WhenMatched, "skip") {
				batch = append(batch, w.insertMissingSQL(T, src, cfg.LoadOptions, sel.cols))
			} else {
				batch = append(batch, w.mergeSQL(T, src, cfg.LoadOptions, sel.cols))
			}
		}
	}
	batch = append(batch, "SELECT COUNT(*) AS n FROM "+src)
	return pre, batch, nil
}

// ---- redshift_delete_table ---------------------------------------------------------------

type RedshiftDeleteConfig struct {
	RedshiftConn
	Tables        []string
	FailIfMissing bool
}

func (d *DAG) NewRedshiftDeleteTableTask(id string, cfg RedshiftDeleteConfig, opts ...TaskOption) *Task {
	if len(cfg.Tables) == 0 {
		d.buildErr(fmt.Errorf("task %s: redshift_delete_table needs tables", id))
	}
	for _, t := range cfg.Tables {
		if !strings.Contains(t, "{{") {
			if _, _, err := rsSplit(t); err != nil {
				d.buildErr(fmt.Errorf("task %s: %v", id, err))
			}
		}
	}
	cfg.RedshiftConn.check(d, id)
	opts = append([]TaskOption{WithOperatorName("RedshiftDeleteTableOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		c, err := cfg.client(ctx, ctx.Logf)
		if err != nil {
			return err
		}
		var sqls, done []string
		w := whSQL{}
		for _, raw := range cfg.Tables {
			name, err := ctx.Render(raw, nil)
			if err != nil {
				return err
			}
			s, t, err := rsSplit(name)
			if err != nil {
				return err
			}
			q := "DROP TABLE IF EXISTS " + w.table(s+"."+t)
			if cfg.FailIfMissing {
				q = "DROP TABLE " + w.table(s+"."+t)
			}
			sqls = append(sqls, q)
			done = append(done, s+"."+t)
		}
		if _, _, err := c.exec(ctx, sqls...); err != nil {
			return err
		}
		return ctx.XComPush("return_value", map[string]any{"dropped": anyList(done)})
	}, opts...)
}
