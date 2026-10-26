package dag

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Catalog sync nodes: read a warehouse's own metadata on a schedule, so the
// catalog also knows the tables people make outside dagsflow (a CREATE TABLE
// in the console, a dbt model, a notebook), their columns and schema
// changes, and the column lineage of the queries that wrote them.
//
//   bigquery_catalog_sync: INFORMATION_SCHEMA.TABLES / COLUMNS (every run),
//     INFORMATION_SCHEMA.JOBS since the last run (lineage, who created what).
//   redshift_catalog_sync: SVV_TABLES / SVV_COLUMNS, SYS_QUERY_HISTORY +
//     SYS_QUERY_TEXT since the last run (Redshift keeps about 7 days).
//
// Both only read metadata views: no table data is scanned (BigQuery bills
// INFORMATION_SCHEMA queries at the 10 MB minimum per query).

// ScannedTable is one table found by a sync.
type ScannedTable struct {
	ID        string
	Kind      string
	Columns   []LineageColumn // nil: not known (only seen in a query)
	CreatedAt *time.Time
	Creator   string
}

// ScannedEdge is lineage found in a query someone ran.
type ScannedEdge struct {
	LineageLink
	Via string // job / query id and user
	At  time.Time
}

// CatalogScan is what one sync run found.
type CatalogScan struct {
	Scope     string // e.g. bigquery:proj:us
	FullScope bool   // Tables is every table of the scope (missing ones were dropped)
	Tables    []ScannedTable
	Edges     []ScannedEdge
}

// CatalogSink stores scans (the engine's store; set by the engine and workers).
type CatalogSink interface {
	SaveCatalogScan(s CatalogScan) error
	CatalogSyncState(scope string) (time.Time, error)
	SetCatalogSyncState(scope string, at time.Time) error
}

var Catalog CatalogSink

type queryFunc func(sql string) ([]any, error)

var bqDatasetRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func rowStr(r any, k string) string {
	m, _ := r.(map[string]any)
	switch v := m[k].(type) {
	case nil:
		return ""
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

func parseWhen(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05.999999999-07", "2006-01-02T15:04:05.999999999"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// ---- BigQuery ---------------------------------------------------------------------------

// BigQueryCatalogSyncConfig: which project / regions / datasets to read.
type BigQueryCatalogSyncConfig struct {
	ConnID, ProjectID string
	Regions           []string // default: the connection's location, else US
	Datasets          []string // only these datasets (default: all)
	LookbackHours     int      // first run: jobs of the last N hours (default 168)
	MaxJobs           int      // per run and region (default 5000); the rest waits for the next run
	NoJobs            bool     // tables and columns only, no query lineage
}

func (d *DAG) NewBigQueryCatalogSyncTask(id string, cfg BigQueryCatalogSyncConfig, opts ...TaskOption) *Task {
	for _, ds := range cfg.Datasets {
		if !bqDatasetRe.MatchString(ds) {
			d.buildErr(fmt.Errorf("task %s: dataset %q: letters, digits and _ only", id, ds))
		}
	}
	opts = append([]TaskOption{WithOperatorName("BigQueryCatalogSyncOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		if Catalog == nil {
			return errors.New("the catalog is not available in this process")
		}
		conn, _ := googleConn(cfg.ConnID)
		project := firstNonEmpty(cfg.ProjectID, str(ctx.Params["project_id"]), conn.String("project_id"))
		if k, _ := googleCreds(conn); project == "" && k != nil {
			project = k.ProjectID
		}
		if project == "" {
			return errBQProject
		}
		regions := cfg.Regions
		if len(regions) == 0 {
			regions = []string{firstNonEmpty(conn.String("location"), "US")}
		}
		summary := map[string]any{}
		for _, region := range regions {
			region = strings.TrimSpace(region)
			run := func(sql string) ([]any, error) {
				return bqQuery(ctx, conn, project, region, sql, 5_000_000, ctx.Logf)
			}
			scope := "bigquery:" + project + ":" + strings.ToLower(region)
			if len(cfg.Datasets) > 0 {
				scope += ":" + strings.Join(cfg.Datasets, ",")
			}
			since, err := Catalog.CatalogSyncState(scope)
			if err != nil {
				return err
			}
			if since.IsZero() {
				h := cfg.LookbackHours
				if h <= 0 {
					h = 168
				}
				since = time.Now().Add(-time.Duration(h) * time.Hour)
			}
			scan, last, err := bqCatalogScan(project, region, cfg, since, run, ctx.Logf)
			if err != nil {
				return fmt.Errorf("region %s: %w", region, err)
			}
			scan.Scope = scope
			if err := Catalog.SaveCatalogScan(scan); err != nil {
				return err
			}
			if !last.IsZero() {
				if err := Catalog.SetCatalogSyncState(scope, last); err != nil {
					return err
				}
			}
			ctx.Logf("%s: %d tables, %d lineage edges", region, len(scan.Tables), len(scan.Edges))
			summary[region] = map[string]any{"tables": len(scan.Tables), "edges": len(scan.Edges), "jobs_until": last}
		}
		return ctx.XComPush("return_value", summary)
	}, opts...)
}

// bqCatalogScan reads one region. It returns the time of the last job read
// (where the next run starts).
func bqCatalogScan(project, region string, cfg BigQueryCatalogSyncConfig, since time.Time, run queryFunc, logf func(string, ...any)) (CatalogScan, time.Time, error) {
	scan := CatalogScan{FullScope: true}
	is := "`" + project + "`.`region-" + strings.ToLower(region) + "`.INFORMATION_SCHEMA"
	filter := ""
	if len(cfg.Datasets) > 0 {
		q := make([]string, len(cfg.Datasets))
		for i, ds := range cfg.Datasets {
			q[i] = bqStr(ds)
		}
		filter = "\nWHERE t.table_schema IN (" + strings.Join(q, ", ") + ")"
	}
	rows, err := run("SELECT t.table_schema, t.table_name, t.table_type, t.creation_time, c.column_name, c.data_type\n" +
		"FROM " + is + ".TABLES t\nLEFT JOIN " + is + ".COLUMNS c USING (table_catalog, table_schema, table_name)" + filter +
		"\nORDER BY t.table_schema, t.table_name, c.ordinal_position")
	if err != nil {
		return scan, time.Time{}, fmt.Errorf("read tables: %w", err)
	}
	var cur *ScannedTable
	for _, r := range rows {
		id := "bigquery:" + project + "." + rowStr(r, "table_schema") + "." + rowStr(r, "table_name")
		if cur == nil || cur.ID != id {
			scan.Tables = append(scan.Tables, ScannedTable{ID: id, Kind: bqKind(rowStr(r, "table_type")), Columns: []LineageColumn{}})
			cur = &scan.Tables[len(scan.Tables)-1]
			if t, ok := parseWhen(rowStr(r, "creation_time")); ok {
				cur.CreatedAt = &t
			}
		}
		if c := rowStr(r, "column_name"); c != "" {
			cur.Columns = append(cur.Columns, LineageColumn{Name: c, Type: rowStr(r, "data_type")})
		}
	}
	if cfg.NoJobs {
		return scan, time.Time{}, nil
	}
	maxJobs := cfg.MaxJobs
	if maxJobs <= 0 {
		maxJobs = 5000
	}
	// one row per job: scripts as a whole (their child jobs are skipped), so
	// temp tables between statements are followed
	jobs, err := run("SELECT job_id, user_email, creation_time, statement_type, query,\n" +
		"  destination_table.project_id AS dp, destination_table.dataset_id AS dd, destination_table.table_id AS dt,\n" +
		"  TO_JSON_STRING(referenced_tables) AS refs\n" +
		"FROM " + is + ".JOBS\n" +
		"WHERE creation_time > TIMESTAMP(" + bqStr(since.UTC().Format(time.RFC3339Nano)) + ")\n" +
		"  AND job_type = 'QUERY' AND state = 'DONE' AND error_result IS NULL AND parent_job_id IS NULL\n" +
		"  AND (statement_type IN ('CREATE_TABLE_AS_SELECT', 'CREATE_TABLE', 'CREATE_VIEW', 'CREATE_MATERIALIZED_VIEW', 'INSERT', 'MERGE', 'UPDATE', 'SCRIPT')\n" +
		"    OR statement_type = 'SELECT' AND destination_table.dataset_id IS NOT NULL AND NOT STARTS_WITH(destination_table.dataset_id, '_'))\n" +
		"ORDER BY creation_time\nLIMIT " + strconv.Itoa(maxJobs))
	if err != nil {
		return scan, time.Time{}, fmt.Errorf("read jobs (needs bigquery.jobs.listAll on the project): %w", err)
	}
	if len(jobs) == maxJobs {
		logf("WARNING: %d jobs read (max_jobs); the next run continues from the last one", maxJobs)
	}
	var last time.Time
	dialect := BigQueryDialect(project)
	for _, j := range jobs {
		at, _ := parseWhen(rowStr(j, "creation_time"))
		if at.After(last) {
			last = at
		}
		dest := ""
		if dt := rowStr(j, "dt"); dt != "" {
			dest = "bigquery:" + rowStr(j, "dp") + "." + rowStr(j, "dd") + "." + dt
		}
		user := rowStr(j, "user_email")
		via := "job " + rowStr(j, "job_id")
		if user != "" {
			via += " by " + user
		}
		resultTo := ""
		if rowStr(j, "statement_type") == "SELECT" {
			resultTo = dest
		}
		l := SQLLineageOf(rowStr(j, "query"), dialect, resultTo, nil)
		if len(l.Edges) == 0 && dest != "" { // the query could not be read: tables only
			var refs []struct {
				ProjectID string `json:"project_id"`
				DatasetID string `json:"dataset_id"`
				TableID   string `json:"table_id"`
			}
			_ = json.Unmarshal([]byte(rowStr(j, "refs")), &refs)
			for _, r := range refs {
				l.Edges = append(l.Edges, LineageLink{From: "bigquery:" + r.ProjectID + "." + r.DatasetID + "." + r.TableID, To: dest})
			}
		}
		scan.addQuery(l, via, user, at)
	}
	return scan, last, nil
}

func bqKind(tableType string) string {
	switch tableType {
	case "VIEW", "MATERIALIZED VIEW":
		return "view"
	case "EXTERNAL":
		return "external"
	}
	return "table"
}

// addQuery adds the lineage of one query (or script) someone ran.
func (s *CatalogScan) addQuery(l Lineage, via, user string, at time.Time) {
	for _, e := range l.Edges {
		s.Edges = append(s.Edges, ScannedEdge{LineageLink: e, Via: via, At: at})
	}
	for _, id := range l.Created {
		t := at
		s.Tables = append(s.Tables, ScannedTable{ID: id, Creator: user, CreatedAt: &t})
	}
}

// ---- Redshift ---------------------------------------------------------------------------

type RedshiftCatalogSyncConfig struct {
	RedshiftConn
	Schemas       []string // only these schemas (default: all user schemas)
	LookbackHours int      // first run (default 168; Redshift keeps about 7 days)
	MaxQueries    int      // per run (default 5000)
	NoQueries     bool
}

func (d *DAG) NewRedshiftCatalogSyncTask(id string, cfg RedshiftCatalogSyncConfig, opts ...TaskOption) *Task {
	cfg.RedshiftConn.check(d, id)
	for _, s := range cfg.Schemas {
		if !rsNameRe.MatchString(s) || strings.Contains(s, ".") {
			d.buildErr(fmt.Errorf("task %s: schema %q: letters, digits, _ and $ only", id, s))
		}
	}
	opts = append([]TaskOption{WithOperatorName("RedshiftCatalogSyncOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		if Catalog == nil {
			return errors.New("the catalog is not available in this process")
		}
		c, err := cfg.client(ctx, ctx.Logf)
		if err != nil {
			return err
		}
		run := func(sql string) ([]any, error) {
			sid, st, err := c.exec(ctx, sql)
			if err != nil {
				return nil, err
			}
			if !st.HasResultSet {
				return nil, nil
			}
			return c.rows(ctx, sid, 0)
		}
		where := firstNonEmpty(cfg.WorkgroupName, cfg.ClusterIdentifier)
		scope := "redshift:" + where + ":" + strings.ToLower(cfg.Database)
		if len(cfg.Schemas) > 0 {
			scope += ":" + strings.ToLower(strings.Join(cfg.Schemas, ","))
		}
		since, err := Catalog.CatalogSyncState(scope)
		if err != nil {
			return err
		}
		if since.IsZero() {
			h := cfg.LookbackHours
			if h <= 0 {
				h = 168
			}
			since = time.Now().Add(-time.Duration(h) * time.Hour)
		}
		scan, last, err := rsCatalogScan(cfg, since, run, ctx.Logf)
		if err != nil {
			return err
		}
		scan.Scope = scope
		if err := Catalog.SaveCatalogScan(scan); err != nil {
			return err
		}
		if !last.IsZero() {
			if err := Catalog.SetCatalogSyncState(scope, last); err != nil {
				return err
			}
		}
		ctx.Logf("%d tables, %d lineage edges", len(scan.Tables), len(scan.Edges))
		return ctx.XComPush("return_value", map[string]any{"tables": len(scan.Tables), "edges": len(scan.Edges), "queries_until": last})
	}, opts...)
}

func rsCatalogScan(cfg RedshiftCatalogSyncConfig, since time.Time, run queryFunc, logf func(string, ...any)) (CatalogScan, time.Time, error) {
	scan := CatalogScan{FullScope: true}
	filter := "c.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal', 'pg_auto_copy', 'catalog_history')\n  AND c.table_schema NOT LIKE 'pg\\_temp%'"
	if len(cfg.Schemas) > 0 {
		q := make([]string, len(cfg.Schemas))
		for i, s := range cfg.Schemas {
			q[i] = whLit(strings.ToLower(s))
		}
		filter = "c.table_schema IN (" + strings.Join(q, ", ") + ")"
	}
	rows, err := run("SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.data_type, t.table_type\n" +
		"FROM svv_columns c JOIN svv_tables t\n  ON t.table_catalog = c.table_catalog AND t.table_schema = c.table_schema AND t.table_name = c.table_name\n" +
		"WHERE c.table_catalog = " + whLit(strings.ToLower(cfg.Database)) + " AND " + filter + "\n" +
		"ORDER BY c.table_schema, c.table_name, c.ordinal_position")
	if err != nil {
		return scan, time.Time{}, fmt.Errorf("read tables: %w", err)
	}
	var cur *ScannedTable
	for _, r := range rows {
		id := "redshift:" + strings.ToLower(rowStr(r, "table_catalog")+"."+rowStr(r, "table_schema")+"."+rowStr(r, "table_name"))
		if cur == nil || cur.ID != id {
			kind := "table"
			switch strings.ToUpper(rowStr(r, "table_type")) {
			case "VIEW":
				kind = "view"
			case "EXTERNAL TABLE":
				kind = "external"
			}
			scan.Tables = append(scan.Tables, ScannedTable{ID: id, Kind: kind, Columns: []LineageColumn{}})
			cur = &scan.Tables[len(scan.Tables)-1]
		}
		cur.Columns = append(cur.Columns, LineageColumn{Name: rowStr(r, "column_name"), Type: rowStr(r, "data_type")})
	}
	if cfg.NoQueries {
		return scan, time.Time{}, nil
	}
	maxQ := cfg.MaxQueries
	if maxQ <= 0 {
		maxQ = 5000
	}
	hist, err := run("SELECT h.query_id, h.session_id, h.user_id, h.database_name, h.start_time, h.query_type, h.query_text, LEN(h.query_text) AS text_len\n" +
		"FROM sys_query_history h\n" +
		"WHERE h.status = 'success' AND h.user_id > 1 AND h.query_type NOT IN ('SELECT', 'UNLOAD', 'UTILITY')\n" +
		"  AND h.start_time > " + whLit(since.UTC().Format("2006-01-02 15:04:05.000000")) + "\n" +
		"ORDER BY h.start_time\nLIMIT " + strconv.Itoa(maxQ))
	if err != nil {
		return scan, time.Time{}, fmt.Errorf("read query history (sys_query_history; needs a superuser or the sys:monitor role to see other users' queries): %w", err)
	}
	if len(hist) == maxQ {
		logf("WARNING: %d queries read (max_queries); the next run continues from the last one", maxQ)
	}
	// full text of long queries (query_text holds the first 4000 characters)
	var long []string
	for _, h := range hist {
		if n, _ := strconv.Atoi(rowStr(h, "text_len")); n >= 4000 {
			long = append(long, rowStr(h, "query_id"))
		}
	}
	full := map[string]string{}
	for start := 0; start < len(long); start += 200 {
		ids := long[start:min(start+200, len(long))]
		parts, err := run("SELECT query_id, sequence, text FROM sys_query_text WHERE query_id IN (" + strings.Join(ids, ", ") + ") ORDER BY query_id, sequence")
		if err != nil {
			logf("WARNING: full query text: %v (lineage of long queries may be incomplete)", err)
			break
		}
		for _, p := range parts {
			full[rowStr(p, "query_id")] += rowStr(p, "text")
		}
	}
	users := map[string]string{}
	if us, err := run("SELECT usesysid, usename FROM pg_user"); err == nil {
		for _, u := range us {
			users[rowStr(u, "usesysid")] = rowStr(u, "usename")
		}
	}
	// a session's statements together, in order: temp tables made in one
	// statement and read in the next are followed
	type session struct {
		db, user, first string
		at              time.Time
		sql             []string
	}
	var order []string
	sessions := map[string]*session{}
	var last time.Time
	for _, h := range hist {
		at, _ := parseWhen(rowStr(h, "start_time"))
		if at.After(last) {
			last = at
		}
		key := rowStr(h, "session_id") + "\x00" + rowStr(h, "database_name")
		s := sessions[key]
		if s == nil {
			s = &session{db: rowStr(h, "database_name"), user: users[rowStr(h, "user_id")], first: rowStr(h, "query_id")}
			if s.user == "" {
				s.user = "user " + rowStr(h, "user_id")
			}
			sessions[key] = s
			order = append(order, key)
		}
		s.at = at
		text := rowStr(h, "query_text")
		if f := full[rowStr(h, "query_id")]; f != "" {
			text = f
		}
		s.sql = append(s.sql, strings.ReplaceAll(strings.TrimRight(strings.TrimSpace(text), ";"), `\n`, "\n"))
	}
	for _, key := range order {
		s := sessions[key]
		l := SQLLineageOf(strings.Join(s.sql, ";\n"), RedshiftDialect("redshift", firstNonEmpty(s.db, cfg.Database), "public"), "", nil)
		scan.addQuery(l, "query "+s.first+" by "+s.user, s.user, s.at)
	}
	return scan, last, nil
}
