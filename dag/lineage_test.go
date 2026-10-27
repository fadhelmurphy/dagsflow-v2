package dag

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"
)

func edgeSet(l Lineage) []string {
	var out []string
	for _, e := range l.Edges {
		s := e.From + "." + e.FromCol + " -> " + e.To + "." + e.ToCol
		if e.Expr != "" {
			s += " [" + e.Expr + "]"
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func hasEdge(t *testing.T, l Lineage, want ...string) {
	t.Helper()
	got := strings.Join(edgeSet(l), "\n")
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Fatalf("missing edge %q in:\n%s", w, got)
		}
	}
}

func lineageOf(t *testing.T, rs *memRun, task string) Lineage {
	t.Helper()
	v, ok := rs.XComPull(task, LineageXCom)
	if !ok {
		t.Fatalf("%s reported no lineage", task)
	}
	b, _ := json.Marshal(v)
	var l Lineage
	_ = json.Unmarshal(b, &l)
	return l
}

func TestSQLLineageScriptTempTables(t *testing.T) {
	l := SQLLineageOf("DECLARE x INT64 DEFAULT 1;\nCREATE TEMP TABLE t AS SELECT id, amount * 2 AS amt FROM stg.orders;\n"+
		"INSERT INTO dw.facts (order_id, amount) SELECT id, amt FROM t JOIN stg.ok USING (id);\nSELECT COUNT(*) FROM dw.facts",
		BigQueryDialect("prj"), "", tableCols(map[string][]string{"stg.orders": {"id", "amount"}}))
	hasEdge(t, l, "bigquery:prj.stg.orders.id -> bigquery:prj.dw.facts.order_id",
		"bigquery:prj.stg.orders.amount -> bigquery:prj.dw.facts.amount", "bigquery:prj.stg.ok. -> bigquery:prj.dw.facts.")
	for _, e := range l.Edges {
		if strings.Contains(e.From, ".t") && !strings.Contains(e.From, "stg") {
			t.Fatalf("temp table leaked: %+v", e)
		}
	}
	if len(l.Created) != 0 {
		t.Fatalf("temp tables are not created tables: %v", l.Created)
	}
}

func TestSQLLineageRedshiftLoad(t *testing.T) {
	ctx := loadCtx(t)
	s := extSource{uris: []string{"s3://b/exp/2026/orders.ndjson"}, format: "ndjson", cols: []bqCol{{"order_id", "STRING"}, {"amount", "INT64"}}}
	cfg := RedshiftLoadConfig{Table: "dw.orders", IAMRole: "default", LoadOptions: LoadOptions{Metadata: []string{"job_date"}, ColumnMap: map[string]ColumnSpec{"amount": {Name: "amt", Cast: "decimal(18,2)"}}}}
	pre, batch, err := redshiftLoadStatements(ctx, cfg, s, "dw", "orders", nil, "x")
	if err != nil {
		t.Fatal(err)
	}
	l := SQLLineageOf(strings.Join(append(pre, batch...), ";\n"), RedshiftDialect("redshift", "dev", "public"), "", nil)
	hasEdge(t, l, "s3:s3://b/exp/2026/orders.ndjson.order_id -> redshift:dev.dw.orders.order_id",
		`s3:s3://b/exp/2026/orders.ndjson.amount -> redshift:dev.dw.orders.amt [CAST("amount" AS decimal(18,2))]`)
	if len(l.Created) != 1 || l.Created[0] != "redshift:dev.dw.orders" {
		t.Fatalf("created %v", l.Created)
	}
}

func TestLoadTableReportsLineage(t *testing.T) {
	setupGoogle(t)
	d := NewDAG("lt", "", WithParams(map[string]any{"project_id": "prj"}))
	src := d.NewTask("ext", func(ctx *TaskContext) error {
		return ctx.XComPush("return_value", map[string]any{"table": "prj.stg.ext", "uris": []any{"gs://b/x/*"}})
	})
	load := d.NewBigQueryLoadTableTask("load", BigQueryLoadConfig{Table: "dw.orders"})
	src.Then(load)
	rs := &memRun{}
	runDAG(t, d, rs)
	l := lineageOf(t, rs, "load")
	hasEdge(t, l, "bigquery:prj.stg.ext.order_id -> bigquery:prj.dw.orders.order_id", "bigquery:prj.stg.ext.status -> bigquery:prj.dw.orders.status")
	for _, d := range l.Datasets { // types from the table after the load
		if d.ID == "bigquery:prj.dw.orders" {
			if len(d.Columns) != 3 || d.Columns[1].Type != "INT64" || d.Columns[2].Type != "ARRAY<STRING>" {
				t.Fatalf("schema: %+v", d.Columns)
			}
			return
		}
	}
	t.Fatal("no dataset for the target")
}

func TestSheetsReportLineage(t *testing.T) {
	f := setupGoogle(t)
	f.sheet = [][]any{{"Id", "Full Name"}, {1.0, "ann"}}
	yml := `dag_id: gsl
tasks:
  - id: read
    operator: gsheet_read
    spreadsheet_id: SHEET
    range: "'Data Q1'!A1:C"
    rename: {full_name: name}
  - id: out
    operator: gcs_write
    bucket: bkt
    object: x/{{ ds }}/people.ndjson
    format: ndjson
    upstream: [read]
`
	dags, err := ParseYAML("gsl.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs)
	hasEdge(t, lineageOf(t, rs, "read"), "gsheet:SHEET!Data Q1.full_name -> task:gsl.read.name", "gsheet:SHEET!Data Q1.id -> task:gsl.read.id")
	l := lineageOf(t, rs, "out")
	if len(l.Edges) != 2 || l.Edges[0].From != "task:gsl.read" || !strings.HasPrefix(l.Edges[0].To, "gcs:gs://bkt/x/") {
		t.Fatalf("%+v", l)
	}
}

func TestCatalogScans(t *testing.T) {
	since := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var asked []string
	bq := func(sql string) ([]any, error) {
		asked = append(asked, sql)
		if strings.Contains(sql, ".JOBS") {
			return []any{
				map[string]any{"job_id": "j1", "user_email": "ann@x.com", "creation_time": "2026-10-02T01:00:00Z", "statement_type": "CREATE_TABLE_AS_SELECT",
					"query": "CREATE TABLE mart.daily AS SELECT DATE(ts) AS day, SUM(amount) AS total FROM raw.orders GROUP BY 1", "dp": "p", "dd": "mart", "dt": "daily"},
				map[string]any{"job_id": "j2", "user_email": "bob@x.com", "creation_time": "2026-10-03T01:00:00Z", "statement_type": "INSERT",
					"query": "CALL proc()", "dp": "p", "dd": "mart", "dt": "x", "refs": `[{"project_id":"p","dataset_id":"raw","table_id":"y"}]`},
			}, nil
		}
		return []any{
			map[string]any{"table_schema": "raw", "table_name": "orders", "table_type": "BASE TABLE", "creation_time": "2026-01-01T00:00:00Z", "column_name": "ts", "data_type": "TIMESTAMP"},
			map[string]any{"table_schema": "raw", "table_name": "orders", "table_type": "BASE TABLE", "column_name": "amount", "data_type": "NUMERIC"},
			map[string]any{"table_schema": "mart", "table_name": "v", "table_type": "VIEW", "column_name": "a", "data_type": "STRING"},
		}, nil
	}
	scan, last, err := bqCatalogScan("p", "US", BigQueryCatalogSyncConfig{Datasets: []string{"raw", "mart"}}, since, bq, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(asked[0], "`p`.`region-us`.INFORMATION_SCHEMA.TABLES") || !strings.Contains(asked[0], "IN ('raw', 'mart')") ||
		!strings.Contains(asked[1], "TIMESTAMP('2026-10-01T00:00:00Z')") {
		t.Fatalf("queries: %s", strings.Join(asked, "\n----\n"))
	}
	if len(scan.Tables) != 3 || scan.Tables[0].ID != "bigquery:p.raw.orders" || len(scan.Tables[0].Columns) != 2 || scan.Tables[1].Kind != "view" ||
		scan.Tables[2].ID != "bigquery:p.mart.daily" || scan.Tables[2].Creator != "ann@x.com" {
		t.Fatalf("tables %+v", scan.Tables)
	}
	var edges []string
	for _, e := range scan.Edges {
		edges = append(edges, e.From+"."+e.FromCol+" -> "+e.To+"."+e.ToCol+" | "+e.Via)
	}
	got := strings.Join(edges, "\n")
	for _, w := range []string{"bigquery:p.raw.orders.ts -> bigquery:p.mart.daily.day | job j1 by ann@x.com", "bigquery:p.raw.orders.amount -> bigquery:p.mart.daily.total",
		"bigquery:p.raw.y. -> bigquery:p.mart.x. | job j2 by bob@x.com"} {
		if !strings.Contains(got, w) {
			t.Fatalf("missing %q in\n%s", w, got)
		}
	}
	if !last.Equal(time.Date(2026, 10, 3, 1, 0, 0, 0, time.UTC)) {
		t.Fatal(last)
	}

	// Redshift: one session's statements are read together (temp tables), long text from sys_query_text
	long := "INSERT INTO public.facts SELECT id, amt FROM tmp_o" + strings.Repeat(" ", 3960)
	rs := func(sql string) ([]any, error) {
		switch {
		case strings.Contains(sql, "svv_columns"):
			return []any{map[string]any{"table_catalog": "dev", "table_schema": "public", "table_name": "facts", "column_name": "id", "data_type": "integer", "table_type": "BASE TABLE"}}, nil
		case strings.Contains(sql, "sys_query_history"):
			return []any{
				map[string]any{"query_id": 10.0, "session_id": 7.0, "user_id": 100.0, "database_name": "dev", "start_time": "2026-10-02 01:00:00.123", "query_text": `CREATE TEMP TABLE tmp_o AS SELECT order_id AS id,\namount AS amt FROM stg.orders`, "text_len": 70.0},
				map[string]any{"query_id": 11.0, "session_id": 7.0, "user_id": 100.0, "database_name": "dev", "start_time": "2026-10-02 01:00:01", "query_text": long[:4000], "text_len": 4000.0},
			}, nil
		case strings.Contains(sql, "sys_query_text"):
			return []any{map[string]any{"query_id": 11.0, "sequence": 0.0, "text": long + " -- end"}}, nil
		case strings.Contains(sql, "pg_user"):
			return []any{map[string]any{"usesysid": 100.0, "usename": "etl"}}, nil
		}
		return nil, nil
	}
	scan, last, err = rsCatalogScan(RedshiftCatalogSyncConfig{RedshiftConn: RedshiftConn{Database: "dev"}}, since, rs, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Tables) != 1 || scan.Tables[0].ID != "redshift:dev.public.facts" || last.IsZero() {
		t.Fatalf("%+v %v", scan.Tables, last)
	}
	if len(scan.Edges) != 2 || scan.Edges[0].From != "redshift:dev.stg.orders" || scan.Edges[0].FromCol != "order_id" || scan.Edges[0].Via != "query 10 by etl" {
		t.Fatalf("%+v", scan.Edges)
	}
}

func TestBigQueryTransferJobs(t *testing.T) {
	pages := []string{`{"nextPageToken":"p2","jobs":[
  {"jobReference":{"jobId":"load1"},"user_email":"etl@x.iam","statistics":{"creationTime":"1793000000000"},
   "configuration":{"jobType":"LOAD","load":{"sourceUris":["gs://b/exports/2026-10-27/orders.csv"],"destinationTable":{"projectId":"p","datasetId":"raw","tableId":"orders"},
     "schema":{"fields":[{"name":"id","type":"INTEGER"},{"name":"amount","type":"NUMERIC"}]}}}},
  {"jobReference":{"jobId":"q1"},"statistics":{"creationTime":"1793000001000"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT 1"}}},
  {"jobReference":{"jobId":"bad"},"errorResult":{"message":"x"},"statistics":{"creationTime":"1793000002000"},
   "configuration":{"jobType":"LOAD","load":{"sourceUris":["gs://b/x"],"destinationTable":{"projectId":"p","datasetId":"raw","tableId":"x"}}}}]}`,
		`{"jobs":[
  {"jobReference":{"jobId":"copy1"},"user_email":"ann@x.com","statistics":{"creationTime":"1792999000000"},
   "configuration":{"jobType":"COPY","copy":{"sourceTables":[{"projectId":"p","datasetId":"raw","tableId":"orders"}],"destinationTable":{"projectId":"p","datasetId":"raw","tableId":"orders_bak"}}}},
  {"jobReference":{"jobId":"ext1"},"statistics":{"creationTime":"1792998000000"},
   "configuration":{"jobType":"EXTRACT","extract":{"sourceTable":{"projectId":"p","datasetId":"raw","tableId":"orders"},"destinationUris":["gs://b/out/orders-*.json"]}}},
  {"jobReference":{"jobId":"other"},"statistics":{"creationTime":"1792997000000"},
   "configuration":{"jobType":"LOAD","load":{"sourceUris":["gs://b/y"],"destinationTable":{"projectId":"p","datasetId":"hr","tableId":"y"}}}}]}`}
	var tokens []string
	list := func(tok string) (bqJobPage, error) {
		tokens = append(tokens, tok)
		var p bqJobPage
		i := 0
		if tok == "p2" {
			i = 1
		}
		err := json.Unmarshal([]byte(pages[i]), &p)
		return p, err
	}
	cols := func(id string) []string {
		if id == "p.raw.orders" {
			return []string{"id", "amount"}
		}
		return nil
	}
	scan, last, err := bqTransferScan(BigQueryCatalogSyncConfig{Datasets: []string{"raw"}}, list, cols, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range scan.Edges {
		got = append(got, e.From+"."+e.FromCol+" -> "+e.To+"."+e.ToCol+" | "+e.Via)
	}
	all := strings.Join(got, "\n")
	for _, w := range []string{
		"gcs:gs://b/exports/2026-10-27/orders.csv.amount -> bigquery:p.raw.orders.amount | job load1 by etl@x.iam",
		"bigquery:p.raw.orders.id -> bigquery:p.raw.orders_bak.id | job copy1 by ann@x.com",
		"bigquery:p.raw.orders.amount -> gcs:gs://b/out/orders-*.json.amount | job ext1",
	} {
		if !strings.Contains(all, w) {
			t.Fatalf("missing %q in\n%s", w, all)
		}
	}
	if strings.Contains(all, "hr.y") || strings.Contains(all, "raw.x") {
		t.Fatalf("out of scope or failed jobs kept:\n%s", all)
	}
	if scan.Queries != 3 || len(tokens) != 2 || last.UnixMilli() != 1793000000000 {
		t.Fatalf("jobs %d, pages %v, last %v", scan.Queries, tokens, last)
	}
}

func TestRedshiftCopyFromHistory(t *testing.T) {
	since := time.Now().Add(-time.Hour)
	rs := func(sql string) ([]any, error) {
		switch {
		case strings.Contains(sql, "svv_columns"):
			return []any{
				map[string]any{"table_catalog": "dev", "table_schema": "raw", "table_name": "events", "column_name": "id", "data_type": "integer", "table_type": "BASE TABLE"},
				map[string]any{"table_catalog": "dev", "table_schema": "raw", "table_name": "events", "column_name": "kind", "data_type": "varchar", "table_type": "BASE TABLE"},
			}, nil
		case strings.Contains(sql, "sys_query_history"):
			return []any{map[string]any{"query_id": 5.0, "session_id": 1.0, "user_id": 100.0, "database_name": "dev", "start_time": time.Now().UTC().Format("2006-01-02 15:04:05"),
				"query_text": "COPY raw.events FROM 's3://lake/events/2026/10/27/' IAM_ROLE 'arn:aws:iam::1:role/r' FORMAT AS PARQUET", "text_len": 90.0}}, nil
		}
		return nil, nil
	}
	scan, _, err := rsCatalogScan(RedshiftCatalogSyncConfig{RedshiftConn: RedshiftConn{Database: "dev"}}, since, rs, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Edges) != 2 || scan.Edges[0].From != "s3:s3://lake/events/2026/10/27/" || scan.Edges[0].To != "redshift:dev.raw.events" || scan.Edges[0].FromCol == "" {
		t.Fatalf("%+v", scan.Edges)
	}
}
