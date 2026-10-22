package dag

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAWS serves Athena and the Redshift Data API.
type fakeAWS struct {
	mu   sync.Mutex
	sqls []string
	auth []string
}

func (f *fakeAWS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	var in map[string]any
	_ = json.Unmarshal(body, &in)
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	out := map[string]any{}
	switch r.Header.Get("X-Amz-Target") {
	case "AmazonAthena.StartQueryExecution":
		f.sqls = append(f.sqls, in["QueryString"].(string))
		out["QueryExecutionId"] = "q1"
	case "AmazonAthena.GetQueryExecution":
		out["QueryExecution"] = map[string]any{"Status": map[string]any{"State": "SUCCEEDED"}}
	case "AmazonAthena.GetQueryResults":
		v := func(s string) map[string]any { return map[string]any{"VarCharValue": s} }
		out["ResultSet"] = map[string]any{
			"ResultSetMetadata": map[string]any{"ColumnInfo": []any{map[string]any{"Name": "id", "Type": "bigint"}, map[string]any{"Name": "name", "Type": "varchar"}}},
			"Rows":              []any{map[string]any{"Data": []any{v("id"), v("name")}}, map[string]any{"Data": []any{v("1"), v("ann")}}}}
	case "RedshiftData.ExecuteStatement":
		f.sqls = append(f.sqls, in["Sql"].(string))
		out["Id"] = "s1"
	case "RedshiftData.BatchExecuteStatement":
		for _, s := range in["Sqls"].([]any) {
			f.sqls = append(f.sqls, s.(string))
		}
		out["Id"] = "s2"
	case "RedshiftData.DescribeStatement":
		out = map[string]any{"Status": "FINISHED", "HasResultSet": true}
	case "RedshiftData.GetStatementResult":
		out = map[string]any{"ColumnMetadata": []any{map[string]any{"Name": "n"}}, "Records": []any{[]any{map[string]any{"longValue": 7}}}}
	default:
		w.WriteHeader(400)
		out = map[string]any{"__type": "UnknownOperationException", "message": r.Header.Get("X-Amz-Target")}
	}
	_ = json.NewEncoder(w).Encode(out)
}

func setupAWS(t *testing.T) *fakeAWS {
	f := &fakeAWS{}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	old := awsServiceURL
	awsServiceURL = func(service, region string) string { return srv.URL + "/" }
	t.Cleanup(func() { awsServiceURL = old })
	t.Setenv("AWS_ACCESS_KEY_ID", "AKID")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "SECRET")
	t.Setenv("AWS_REGION", "ap-southeast-3")
	return f
}

func TestAthenaAndRedshiftQuery(t *testing.T) {
	f := setupAWS(t)
	yml := `dag_id: aws
tasks:
  - id: a
    operator: athena_query
    database: stg
    output_location: s3://b/res/
    sql: SELECT * FROM t WHERE d = '{{ ds }}'
  - id: r
    operator: redshift_query
    workgroup_name: wg
    database: dev
    sql: SELECT 1
`
	dags, err := ParseYAML("a.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs)
	v, _ := rs.XComPull("a", "return_value")
	rows := v.([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["name"] != "ann" || rows[0].(map[string]any)["id"] != float64(1) {
		t.Fatalf("athena rows: %v", v)
	}
	if !strings.Contains(strings.Join(f.sqls, "\n"), "WHERE d = '2026-01-31'") {
		t.Fatalf("sqls: %v", f.sqls)
	}
	if !strings.Contains(f.auth[0], "/ap-southeast-3/athena/aws4_request") {
		t.Fatalf("auth: %s", f.auth[0])
	}
	v, _ = rs.XComPull("r", "return_value")
	if len(v.([]any)) != 1 {
		t.Fatalf("redshift rows: %v", v)
	}
}

func TestAthenaExternalDDL(t *testing.T) {
	s := extSource{uris: []string{"s3://b/x/f.ndjson"}, format: "ndjson", cols: []bqCol{{"Order ID", "INT64"}, {"amount", "FLOAT64"}, {"meta", "JSON"}}}
	ddl, cols, err := athenaExternalDDL("stg", "orders", AthenaExternalConfig{DefaultType: "auto", ColumnTypes: map[string]string{"amount": "NUMERIC"}}, s, "s3://b/x/")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"CREATE EXTERNAL TABLE `stg`.`orders` (", "`order_id` bigint", "`amount` decimal(38,9)", "`meta` string",
		"org.openx.data.jsonserde.JsonSerDe", "'mapping.order_id' = 'Order ID'", "LOCATION 's3://b/x/'"} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("missing %q in:\n%s", want, ddl)
		}
	}
	if cols[0].Name != "order_id" {
		t.Fatal(cols)
	}
	s.format, s.delimiter = "csv", ";"
	ddl, _, _ = athenaExternalDDL("stg", "orders", AthenaExternalConfig{}, s, "s3://b/x/")
	for _, want := range []string{"OpenCSVSerde", "'separatorChar' = ';'", "'skip.header.line.count' = '1'", "`amount` string"} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("csv: missing %q in:\n%s", want, ddl)
		}
	}
}

func TestAthenaLoadStatements(t *testing.T) {
	ctx := loadCtx(t)
	src := []bqCol{{"order_id", "varchar"}, {"amount", "varchar"}, {"status", "varchar"}}
	cfg := AthenaLoadConfig{Table: "dw.orders", TableLocation: "s3://wh/iceberg", PartitionBy: []string{"metadata_job_date"},
		LoadOptions: LoadOptions{Conditions: []LoadCondition{{"status", "=", "paid"}, {"amount", ">", "10"}}, Timezone: "Asia/Jakarta",
			ColumnMap: map[string]ColumnSpec{"amount": {Name: "total", Cast: "NUMERIC"}}}}
	st, err := athenaLoadStatements(ctx, cfg, "stg.ext", "dw", "orders", src, nil, true, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	q := st[0]
	for _, want := range []string{`CREATE TABLE "dw"."orders"`, "table_type = 'ICEBERG'", "location = 's3://wh/iceberg/orders/'", "partitioning = ARRAY['metadata_job_date']",
		`TRY_CAST("amount" AS decimal(38,9)) AS "total"`, `"$path" AS "metadata_path"`, `AS TIMESTAMP(6)) AS "metadata_inserted_at"`,
		`CAST(20`, `AS BIGINT) AS "metadata_job_id"`, `FROM "stg"."ext"`, `WHERE CAST("status" AS VARCHAR) = 'paid' AND TRY_CAST("amount" AS DOUBLE) > 10`} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in:\n%s", want, q)
		}
	}
	// existing table: new column + merge
	cfg.Mode, cfg.MergeKeys = "merge", []string{"order_id"}
	cfg.ColumnMap["status"] = ColumnSpec{Cast: "my_db.f({col})"}
	tgt := []bqCol{{"order_id", "varchar"}, {"total", "decimal(38,9)"}, {"metadata_path", "varchar"}, {"metadata_inserted_at", "timestamp(6)"}, {"metadata_job_id", "bigint"}}
	st, err = athenaLoadStatements(ctx, cfg, "stg.ext", "dw", "orders", src, tgt, true, "", func(string) (map[string]string, error) {
		return map[string]string{"status": "varchar"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(st) != 2 || st[0] != "ALTER TABLE `dw`.`orders` ADD COLUMNS (`status` string, `metadata_job_date` date)" {
		t.Fatalf("alter: %q", st)
	}
	for _, want := range []string{`MERGE INTO "dw"."orders" AS t`, "ROW_NUMBER() OVER (PARTITION BY \"order_id\"", `ON t."order_id" = s."order_id"`,
		`WHEN MATCHED THEN UPDATE SET "total" = s."total"`, `WHEN NOT MATCHED THEN INSERT`, `(my_db.f("status")) AS "status"`} {
		if !strings.Contains(st[1], want) {
			t.Fatalf("merge: missing %q in:\n%s", want, st[1])
		}
	}
}

func TestRedshiftLoadStatements(t *testing.T) {
	ctx := loadCtx(t)
	s := extSource{uris: []string{"s3://b/exp/2026/orders.ndjson.gz"}, format: "ndjson", gzip: true, cols: []bqCol{{"order_id", "STRING"}, {"amount", "INT64"}}}
	cfg := RedshiftLoadConfig{Table: "dw.orders", IAMRole: "arn:aws:iam::1:role/r", AllFiles: true, DistKey: "order_id", SortKey: []string{"metadata_job_date"},
		LoadOptions: LoadOptions{Metadata: []string{"path", "job_date"}, ColumnMap: map[string]ColumnSpec{"amount": {Cast: "decimal(18,2)"}}}}
	pre, batch, err := redshiftLoadStatements(ctx, cfg, s, "dw", "orders", nil, "x")
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(batch, ";\n")
	for _, want := range []string{`CREATE TEMP TABLE dagsflow_stage_x ("order_id" VARCHAR(65535), "amount" VARCHAR(65535))`,
		`COPY dagsflow_stage_x ("order_id", "amount")`, "FROM 's3://b/exp/2026/'", "IAM_ROLE 'arn:aws:iam::1:role/r'", "FORMAT AS JSON 'auto ignorecase' GZIP",
		`CAST("amount" AS decimal(18,2)) AS "amount"`, `CAST('s3://b/exp/2026/' AS VARCHAR(1024)) AS "metadata_path"`,
		`CREATE TABLE "dw"."orders" DISTKEY("order_id") SORTKEY("metadata_job_date") AS SELECT * FROM dagsflow_src_x`, "SELECT COUNT(*) AS n FROM dagsflow_src_x"} {
		if !strings.Contains(all, want) {
			t.Fatalf("missing %q in:\n%s", want, all)
		}
	}
	if len(pre) != 0 {
		t.Fatal(pre)
	}
	// existing table, merge skip, a new column
	cfg.Mode, cfg.MergeKeys, cfg.WhenMatched = "merge", []string{"order_id"}, "skip"
	pre, batch, err = redshiftLoadStatements(ctx, cfg, s, "dw", "orders", []bqCol{{"order_id", "varchar(65535)"}, {"amount", "numeric"}, {"metadata_path", "varchar(1024)"}}, "x")
	if err != nil {
		t.Fatal(err)
	}
	if len(pre) != 1 || pre[0] != `ALTER TABLE "dw"."orders" ADD COLUMN "metadata_job_date" date` {
		t.Fatalf("pre: %q", pre)
	}
	all = strings.Join(batch, ";\n")
	if !strings.Contains(all, `WHERE NOT EXISTS (SELECT 1 FROM "dw"."orders" AS t WHERE t."order_id" = s."order_id")`) {
		t.Fatalf("skip:\n%s", all)
	}
	cfg.WhenMatched = ""
	_, batch, _ = redshiftLoadStatements(ctx, cfg, s, "dw", "orders", []bqCol{{"order_id", "varchar"}}, "x")
	if !strings.Contains(strings.Join(batch, ";"), `MERGE INTO "dw"."orders" AS t`) {
		t.Fatal(batch)
	}
	// expression without ::type for a new column
	cfg.MergeKeys = []string{"oid"}
	cfg.ColumnMap["order_id"] = ColumnSpec{Name: "oid", Cast: "f({col})"}
	if _, _, err := redshiftLoadStatements(ctx, cfg, s, "dw", "orders", []bqCol{{"x", "int"}}, "x"); err == nil || !strings.Contains(err.Error(), "::type") {
		t.Fatalf("unknown type: %v", err)
	}
	cfg.ColumnMap["order_id"] = ColumnSpec{Name: "oid", Cast: "f({col})::date"}
	if pre, _, err := redshiftLoadStatements(ctx, cfg, s, "dw", "orders", []bqCol{{"x", "int"}}, "x"); err != nil || !strings.Contains(strings.Join(pre, ";"), `"oid" date`) {
		t.Fatalf("::date: %v %v", pre, err)
	}
	_ = time.Now
}

func TestAWSWarehouseYAMLChecks(t *testing.T) {
	for _, y := range []string{
		"dag_id: a\ntasks:\n  - {id: x, operator: athena_load_table, table: dw.t}\n",                                         // no table_location
		"dag_id: a\ntasks:\n  - {id: x, operator: redshift_load_table, table: dw.t, database: dev, workgroup_name: wg}\n",    // no iam_role
		"dag_id: a\ntasks:\n  - {id: x, operator: redshift_query, sql: x, database: dev}\n",                                  // no cluster / workgroup
		"dag_id: a\ntasks:\n  - {id: x, operator: athena_external_table, table: 'a b'}\n",                                    // bad name
		"dag_id: a\ntasks:\n  - {id: x, operator: athena_load_table, table: dw.t, table_location: 's3://w/', mode: merge}\n", // merge without keys
	} {
		dd, err := ParseYAML("a.yaml", []byte(y))
		if err == nil {
			err = dd[0].validate()
		}
		if err == nil {
			t.Fatalf("expected error for %s", y)
		}
	}
}
