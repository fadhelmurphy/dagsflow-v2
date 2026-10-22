package dag

import (
	"strings"
	"testing"
	"time"
)

func loadCtx(t *testing.T) *TaskContext {
	d := NewDAG("l", "", WithParams(map[string]any{"env": "prod"}))
	task := d.NewTask("x", noop)
	return NewTaskContext(t.Context(), task, RunInfo{LogicalDate: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC), RunID: "r", MapIndex: -1}, &memRun{}, nil)
}

func TestLoadScriptModes(t *testing.T) {
	ctx := loadCtx(t)
	src := tableInfo{exists: true, external: true, cols: []bqCol{{"order_id", "STRING"}, {"status", "STRING"}, {"amount", "STRING"}}}
	now := time.Date(2026, 10, 21, 12, 10, 23, 0, time.UTC)
	cfg := BigQueryLoadConfig{Timezone: "Asia/Jakarta", Conditions: []LoadCondition{{"status", "=", "paid"}, {"amount", ">", "100"}, {"order_id", "in", "a, b"}, {"status", "not_empty", ""}}}
	meta, err := cfg.metaColumns(now, true, "")
	if err != nil {
		t.Fatal(err)
	}
	// first run: the table does not exist -> CREATE ... AS SELECT
	sc, err := loadScript(ctx, cfg, "p.s.ext", "p.d.orders", src, tableInfo{}, meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"CREATE TABLE `p.d.orders` AS", "_FILE_NAME AS `metadata_path`",
		"TIMESTAMP '2026-10-21 12:10:23.000000 UTC' AS `metadata_inserted_at`", "20261021191023 AS `metadata_job_id`",
		"DATE '2026-10-21' AS `metadata_job_date`", "CAST(`status` AS STRING) = 'paid' AND SAFE_CAST(`amount` AS FLOAT64) > 100",
		"CAST(`order_id` AS STRING) IN ('a', 'b')", "TRIM(CAST(`status` AS STRING)) != ''"} {
		if !strings.Contains(sc, want) {
			t.Fatalf("missing %q in:\n%s", want, sc)
		}
	}
	// next runs: append with the new column added first
	tgt := tableInfo{exists: true, cols: []bqCol{{"order_id", "STRING"}, {"status", "STRING"}, {"metadata_path", "STRING"}}}
	sc, _ = loadScript(ctx, cfg, "p.s.ext", "p.d.orders", src, tgt, meta, nil)
	for _, want := range []string{"ALTER TABLE `p.d.orders` ADD COLUMN IF NOT EXISTS `amount` STRING;", "ADD COLUMN IF NOT EXISTS `metadata_job_id` INT64;",
		"INSERT INTO `p.d.orders` (`order_id`, `status`, `amount`, `metadata_path`", "SELECT @@row_count AS n"} {
		if !strings.Contains(sc, want) {
			t.Fatalf("append: missing %q in:\n%s", want, sc)
		}
	}
	// merge, custom names, local inserted_at, any-of conditions, partition
	cfg = BigQueryLoadConfig{Mode: "merge", MergeKeys: []string{"order_id"}, ConditionMode: "any", Conditions: cfg.Conditions[:1],
		MetadataPrefix: "metaingest_", MetadataNames: map[string]string{"inserted_at": "ingest_at"}, InsertedAt: "local", JobDate: "utc",
		Timezone: "Asia/Jakarta", Metadata: []string{"inserted_at", "job_date"}, PartitionBy: "metaingest_job_date"}
	meta, _ = cfg.metaColumns(now, true, "")
	sc, err = loadScript(ctx, cfg, "p.s.ext", "p.d.orders", src, tgt, meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"MERGE `p.d.orders` T", "QUALIFY ROW_NUMBER() OVER (PARTITION BY `order_id`) = 1",
		"ON T.`order_id` = S.`order_id`", "WHEN MATCHED THEN UPDATE SET `status` = S.`status`, `amount` = S.`amount`, `ingest_at` = S.`ingest_at`",
		"DATETIME '2026-10-21 19:10:23.000000' AS `ingest_at`", "DATE '2026-10-21' AS `metaingest_job_date`", "WHEN NOT MATCHED THEN INSERT"} {
		if !strings.Contains(sc, want) {
			t.Fatalf("merge: missing %q in:\n%s", want, sc)
		}
	}
	if strings.Contains(sc, "metaingest_path") || strings.Contains(sc, "job_id") {
		t.Fatalf("unwanted metadata:\n%s", sc)
	}
	// replace with partition on the job date
	cfg = BigQueryLoadConfig{Mode: "replace", PartitionBy: "metadata_job_date", Metadata: []string{"job_date"}}
	meta, _ = cfg.metaColumns(now, false, "gs://b/x.ndjson")
	sc, _ = loadScript(ctx, cfg, "p.s.ext", "p.d.orders", src, tgt, meta, nil)
	if !strings.Contains(sc, "CREATE OR REPLACE TABLE `p.d.orders`\nPARTITION BY `metadata_job_date` AS") {
		t.Fatalf("replace:\n%s", sc)
	}
	// errors: unknown columns
	for _, c := range []BigQueryLoadConfig{
		{Conditions: []LoadCondition{{"nope", "=", "x"}}},
		{Mode: "merge", MergeKeys: []string{"nope"}},
		{PartitionBy: "status"},
		{MetadataNames: map[string]string{"path": "status"}},
	} {
		meta, _ := c.metaColumns(now, true, "")
		if _, err := loadScript(ctx, c, "p.s.ext", "p.d.orders", src, tgt, meta, nil); err == nil {
			t.Fatalf("expected an error for %+v", c)
		}
	}
}

func TestLoadTableYAML(t *testing.T) {
	ok := "dag_id: a\ntasks:\n  - id: x\n    operator: bigquery_load_table\n    table: d.t\n    mode: merge\n    merge_keys: [id]\n    conditions:\n      - {column: status, op: '=', value: paid}\n    metadata: [path, job_id]\n    metadata_names: {path: ingest_path}\n    timezone: Asia/Jakarta\n"
	dd, err := ParseYAML("a.yaml", []byte(ok))
	if err == nil {
		err = dd[0].validate()
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, y := range []string{
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_load_table}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_load_table, table: d.t, mode: merge}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_load_table, table: d.t, mode: upsert}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_load_table, table: d.t, conditions: [{column: a, op: like}]}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_load_table, table: d.t, timezone: Mars/Base}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_load_table, table: d.t, metadata: [file]}\n",
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

func TestLoadTableRun(t *testing.T) {
	f := setupGoogle(t)
	d := NewDAG("lt", "", WithParams(map[string]any{"project_id": "prj"}))
	src := d.NewTask("ext", func(ctx *TaskContext) error {
		return ctx.XComPush("return_value", map[string]any{"table": "prj.stg.ext", "uris": []any{"gs://b/x/*"}})
	})
	load := d.NewBigQueryLoadTableTask("load", BigQueryLoadConfig{Table: "dw.orders", Conditions: []LoadCondition{{"status", "=", "{{ params.project_id }}"}}})
	src.Then(load)
	rs := &memRun{}
	runDAG(t, d, rs)
	if len(f.queries) != 3 {
		t.Fatalf("queries: %q", f.queries)
	}
	sc := f.queries[2]
	for _, want := range []string{"CREATE TABLE `prj.dw.orders` AS", "SELECT `order_id`, `status`, _FILE_NAME AS `metadata_path`", "FROM `prj.stg.ext`", "WHERE CAST(`status` AS STRING) = 'prj'"} {
		if !strings.Contains(sc, want) {
			t.Fatalf("missing %q in:\n%s", want, sc)
		}
	}
	v, _ := rs.XComPull("load", "return_value")
	if m := v.(map[string]any); m["table"] != "prj.dw.orders" || m["mode"] != "append" {
		t.Fatalf("output: %v", v)
	}
}

func TestLoadColumnMap(t *testing.T) {
	ctx := loadCtx(t)
	src := tableInfo{exists: true, cols: []bqCol{{"order_id", "STRING"}, {"amount", "STRING"}, {"tgl", "STRING"}, {"paid", "STRING"}}}
	cfg := BigQueryLoadConfig{Mode: "merge", MergeKeys: []string{"id"}, Metadata: []string{"none"}, PartitionBy: "order_date",
		ColumnMap: map[string]ColumnSpec{
			"order_id": {Name: "id"},
			"amount":   {Name: "total", Cast: "numeric"},
			"tgl":      {Name: "order_date", Cast: "my-proj.utils.to_date({col})"},
			"paid":     {Cast: "my-proj.utils.to_bool"},
		}}
	var dryQ string
	dry := func(q string) (map[string]string, error) {
		dryQ = q
		return map[string]string{"id": "STRING", "total": "NUMERIC", "order_date": "DATE", "paid": "BOOL"}, nil
	}
	tgt := tableInfo{exists: true, cols: []bqCol{{"id", "STRING"}, {"total", "NUMERIC"}}}
	sc, err := loadScript(ctx, cfg, "p.s.ext", "p.d.orders", src, tgt, nil, dry)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"`order_id` AS `id`", "SAFE_CAST(`amount` AS NUMERIC) AS `total`",
		"(my-proj.utils.to_date(`tgl`)) AS `order_date`", "my-proj.utils.to_bool(`paid`) AS `paid`",
		"ADD COLUMN IF NOT EXISTS `order_date` DATE;", "ADD COLUMN IF NOT EXISTS `paid` BOOL;",
		"PARTITION BY `id`) = 1", "ON T.`id` = S.`id`"} {
		if !strings.Contains(sc, want) {
			t.Fatalf("missing %q in:\n%s", want, sc)
		}
	}
	if !strings.Contains(dryQ, "SELECT `order_id` AS `id`") {
		t.Fatalf("dry run query: %s", dryQ)
	}
	cfg.CastMode = "strict"
	sc, _ = loadScript(ctx, cfg, "p.s.ext", "p.d.orders", src, tgt, nil, dry)
	if !strings.Contains(sc, "CAST(`amount` AS NUMERIC)") || strings.Contains(sc, "SAFE_CAST") {
		t.Fatalf("strict:\n%s", sc)
	}
	// without types from a dry run, a new column converted by an expression can't be added
	cfg.CastMode = ""
	if _, err := loadScript(ctx, cfg, "p.s.ext", "p.d.orders", src, tgt, nil, nil); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown type: %v", err)
	}
	for _, bad := range []map[string]ColumnSpec{
		{"nope": {Name: "x"}},
		{"amount": {Name: "order_id"}},
	} {
		c := BigQueryLoadConfig{Metadata: []string{"none"}, ColumnMap: bad}
		if _, err := loadScript(ctx, c, "p.s.ext", "p.d.orders", src, tableInfo{}, nil, dry); err == nil {
			t.Fatalf("expected an error for %v", bad)
		}
	}
}
