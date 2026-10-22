package dag

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBigQueryExternalTableFromGCS(t *testing.T) {
	f := setupGoogle(t)
	d := NewDAG("ext", "", WithParams(map[string]any{"project_id": "prj"}))
	src := d.NewTask("src", func(ctx *TaskContext) error {
		return ctx.XComPush("return_value", []any{
			map[string]any{"sub_channel": "web", "phone number": 62812, "amount": 1.5, "ok": true, "meta": map[string]any{"a": 1}},
			map[string]any{"sub_channel": "app", "phone number": 62813, "amount": 2.0, "ok": false, "meta": nil},
		})
	})
	dump := d.NewGCSWriteTask("dump", StoreWriteConfig{Bucket: "bkt", Object: "export/testing/{{ .ds }}", Format: "ndjson"})
	ext := d.NewBigQueryExternalTableTask("ext", BigQueryExternalConfig{Table: "7r_staging.testing_fadhel", DefaultType: "auto",
		ColumnTypes: map[string]string{"phone number": "string"}, AllFiles: true, Expiration: "7d"})
	src.Then(dump).Then(ext)
	rs := &memRun{}
	runDAG(t, d, rs)
	if len(f.queries) != 1 {
		t.Fatalf("queries: %v", f.queries)
	}
	q := f.queries[0]
	for _, want := range []string{
		"CREATE OR REPLACE EXTERNAL TABLE `prj.7r_staging.testing_fadhel` (",
		"`amount` FLOAT64,", "`meta` JSON,", "`ok` BOOL,", "`phone number` STRING,", "`sub_channel` STRING\n",
		"format = 'NEWLINE_DELIMITED_JSON'", "uris = ['gs://bkt/export/testing/*']",
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in:\n%s", want, q)
		}
	}
	if !strings.Contains(q, "expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10080 MINUTE)") {
		t.Fatalf("expiration:\n%s", q)
	}
	v, _ := rs.XComPull("ext", "return_value")
	if m := v.(map[string]any); m["table"] != "prj.7r_staging.testing_fadhel" {
		t.Fatalf("output: %v", v)
	}
}

func TestExternalDDLCSVAndErrors(t *testing.T) {
	s := extSource{uris: []string{"gs://b/x.csv.gz"}, format: "csv", delimiter: ";", gzip: true,
		cols: []bqCol{{"User ID", "INT64"}, {"name", "STRING"}}}
	ddl, _, err := externalDDL("p.d.t", BigQueryExternalConfig{IfNotExists: true}, s)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"CREATE EXTERNAL TABLE IF NOT EXISTS `p.d.t`", "`User ID` STRING,", "format = 'CSV'",
		"skip_leading_rows = 1", "field_delimiter = ';'", "compression = 'GZIP'"} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("missing %q in:\n%s", want, ddl)
		}
	}
	if _, _, err := externalDDL("p.d.t", BigQueryExternalConfig{ColumnTypes: map[string]string{"nope": "INT64"}}, s); err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("unknown column: %v", err)
	}
	for _, y := range []string{
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_external_table}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_external_table, table: d.t, default_type: INT64}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_external_table, table: d.t, column_types: {a: NUMBER}}\n",
	} {
		dd, err := ParseYAML("a.yaml", []byte(y))
		if err == nil {
			err = dd[0].validate()
		}
		if err == nil {
			t.Fatalf("expected error for %s", y)
		}
	}
	if _, err := ParseYAML("a.yaml", []byte("dag_id: a\ntasks:\n  - {id: x, operator: bigquery_external_table, table: p.d.t, uris: ['gs://b/*'], format: csv, columns: [a, b]}\n")); err != nil {
		t.Fatal(err)
	}
}

func TestReadRename(t *testing.T) {
	UploadsDir = t.TempDir()
	t.Cleanup(func() { UploadsDir = "" })
	_ = os.WriteFile(filepath.Join(UploadsDir, "u.csv"), []byte("User ID,Full Name\n1,ana\n"), 0o600)
	dags, err := ParseYAML("r.yaml", []byte("dag_id: r\ntasks:\n  - id: src\n    operator: file_read\n    file: u.csv\n    rename: {user_id: id, full_name: 'Nama Lengkap'}\n"))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs)
	v, _ := rs.XComPull("src", "return_value")
	row := v.([]any)[0].(map[string]any)
	if row["id"] != "1" || row["Nama Lengkap"] != "ana" || len(row) != 2 {
		t.Fatalf("row: %v", row)
	}
	cols, _ := rs.XComPull("src", "columns")
	if fmt.Sprint(cols) != "[full_name user_id]" {
		t.Fatalf("columns: %v", cols)
	}
	// two columns ending with the same name
	dags, _ = ParseYAML("r.yaml", []byte("dag_id: r\ntasks:\n  - id: src\n    operator: file_read\n    file: u.csv\n    rename: {user_id: full_name}\n"))
	task, _ := dags[0].Task("src")
	ctx := NewTaskContext(t.Context(), task, RunInfo{RunID: "r", MapIndex: -1}, &memRun{}, io.Discard)
	if _, err := task.Run(ctx); err == nil || !strings.Contains(err.Error(), "both be named") {
		t.Fatalf("collision: %v", err)
	}
	dd, err := ParseYAML("r.yaml", []byte("dag_id: r\ntasks:\n  - {id: s, operator: file_read, file: u.csv, rename: {a: x, b: x}}\n"))
	if err == nil {
		err = dd[0].validate()
	}
	if err == nil {
		t.Fatal("duplicate target should fail")
	}
}
