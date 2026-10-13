package dag

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// memRun is a minimal RunState: XComs in memory.
type memRun struct {
	mu sync.Mutex
	x  map[string]map[string]any
}

func (m *memRun) XComPush(task string, _ int, key string, v any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.x == nil {
		m.x = map[string]map[string]any{}
	}
	if m.x[task] == nil {
		m.x[task] = map[string]any{}
	}
	b, _ := json.Marshal(v) // like the real store: values travel as JSON
	var back any
	_ = json.Unmarshal(b, &back)
	m.x[task][key] = back
	return nil
}
func (m *memRun) XComPull(task, key string) (any, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.x[task][key]
	return v, ok
}
func (m *memRun) TriggerDagRun(context.Context, string, map[string]any, bool) (string, error) {
	return "", nil
}
func (m *memRun) ExternalTaskState(string, string, time.Time) (State, bool, error) {
	return "", false, nil
}
func (m *memRun) SetRendered(string, int, string, string) error            { return nil }
func (m *memRun) SetOutletExtra(string, int, string, map[string]any) error { return nil }

// runDAG executes every task in order.
func runDAG(t *testing.T, d *DAG, rs *memRun) {
	t.Helper()
	if err := d.validate(); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
	for _, task := range d.Tasks() {
		ctx := NewTaskContext(context.Background(), task, RunInfo{RunID: "r", LogicalDate: now, DataIntervalStart: now, DataIntervalEnd: now, MapIndex: -1}, rs, io.Discard)
		if _, err := task.Run(ctx); err != nil {
			t.Fatalf("task %s: %v", task.ID, err)
		}
	}
}

func TestHTTPTransformChain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":1,"st":"paid"},{"id":2,"st":"open"}],"day":"` + r.URL.Query().Get("d") + `"}`))
	}))
	defer srv.Close()
	yml := "dag_id: chain\ntasks:\n" +
		"  - id: fetch\n    operator: http\n    url: " + srv.URL + "?d={{ .ds }}\n" +
		"  - id: shape\n    operator: transform\n    upstream: [fetch]\n    steps:\n" +
		"      - {op: extract, path: data}\n      - {op: filter, field: st, cmp: '==', value: paid}\n" +
		"      - {op: set, field: day, value: '{{ .ds }}'}\n"
	dags, err := ParseYAML("c.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs)
	v, _ := rs.XComPull("shape", "return_value")
	rows, _ := v.([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["day"] != "2026-01-31" || rows[0].(map[string]any)["id"] != float64(1) {
		t.Fatalf("rows: %v", v)
	}
}

func TestTransformInputErrors(t *testing.T) {
	d := NewDAG("tx_err", "")
	a := d.NewTask("a", func(c *TaskContext) error { return c.XComPush("return_value", "not json") })
	d.NewTransformTask("t", TransformConfig{Steps: []TransformStep{{Op: "limit", N: 1}}}).After(a)
	d.NewTransformTask("lonely", TransformConfig{Steps: []TransformStep{{Op: "limit", N: 1}}})
	if err := d.validate(); err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	now := time.Now()
	run := func(id string) error {
		task, _ := d.Task(id)
		ctx := NewTaskContext(context.Background(), task, RunInfo{RunID: "r", LogicalDate: now, MapIndex: -1}, rs, io.Discard)
		_, err := task.Run(ctx)
		return err
	}
	_ = run("a")
	if err := run("t"); err == nil || !strings.Contains(err.Error(), "valid JSON") {
		t.Fatalf("want JSON error, got %v", err)
	}
	if err := run("lonely"); err == nil || !strings.Contains(err.Error(), "no input") {
		t.Fatalf("want no-input error, got %v", err)
	}
}

func TestPostgresDSN(t *testing.T) {
	cases := []struct {
		c    Connection
		want string
	}{
		{Connection{Config: map[string]any{"uri": "postgres://u@h/db"}}, "postgres://u@h/db"},
		{Connection{Config: map[string]any{"host": "h", "login": "me", "password": "p@ss/w", "database": "d"}}, "postgres://me:p%40ss%2Fw@h:5432/d"},
		{Connection{Config: map[string]any{"host": "h", "port": float64(6543), "sslmode": "require"}}, "postgres://h:6543/postgres?sslmode=require"},
	}
	for _, c := range cases {
		if got, err := PostgresDSN(c.c); err != nil || got != c.want {
			t.Errorf("got %q (%v), want %q", got, err, c.want)
		}
	}
	if _, err := PostgresDSN(Connection{Config: map[string]any{}}); err == nil {
		t.Error("empty connection accepted")
	}
}

func TestPostgresBuildErrors(t *testing.T) {
	d := NewDAG("pg_err", "")
	d.NewPostgresTask("a", PostgresConfig{})
	d.NewPostgresTask("b", PostgresConfig{ConnID: "c", Table: "a.b.c"})
	d.NewPostgresTask("c", PostgresConfig{ConnID: "c", Table: "t", SQL: "select 1"})
	d.NewPostgresTask("d", PostgresConfig{ConnID: "c", Table: "t", OnConflict: "replace"})
	err := d.validate()
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, w := range []string{"needs conn_id", "schema.table", "either sql or table", "on_conflict"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("missing %q in %v", w, err)
		}
	}
}

// Needs a database the test may write to, e.g.
// DAGSFLOW_TEST_PG_DSN=postgres://postgres@127.0.0.1:5433/btl
func TestPostgresQueryAndInsert(t *testing.T) {
	dsn := os.Getenv("DAGSFLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("DAGSFLOW_TEST_PG_DSN not set")
	}
	ctx := context.Background()
	pg, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close(ctx)
	_, _ = pg.Exec(ctx, `DROP TABLE IF EXISTS etl_test_src; DROP TABLE IF EXISTS etl_test_dst`)
	if _, err := pg.Exec(ctx, `CREATE TABLE etl_test_src(id int, name text, amount numeric, meta jsonb, at timestamptz, uid uuid);
		CREATE TABLE etl_test_dst(id int PRIMARY KEY, name text, amount numeric, meta jsonb);
		INSERT INTO etl_test_src VALUES (1,'a',1.5,'{"k":[1,2]}','2026-01-31T10:00:00Z','11111111-2222-3333-4444-555555555555'),(2,'b',2,NULL,NULL,NULL),(3,'c',3,'{}',NULL,NULL)`); err != nil {
		t.Fatal(err)
	}
	defer pg.Exec(ctx, `DROP TABLE etl_test_src; DROP TABLE etl_test_dst`)
	SetSecretsBackends(EnvBackend{})
	conn, _ := json.Marshal(Connection{Type: "postgres", Config: map[string]any{"uri": dsn}})
	t.Setenv("DAGSFLOW_CONN_ETLTEST", string(conn))

	yml := `dag_id: pgflow
tasks:
  - id: read
    operator: postgres
    conn_id: etltest
    sql: "SELECT * FROM etl_test_src WHERE id <= {{ .params.max }} ORDER BY id"
    limit: 2
  - id: keep
    operator: transform
    upstream: [read]
    steps:
      - {op: select, fields: [id, name, amount, meta]}
  - id: write
    operator: postgres
    conn_id: etltest
    table: etl_test_dst
    on_conflict: do_nothing
    upstream: [keep]
  - id: again
    operator: postgres
    conn_id: etltest
    table: etl_test_dst
    on_conflict: do_nothing
    rows_from: keep
    upstream: [write]
  - id: count
    operator: postgres
    conn_id: etltest
    sql: "SELECT count(*)::int AS n, sum(amount) AS s FROM etl_test_dst"
    upstream: [again]
params:
  max: 3
`
	dags, err := ParseYAML("pg.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs)
	read, _ := rs.XComPull("read", "return_value")
	rows := read.([]any)
	if len(rows) != 2 { // limit
		t.Fatalf("limit ignored: %v", rows)
	}
	first := rows[0].(map[string]any)
	if first["uid"] != "11111111-2222-3333-4444-555555555555" || first["id"] != float64(1) || first["amount"] != "1.5" {
		t.Fatalf("types: %v", first)
	}
	cnt, _ := rs.XComPull("count", "return_value")
	row := cnt.([]any)[0].(map[string]any)
	if row["n"] != float64(2) || row["s"] != "3.5" {
		t.Fatalf("after insert + duplicate insert: %v", row)
	}
	var meta string
	_ = pg.QueryRow(ctx, `SELECT meta::text FROM etl_test_dst WHERE id=1`).Scan(&meta)
	if !strings.Contains(meta, `"k"`) {
		t.Fatalf("json column: %q", meta)
	}
	// without on_conflict a duplicate fails and rolls back
	dags2, _ := ParseYAML("pg2.yaml", []byte(strings.ReplaceAll(strings.Replace(yml, "    on_conflict: do_nothing\n    upstream: [keep]", "    upstream: [keep]", 1), "pgflow", "pgflow2")))
	d2 := dags2[0]
	if err := d2.validate(); err != nil {
		t.Fatal(err)
	}
	rs2 := &memRun{}
	now := time.Now()
	for _, id := range []string{"read", "keep", "write"} {
		task, _ := d2.Task(id)
		c := NewTaskContext(context.Background(), task, RunInfo{RunID: "r", LogicalDate: now, MapIndex: -1}, rs2, io.Discard)
		_, err := task.Run(c)
		if id == "write" && (err == nil || !strings.Contains(err.Error(), "duplicate")) {
			t.Fatalf("expected duplicate key error, got %v", err)
		}
	}
	// multi-statement SQL and DDL work, rows_affected is reported
	d3 := NewDAG("pg3", "")
	d3.NewPostgresTask("ddl", PostgresConfig{ConnID: "etltest", SQL: "UPDATE etl_test_dst SET name = upper(name); UPDATE etl_test_dst SET amount = amount + 1"})
	rs3 := &memRun{}
	runDAG(t, d3, rs3)
	var name string
	_ = pg.QueryRow(ctx, `SELECT name FROM etl_test_dst WHERE id=1`).Scan(&name)
	if name != "A" {
		t.Fatalf("multi statement: %q", name)
	}
}
