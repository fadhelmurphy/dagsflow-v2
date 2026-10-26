package dag

import (
	"strings"
	"testing"
	"time"
)

// Real-world shapes: Dataform compiled SQL, BigQuery scripting, Redshift.
func TestSQLLineageCorpus(t *testing.T) {
	cols := tableCols(map[string][]string{
		"raw.events":   {"event_id", "user", "props", "ts", "items"},
		"raw.users":    {"id", "email", "country"},
		"p.raw.orders": {"order_id", "customer_id", "amount", "status", "updated_at"},
	})
	cases := []struct {
		name, sql string
		bq        bool
		want      []string
	}{
		{"dataform incremental", `
BEGIN
  CREATE SCHEMA IF NOT EXISTS ` + "`p.mart`" + ` OPTIONS(location="US");
EXCEPTION WHEN ERROR THEN
  IF NOT CONTAINS_SUBSTR(@@error.message, "already exists") THEN RAISE USING MESSAGE = @@error.message; END IF;
END;
BEGIN
  DECLARE dataform_table_type DEFAULT (
    SELECT ANY_VALUE(table_type) FROM ` + "`p.mart.INFORMATION_SCHEMA.TABLES`" + ` WHERE table_name = 'orders_daily'
  );
  IF dataform_table_type IS NOT NULL THEN
    CREATE OR REPLACE TEMP TABLE dataform_tmp AS (
      SELECT DATE(updated_at) AS day, customer_id, SUM(amount) AS total
      FROM ` + "`p.raw.orders`" + `
      WHERE updated_at > (SELECT MAX(day) FROM ` + "`p.mart.orders_daily`" + `)
      GROUP BY 1, 2
    );
    MERGE ` + "`p.mart.orders_daily`" + ` T
    USING dataform_tmp S
    ON T.day = S.day AND T.customer_id = S.customer_id
    WHEN MATCHED THEN UPDATE SET total = S.total
    WHEN NOT MATCHED THEN INSERT (day, customer_id, total) VALUES (day, customer_id, total);
  ELSE
    CREATE OR REPLACE TABLE ` + "`p.mart.orders_daily`" + ` PARTITION BY day CLUSTER BY customer_id OPTIONS() AS (
      SELECT DATE(updated_at) AS day, customer_id, SUM(amount) AS total FROM ` + "`p.raw.orders`" + ` GROUP BY 1, 2
    );
  END IF;
END;`, true, []string{
			"bigquery:p.raw.orders.updated_at -> bigquery:p.mart.orders_daily.day",
			"bigquery:p.raw.orders.amount -> bigquery:p.mart.orders_daily.total [SUM(amount)]",
			"bigquery:p.raw.orders.customer_id -> bigquery:p.mart.orders_daily.customer_id",
		}},
		{"execute immediate literal", `EXECUTE IMMEDIATE "INSERT INTO mart.u (id, mail) SELECT id, LOWER(email) FROM raw.users";`, true, []string{
			"bigquery:p.raw.users.email -> bigquery:p.mart.u.mail [LOWER(email)]",
		}},
		{"struct paths and unnest", `CREATE OR REPLACE TABLE mart.items AS
SELECT e.event_id, e.props.page.url AS page_url, it.sku, it.qty * 2 AS qty2
FROM raw.events AS e, UNNEST(e.items) AS it
WHERE e.props.kind = 'buy'`, true, []string{
			"bigquery:p.raw.events.event_id -> bigquery:p.mart.items.event_id",
			"bigquery:p.raw.events.props -> bigquery:p.mart.items.page_url",
			"bigquery:p.raw.events.items -> bigquery:p.mart.items.sku",
			"bigquery:p.raw.events.items -> bigquery:p.mart.items.qty2",
		}},
		{"window qualify safe functions", `INSERT INTO mart.latest (user, ts)
WITH ranked AS (SELECT user, ts, ROW_NUMBER() OVER (PARTITION BY user ORDER BY ts DESC) rn FROM raw.events)
SELECT user, SAFE.TIMESTAMP_MILLIS(SAFE_CAST(ts AS INT64)) FROM ranked WHERE rn = 1 QUALIFY TRUE`, true, []string{
			"bigquery:p.raw.events.user -> bigquery:p.mart.latest.user",
			"bigquery:p.raw.events.ts -> bigquery:p.mart.latest.ts",
		}},
		{"select as struct and if", `CREATE TABLE mart.x AS SELECT u.id, IF(u.country = 'ID', 'local', 'intl') AS segment,
(SELECT AS STRUCT u.email AS e) AS contact FROM raw.users u LEFT JOIN UNNEST([1,2]) n ON TRUE`, true, []string{
			"bigquery:p.raw.users.country -> bigquery:p.mart.x.segment",
			"bigquery:p.raw.users.email -> bigquery:p.mart.x.contact",
		}},
		{"merge not matched by source", `MERGE mart.dim_user AS t USING (SELECT id, email FROM raw.users) AS s ON t.id = s.id
WHEN MATCHED AND t.email <> s.email THEN UPDATE SET t.email = s.email
WHEN NOT MATCHED BY TARGET THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE THEN DELETE`, true, []string{
			"bigquery:p.raw.users.email -> bigquery:p.mart.dim_user.email",
			"bigquery:p.raw.users.id -> bigquery:p.mart.dim_user.id",
		}},
		{"pivot-free group by rollup and except distinct", `CREATE TABLE mart.c AS SELECT country, COUNT(DISTINCT id) AS n FROM raw.users GROUP BY ROLLUP(country)
EXCEPT DISTINCT SELECT 'x', 0`, true, []string{
			"bigquery:p.raw.users.country -> bigquery:p.mart.c.country",
			"bigquery:p.raw.users.id -> bigquery:p.mart.c.n",
		}},
		{"redshift ctas, quoted, ::, nvl, getdate", `CREATE TABLE "mart"."o" DISTSTYLE KEY DISTKEY(order_id) SORTKEY(day) AS
SELECT o.order_id, o.amount::decimal(18,2) AS amount, NVL(o.status, 'n/a') AS status, TRUNC(o.updated_at) AS day, GETDATE() AS loaded_at
FROM stg.orders o JOIN stg.cust c ON c.id = o.customer_id WHERE c.active`, false, []string{
			"redshift:dev.stg.orders.order_id -> redshift:dev.mart.o.order_id",
			"redshift:dev.stg.orders.amount -> redshift:dev.mart.o.amount",
			"redshift:dev.stg.orders.status -> redshift:dev.mart.o.status",
			"redshift:dev.stg.orders.updated_at -> redshift:dev.mart.o.day",
			"redshift:dev.stg.cust. -> redshift:dev.mart.o.",
		}},
		{"redshift select into and insert with cte", `SELECT id, email INTO TEMP t_users FROM stg.users;
INSERT INTO mart.users (id, email) WITH x AS (SELECT * FROM t_users) SELECT id, email FROM x;`, false, []string{
			"redshift:dev.stg.users.email -> redshift:dev.mart.users.email",
		}},
		{"redshift update from and delete", `UPDATE mart.users SET email = s.email FROM stg.users s WHERE mart.users.id = s.id;
DELETE FROM mart.users USING stg.gone g WHERE mart.users.id = g.id;`, false, []string{
			"redshift:dev.stg.users.email -> redshift:dev.mart.users.email",
		}},
		{"create view with column list", `CREATE OR REPLACE VIEW mart.v (uid, mail) AS SELECT id, email FROM raw.users`, true, []string{
			"bigquery:p.raw.users.id -> bigquery:p.mart.v.uid",
			"bigquery:p.raw.users.email -> bigquery:p.mart.v.mail",
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := BigQueryDialect("p")
			if !c.bq {
				d = RedshiftDialect("redshift", "dev", "public")
			}
			l := SQLLineageOf(c.sql, d, "", cols)
			got := strings.Join(edgeSet(l), "\n")
			for _, w := range c.want {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q in:\n%s", w, got)
				}
			}
		})
	}
}

// The append / merge load scripts end with SELECT @@row_count: this used to
// loop forever in the tokenizer.
func TestSQLLineageLoadScriptsAllModes(t *testing.T) {
	ctx := loadCtx(t)
	src := tableInfo{exists: true, cols: []bqCol{{"order_id", "STRING"}, {"amount", "STRING"}}}
	dst := tableInfo{exists: true, cols: []bqCol{{"order_id", "STRING"}, {"amount", "NUMERIC"}}}
	for _, mode := range []string{"append", "replace", "merge"} {
		cfg := BigQueryLoadConfig{Mode: mode, MergeKeys: []string{"order_id"}, ColumnMap: map[string]ColumnSpec{"amount": {Cast: "NUMERIC"}}}
		meta, _ := cfg.metaColumns(time.Now(), false, "")
		script, err := loadScript(ctx, cfg, "p.stg.o", "p.dw.o", src, dst, meta, nil)
		if err != nil {
			t.Fatal(err)
		}
		l := SQLLineageOf(script, BigQueryDialect("p"), "", nil)
		hasEdge(t, l, "bigquery:p.stg.o.order_id -> bigquery:p.dw.o.order_id", "bigquery:p.stg.o.amount -> bigquery:p.dw.o.amount")
	}
}

// Whatever the input, reading lineage ends quickly and never panics.
func TestSQLLineageNeverHangs(t *testing.T) {
	words := []string{"SELECT", "FROM", "(", ")", ",", "a", "b.c", "`p.d.t`", "WITH", "AS", "UNNEST", "JOIN", "ON", "=", "MERGE", "USING", "WHEN",
		"THEN", "INSERT", "UPDATE", "SET", "CREATE", "TABLE", "*", "EXCEPT", "@@x", "@p", "'s'", "\"q\"", "1", ";", "IF", "BEGIN", "END", "CASE",
		"INTO", "VALUES", "COPY", "EXECUTE", "IMMEDIATE", "--c\n", "/*", "::", "[", "]", "$1", "OVER", "PARTITION", "BY", "<", ">"}
	seed := uint32(1)
	next := func() int { seed = seed*1664525 + 1013904223; return int(seed >> 8) }
	deadline := time.Now().Add(3 * time.Second)
	for n := 0; n < 3000 && time.Now().Before(deadline); n++ {
		var b strings.Builder
		for i := next() % 60; i > 0; i-- {
			b.WriteString(words[next()%len(words)])
			b.WriteByte(' ')
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			SQLLineageOf(b.String(), BigQueryDialect("p"), "task:x", tableCols(map[string][]string{"a": {"x"}}))
			SQLLineageOf(b.String(), RedshiftDialect("redshift", "dev", "public"), "", nil)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("hangs on: %s", b.String())
		}
	}
}

func TestSQLLineageUnread(t *testing.T) {
	l := SQLLineageOf("DECLARE x INT64; CALL mart.refresh(); DROP TABLE a.b; INSERT INTO mart.t VALUES (1); CREATE SCHEMA s", BigQueryDialect("p"), "", nil)
	if len(l.Unread) != 1 || !strings.HasPrefix(l.Unread[0], "CALL mart.refresh") {
		t.Fatalf("%q", l.Unread)
	}
}
