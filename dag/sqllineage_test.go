package dag

import (
	"encoding/json"
	"strings"
	"testing"
)

func colsFixture(t string) []string {
	switch strings.ToLower(t) {
	case "p.raw.orders":
		return []string{"id", "cust_id", "amount", "ts"}
	case "p.raw.customers":
		return []string{"id", "name", "country"}
	case "public.sales":
		return []string{"sale_id", "qty", "price"}
	}
	return nil
}

func lin(t *testing.T, sql string, bq bool) []SQLLineage {
	t.Helper()
	out := ParseSQLLineage(sql, bq, colsFixture)
	if len(out) == 0 {
		t.Fatalf("no lineage for %q", sql)
	}
	return out
}

func colFrom(l SQLLineage, name string) string {
	for _, c := range l.Columns {
		if strings.EqualFold(c.Name, name) {
			var s []string
			for _, r := range c.From {
				s = append(s, r.Table+"."+r.Column)
			}
			return strings.Join(s, ",")
		}
	}
	return "<missing>"
}

func TestSQLLineageCTAS(t *testing.T) {
	l := lin(t, "CREATE OR REPLACE TABLE `p.mart.rev` PARTITION BY DATE(ts) AS\n"+
		"WITH o AS (SELECT cust_id, amount * 1.1 AS amt, ts FROM `p.raw.orders` WHERE amount > 0)\n"+
		"SELECT c.name AS customer, SUM(o.amt) total, o.ts FROM o JOIN p.raw.customers c ON c.id = o.cust_id GROUP BY 1, 3", true)[0]
	if l.Kind != "create" || l.Target != "p.mart.rev" {
		t.Fatalf("%+v", l)
	}
	if got := strings.Join(l.Sources, ","); got != "p.raw.customers,p.raw.orders" {
		t.Fatalf("sources %s", got)
	}
	for name, want := range map[string]string{"customer": "p.raw.customers.name", "total": "p.raw.orders.amount", "ts": "p.raw.orders.ts"} {
		if got := colFrom(l, name); got != want {
			t.Errorf("%s: %s want %s", name, got, want)
		}
	}
}

func TestSQLLineageStarInsertUnion(t *testing.T) {
	l := lin(t, `INSERT INTO p.mart.o2 SELECT * EXCEPT (ts) FROM p.raw.orders UNION ALL SELECT id, cust_id, amount FROM p.raw.orders_old`, true)[0]
	if l.Kind != "insert" || len(l.Columns) != 3 {
		b, _ := json.Marshal(l)
		t.Fatalf("%s", b)
	}
	if got := colFrom(l, "amount"); got != "p.raw.orders.amount,p.raw.orders_old.amount" {
		t.Errorf("amount %s", got)
	}
}

func TestSQLLineageMergeRedshift(t *testing.T) {
	sql := `MERGE INTO public.sales USING (SELECT "sale_id", qty, price::numeric(10,2) AS price FROM staging.s) s ON public.sales.sale_id = s.sale_id
WHEN MATCHED THEN UPDATE SET qty = s.qty, price = s.price * 2
WHEN NOT MATCHED THEN INSERT (sale_id, qty, price) VALUES (s.sale_id, s.qty, s.price);`
	l := lin(t, sql, false)[0]
	if l.Kind != "merge" || l.Target != "public.sales" {
		t.Fatalf("%+v", l)
	}
	for _, c := range []string{"sale_id", "qty", "price"} {
		if got := colFrom(l, c); got != "staging.s."+c {
			t.Errorf("%s: %s", c, got)
		}
	}
}

func TestSQLLineageUpdateAndMulti(t *testing.T) {
	out := lin(t, `UPDATE p.raw.customers c SET country = x.cc FROM p.ref.countries x WHERE x.name = c.country; SELECT 1;
CREATE VIEW v.x AS SELECT id, CASE WHEN amount > 10 THEN 'big' ELSE 'small' END AS size FROM p.raw.orders`, true)
	if len(out) != 3 {
		t.Fatalf("%d statements", len(out))
	}
	if got := colFrom(out[0], "country"); got != "p.ref.countries.cc" {
		t.Errorf("update: %s", got)
	}
	if got := colFrom(out[2], "size"); got != "p.raw.orders.amount" || out[2].Columns[1].Expr == "" {
		t.Errorf("view: %s %+v", got, out[2].Columns)
	}
}

func TestSQLLineageScalarSubqueryAndUnknown(t *testing.T) {
	l := lin(t, `CREATE TABLE a.b AS SELECT x.k, (SELECT MAX(amount) FROM p.raw.orders) AS mx FROM some.unknown x`, false)[0]
	if got := colFrom(l, "k"); got != "some.unknown.k" {
		t.Errorf("k %s", got)
	}
	if got := colFrom(l, "mx"); got != "p.raw.orders.amount" {
		t.Errorf("mx %s", got)
	}
}
