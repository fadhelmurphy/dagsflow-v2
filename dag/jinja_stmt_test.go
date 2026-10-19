package dag

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func renderT(t *testing.T, tmpl string, params map[string]any, conf map[string]any) (string, error) {
	t.Helper()
	d := NewDAG("t_jinja", "", WithParams(params))
	task := d.NewTask("x", noop)
	logical := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	rs := &memRun{}
	_ = rs.XComPush("extract", -1, "return_value", []any{map[string]any{"id": 1.0, "n": "a"}, map[string]any{"id": 2.0, "n": "b"}})
	_ = rs.XComPush("extract", -1, "count", 2.0)
	ctx := NewTaskContext(t.Context(), task, RunInfo{LogicalDate: logical, RunID: "r1", MapIndex: -1, Conf: conf}, rs, nil)
	return ctx.Render(tmpl, nil)
}

func TestJinjaStatementsAndFilters(t *testing.T) {
	params := map[string]any{"project_id": "proj", "env": "prod", "tables": []any{"orders", "items"},
		"limit": 10.0, "cols": map[string]any{"a": "INT", "b": "STRING"}, "flag": false, "name": " Mixed Case "}
	cases := []struct{ in, want string }{
		{"{% if params.env == 'prod' %}P{% else %}D{% endif %}", "P"},
		{"{% if params.env != 'prod' %}x{% elif params.limit > 5 %}big{% else %}small{% endif %}", "big"},
		{"{% if params.missing %}yes{% else %}no{% endif %}", "no"}, // undefined is falsy
		{"{% if params.missing is defined %}d{% else %}u{% endif %}", "u"},
		{"{% if not params.flag and 'orders' in params.tables %}ok{% endif %}", "ok"},
		{"{% for t in params.tables %}{{ loop.index }}:{{ t | upper }}{% if not loop.last %},{% endif %}{% endfor %}", "1:ORDERS,2:ITEMS"},
		{"{% for t in params.tables %}{{ params.project_id }}.{{ t }} {% endfor %}", "proj.orders proj.items "},
		{"{% for k, v in params.cols.items() %}{{ k }} {{ v }};{% endfor %}", "a INT;b STRING;"},
		{"{% for x in params.none %}x{% else %}empty{% endfor %}", "empty"},
		{"{% set p = params.project_id ~ '_' ~ ds_nodash %}{{ p }}", "proj_20260928"},
		{"{{ params.name | trim | lower }}", "mixed case"},
		{"{{ params.missing | default('dflt') }}", "dflt"},
		{"{{ params.tables | join(', ') }}", "orders, items"},
		{"{{ params.tables | length }}", "2"},
		{"{{ params.limit | int * 2 }}", "20"},
		{"{{ params.limit + 5 }}", "15"},
		{"{{ 'a-b' | replace('-', '_') }}", "a_b"},
		{"{{ ti.xcom_pull(task_ids='extract') | length }}", "2"},
		{"{{ ti.xcom_pull(task_ids='extract', key='count') | int }}", "2"},
		{"{{ (ti.xcom_pull(task_ids='extract') | map(attribute='n') | join('|')) }}", "a|b"},
		{"{{ macros.ds_add(ds, -7) }}", "2026-09-21"},
		{"{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}", "202609"},
		{"{{ 'yes' if params.env == 'prod' else 'no' }}", "yes"},
		{"{{ params.get('missing', 'fb') }}", "fb"},
		{"{{ dag_run.conf.get('table', 'default_t') }}", "default_t"},
		{"{# a comment #}x{{ ds }}", "x2026-09-28"},
		{"{% raw %}{{ not rendered }}{% endraw %}", "{{ not rendered }}"},
		{"a {%- if true %} b {% endif -%} c", "a b c"},
		{"{{ params.tables | first }}/{{ params.tables | last }}", "orders/items"},
		{"{{ params.cols | tojson }}", `{"a":"INT","b":"STRING"}`},
		{"{{ .ds }} {{ ds }} {{ xcom_pull \"extract\" \"count\" }}", "2026-09-28 2026-09-28 2"}, // Go style still works
		{"{{ range $i, $t := .params.tables }}{{ $t }}{{ end }}", "ordersitems"},
		{"{% for i in range(3) %}{{ i }}{% endfor %}", "012"},
		{"{{ params.tables | sort(reverse=true) | join(',') }}", "orders,items"},
		{"{{ \"%s-%s\" | format(params.env, ds_nodash) }}", "prod-20260928"},
	}
	for _, c := range cases {
		got, err := renderT(t, c.in, params, map[string]any{})
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

func TestJinjaErrors(t *testing.T) {
	for in, want := range map[string]string{
		"{% if true %}x":                "not closed",
		"{% endif %}":                   "without",
		"{% macro m() %}{% endmacro %}": "not supported",
		"{{ x | nosuchfilter }}":        "not supported",
		"{% for x in %}{% endfor %}":    "for",
		"{{ params.missing }}":          "missing", // plain access to a missing key still fails loudly
	} {
		_, err := renderT(t, in, map[string]any{}, map[string]any{})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v, want it to mention %q", in, err, want)
		}
	}
}

func TestJinjaInclude(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "where.sql"), []byte("WHERE d = '{{ ds }}'{% if params.env == 'prod' %} AND live{% endif %}"), 0o644); err != nil {
		t.Fatal(err)
	}
	d := NewDAG("t_inc", "", WithParams(map[string]any{"env": "prod"}))
	d.File = filepath.Join(dir, "dag.yaml")
	task := d.NewTask("x", noop)
	ctx := NewTaskContext(t.Context(), task, RunInfo{LogicalDate: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), MapIndex: -1}, &memRun{}, nil)
	got, err := ctx.Render("SELECT * FROM t {% include 'where.sql' %}{% include 'nope.sql' ignore missing %}", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "SELECT * FROM t WHERE d = '2026-01-02' AND live" {
		t.Fatalf("got %q", got)
	}
	if _, err := ctx.Render("{% include 'nope.sql' %}", nil); err == nil {
		t.Fatal("missing include should fail")
	}
}

func TestJinjaREADMEExample(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "common"), 0o755)
	_ = os.WriteFile(filepath.Join(dir, "common", "where_active.sql"), []byte("AND active"), 0o644)
	d := NewDAG("t_readme", "", WithParams(map[string]any{"project_id": "p", "table": "orders", "columns": []any{"ID", "Total"}, "env": "prod"}))
	d.File = filepath.Join(dir, "dag.yaml")
	task := d.NewTask("x", noop)
	ctx := NewTaskContext(t.Context(), task, RunInfo{LogicalDate: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), MapIndex: -1}, &memRun{}, nil)
	got, err := ctx.Render(readmeSQL, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"INSERT INTO `" + "p.dw.orders" + "`", "SELECT id, total", "BETWEEN '2026-01-01' AND '2026-01-08'", "AND status = 'paid'", "AND active"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "LIMIT") || strings.Contains(got, "comments") {
		t.Fatalf("unexpected output:\n%s", got)
	}
}

// readmeSQL is the example in the README (keep them in sync).
const readmeSQL = `{# comments are dropped #}
{% set target = params.project_id ~ '.dw.' ~ params.table %}
INSERT INTO ` + "`" + `{{ target }}` + "`" + `
SELECT {% for c in params.columns %}{{ c | lower }}{% if not loop.last %}, {% endif %}{% endfor %}
FROM ` + "`" + `{{ params.project_id }}.raw.orders` + "`" + `
WHERE d BETWEEN '{{ macros.ds_add(ds, -7) }}' AND '{{ ds }}'
{% if params.env == 'prod' and dag_run.conf.get('full', false) is not true %}
  AND status = 'paid'
{% elif params.limit | default(0) | int > 0 %}
LIMIT {{ params.limit | int }}
{% endif %}
{% include 'common/where_active.sql' %}
`
