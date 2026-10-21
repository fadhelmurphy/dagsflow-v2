package dag

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ETL nodes for the visual builder (and YAML): "postgres" reads/writes a
// Postgres table, "transform" reshapes the rows another task produced. Rows
// travel between tasks as XCom "return_value": a JSON array of objects.

const (
	defaultRowLimit = math.MaxInt // no cap unless the node sets limit
	insertChunk     = 500
)

// ---- shared helpers ----------------------------------------------------------

// upstreamInput resolves where a node reads its rows from: the task named in
// `from`, otherwise its only upstream task.
func upstreamInput(ctx *TaskContext, from string) (string, error) {
	if from != "" {
		return from, nil
	}
	ups := ctx.Task.Upstream()
	switch len(ups) {
	case 1:
		return ups[0].ID, nil
	case 0:
		return "", errors.New("no input: connect this node to the node that produces the data (or set the input task)")
	default:
		return "", errors.New("more than one upstream node: choose which one is the input")
	}
}

// asRows turns an XCom value (JSON text, array or single object) into rows.
func asRows(v any) ([]any, error) {
	if s, ok := v.(string); ok {
		var parsed any
		if err := json.Unmarshal([]byte(s), &parsed); err != nil {
			return nil, fmt.Errorf("input is not valid JSON (%v)", err)
		}
		v = parsed
	}
	switch x := v.(type) {
	case nil:
		return nil, errors.New("input is empty: the upstream node produced no output")
	case []any:
		return x, nil
	case []map[string]any:
		out := make([]any, len(x))
		for i, r := range x {
			out[i] = r
		}
		return out, nil
	case map[string]any:
		return []any{x}, nil
	default:
		return nil, fmt.Errorf("input must be a JSON array or object, got %T", v)
	}
}

func dig(v any, path string) (any, error) {
	for _, p := range strings.Split(path, ".") {
		if p == "" {
			continue
		}
		switch x := v.(type) {
		case map[string]any:
			nv, ok := x[p]
			if !ok {
				return nil, fmt.Errorf("path %q: no field %q", path, p)
			}
			v = nv
		case []any:
			i, err := strconv.Atoi(p)
			if err != nil || i < 0 || i >= len(x) {
				return nil, fmt.Errorf("path %q: bad index %q", path, p)
			}
			v = x[i]
		default:
			return nil, fmt.Errorf("path %q: cannot go into %T at %q", path, v, p)
		}
	}
	return v, nil
}

// ---- transform -----------------------------------------------------------------

// TransformStep is one no-code step. Op: extract, select, drop, rename, filter,
// set, sort, limit, dedupe.
type TransformStep struct {
	Op     string            `yaml:"op" json:"op"`
	Path   string            `yaml:"path,omitempty" json:"path,omitempty"`     // extract: "data.items"
	Fields []string          `yaml:"fields,omitempty" json:"fields,omitempty"` // select / drop / dedupe
	Map    map[string]string `yaml:"map,omitempty" json:"map,omitempty"`       // rename: old -> new
	Field  string            `yaml:"field,omitempty" json:"field,omitempty"`   // filter / set / sort
	Cmp    string            `yaml:"cmp,omitempty" json:"cmp,omitempty"`       // == != > >= < <= contains exists not_exists
	Value  any               `yaml:"value,omitempty" json:"value,omitempty"`   // filter / set (strings are templated)
	N      int               `yaml:"n,omitempty" json:"n,omitempty"`           // limit
	Desc   bool              `yaml:"desc,omitempty" json:"desc,omitempty"`     // sort
}

var stepOps = map[string]bool{"extract": true, "select": true, "drop": true, "rename": true, "filter": true,
	"set": true, "sort": true, "limit": true, "dedupe": true, "snake_case": true}

var cmpOps = map[string]bool{"==": true, "!=": true, ">": true, ">=": true, "<": true, "<=": true,
	"contains": true, "exists": true, "not_exists": true}

// Validate checks a step without running it.
func (s TransformStep) Validate() error {
	if !stepOps[s.Op] {
		return fmt.Errorf("unknown step %q (extract, select, drop, rename, filter, set, sort, limit, dedupe, snake_case)", s.Op)
	}
	need := func(ok bool, what string) error {
		if !ok {
			return fmt.Errorf("step %q needs %s", s.Op, what)
		}
		return nil
	}
	switch s.Op {
	case "extract":
		return need(s.Path != "", "a path, e.g. data.items")
	case "select", "drop":
		return need(len(s.Fields) > 0, "at least one field")
	case "rename":
		if err := need(len(s.Map) > 0, "at least one old → new name"); err != nil {
			return err
		}
		for from, to := range s.Map {
			if strings.TrimSpace(from) == "" || strings.TrimSpace(to) == "" {
				return errors.New("step \"rename\": every old and new name must be filled in")
			}
		}
	case "filter":
		if err := need(s.Field != "", "a field"); err != nil {
			return err
		}
		if !cmpOps[s.Cmp] {
			return fmt.Errorf("step filter: unknown comparison %q", s.Cmp)
		}
	case "set":
		return need(s.Field != "", "a field")
	case "sort":
		return need(s.Field != "", "a field")
	case "limit":
		return need(s.N > 0, "n > 0")
	}
	return nil
}

func num(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	}
	return 0, false
}

func compare(a any, cmp string, b any) bool {
	switch cmp {
	case "exists":
		return a != nil
	case "not_exists":
		return a == nil
	case "contains":
		return strings.Contains(fmt.Sprint(a), fmt.Sprint(b))
	}
	if a == nil {
		return cmp == "!=" && b != nil
	}
	if fa, ok := num(a); ok {
		if fb, ok := num(b); ok {
			switch cmp {
			case "==":
				return fa == fb
			case "!=":
				return fa != fb
			case ">":
				return fa > fb
			case ">=":
				return fa >= fb
			case "<":
				return fa < fb
			case "<=":
				return fa <= fb
			}
		}
	}
	sa, sb := fmt.Sprint(a), fmt.Sprint(b)
	switch cmp {
	case "==":
		return sa == sb
	case "!=":
		return sa != sb
	case ">":
		return sa > sb
	case ">=":
		return sa >= sb
	case "<":
		return sa < sb
	case "<=":
		return sa <= sb
	}
	return false
}

// ApplyTransform runs the steps over the input. render expands "{{ }}" in
// string values (row = the current row); it may be nil in tests.
func ApplyTransform(input any, steps []TransformStep, render func(raw string, row map[string]any) (string, error)) ([]any, error) {
	var cur any = input
	asList := func() ([]any, error) { return asRows(cur) }
	for i, s := range steps {
		if err := s.Validate(); err != nil {
			return nil, fmt.Errorf("step %d: %w", i+1, err)
		}
		if s.Op == "extract" {
			if str, ok := cur.(string); ok {
				var p any
				if err := json.Unmarshal([]byte(str), &p); err != nil {
					return nil, fmt.Errorf("step %d: input is not valid JSON (%v)", i+1, err)
				}
				cur = p
			}
			v, err := dig(cur, s.Path)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i+1, err)
			}
			cur = v
			continue
		}
		rows, err := asList()
		if err != nil {
			return nil, fmt.Errorf("step %d (%s): %w", i+1, s.Op, err)
		}
		out := make([]any, 0, len(rows))
		switch s.Op {
		case "limit":
			if s.N < len(rows) {
				rows = rows[:s.N]
			}
			out = rows
		case "sort":
			out = append(out, rows...)
			sort.SliceStable(out, func(a, b int) bool {
				ma, _ := out[a].(map[string]any)
				mb, _ := out[b].(map[string]any)
				if s.Desc {
					return compare(ma[s.Field], ">", mb[s.Field])
				}
				return compare(ma[s.Field], "<", mb[s.Field])
			})
		case "snake_case":
			for _, r := range rows {
				m, ok := r.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("step %d (snake_case): rows must be objects, got %T", i+1, r)
				}
				out = append(out, snakeKeys(m))
			}
		case "dedupe":
			seen := map[string]bool{}
			for _, r := range rows {
				m, _ := r.(map[string]any)
				var key string
				if len(s.Fields) == 0 {
					b, _ := json.Marshal(m)
					key = string(b)
				} else {
					parts := make([]string, len(s.Fields))
					for j, f := range s.Fields {
						parts[j] = fmt.Sprint(m[f])
					}
					key = strings.Join(parts, "\x00")
				}
				if !seen[key] {
					seen[key] = true
					out = append(out, r)
				}
			}
		default:
			for _, r := range rows {
				m, ok := r.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("step %d (%s): rows must be objects, got %T", i+1, s.Op, r)
				}
				switch s.Op {
				case "select":
					n := map[string]any{}
					for _, f := range s.Fields {
						if v, ok := m[f]; ok {
							n[f] = v
						}
					}
					out = append(out, n)
				case "drop":
					n := map[string]any{}
					for k, v := range m {
						n[k] = v
					}
					for _, f := range s.Fields {
						delete(n, f)
					}
					out = append(out, n)
				case "rename":
					n := map[string]any{}
					for k, v := range m {
						if to, ok := s.Map[k]; ok {
							k = to
						}
						n[k] = v
					}
					out = append(out, n)
				case "filter":
					want := s.Value
					if str, ok := want.(string); ok && render != nil && strings.Contains(str, "{{") {
						rv, err := render(str, m)
						if err != nil {
							return nil, fmt.Errorf("step %d: %w", i+1, err)
						}
						want = rv
					}
					if compare(m[s.Field], s.Cmp, want) {
						out = append(out, m)
					}
				case "set":
					n := map[string]any{}
					for k, v := range m {
						n[k] = v
					}
					v := s.Value
					if str, ok := v.(string); ok && render != nil && strings.Contains(str, "{{") {
						rv, err := render(str, m)
						if err != nil {
							return nil, fmt.Errorf("step %d: %w", i+1, err)
						}
						v = rv
					}
					n[s.Field] = v
					out = append(out, n)
				}
			}
		}
		cur = out
	}
	return asList()
}

// TransformConfig configures a transform node.
type TransformConfig struct {
	Input string // upstream task; default: the only upstream task
	Steps []TransformStep
}

// NewTransformTask reshapes the rows produced upstream. Output (XCom return_value)
// is a JSON array of objects.
func (d *DAG) NewTransformTask(id string, cfg TransformConfig, opts ...TaskOption) *Task {
	for i, s := range cfg.Steps {
		if err := s.Validate(); err != nil {
			d.buildErr(fmt.Errorf("task %s: step %d: %w", id, i+1, err))
		}
	}
	b, _ := json.MarshalIndent(cfg.Steps, "", "  ")
	opts = append([]TaskOption{WithOperatorName("TransformOperator"),
		WithTemplateFields(TemplateField{Name: "steps", Raw: string(b), Lang: "json"})}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		from, err := upstreamInput(ctx, cfg.Input)
		if err != nil {
			return err
		}
		v, ok := ctx.XComLookup(from, "return_value")
		if !ok {
			return fmt.Errorf("task %q produced no output", from)
		}
		res, err := ApplyTransform(v, cfg.Steps, func(raw string, row map[string]any) (string, error) {
			return ctx.Render(raw, map[string]any{"row": row})
		})
		if err != nil {
			return err
		}
		ctx.Logf("transform: %d steps, %d rows out", len(cfg.Steps), len(res))
		return ctx.XComPush("return_value", res)
	}, opts...)
}

// ---- postgres ------------------------------------------------------------------

// PostgresConfig configures a postgres node: run SQL (rows come back as the
// task output), or insert the rows produced upstream into Table.
type PostgresConfig struct {
	ConnID     string
	SQL        string // templated
	Table      string // set = insert mode ("schema.table")
	RowsFrom   string // upstream task; default: the only upstream task
	Limit      int    // max rows returned by a query (default: no limit)
	OnConflict string // "" (error) or "do_nothing"
}

// PostgresDSN builds a connection string from a connection: either "uri"/"dsn",
// or host, port, login|user, password, database|schema, sslmode.
func PostgresDSN(c Connection) (string, error) {
	if u := firstNonEmpty(c.String("uri"), c.String("dsn")); u != "" {
		return u, nil
	}
	host := c.String("host")
	if host == "" {
		return "", errors.New("connection needs uri, or host/login/password/database")
	}
	port := c.String("port")
	if port == "" {
		if f, ok := c.Config["port"].(float64); ok {
			port = strconv.Itoa(int(f))
		} else {
			port = "5432"
		}
	}
	u := url.URL{Scheme: "postgres", Host: host + ":" + port, Path: "/" + firstNonEmpty(c.String("database"), c.String("schema"), "postgres")}
	if user := firstNonEmpty(c.String("login"), c.String("user")); user != "" {
		u.User = url.UserPassword(user, c.String("password"))
	}
	if ssl := c.String("sslmode"); ssl != "" {
		u.RawQuery = "sslmode=" + url.QueryEscape(ssl)
	}
	return u.String(), nil
}

func splitTable(t string) (pgx.Identifier, error) {
	parts := strings.Split(strings.TrimSpace(t), ".")
	if len(parts) == 0 || len(parts) > 2 {
		return nil, fmt.Errorf("table %q must be table or schema.table", t)
	}
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			return nil, fmt.Errorf("table %q must be table or schema.table", t)
		}
	}
	return pgx.Identifier(parts), nil
}

func jsonSafe(v any) any {
	if vl, ok := v.(driver.Valuer); ok {
		if x, err := vl.Value(); err == nil {
			return jsonSafe(x)
		}
	}
	switch x := v.(type) {
	case [16]byte:
		return fmt.Sprintf("%x-%x-%x-%x-%x", x[0:4], x[4:6], x[6:8], x[8:10], x[10:16])
	case []byte:
		return string(x)
	case map[string]any:
		for k, e := range x {
			x[k] = jsonSafe(e)
		}
	case []any:
		for i, e := range x {
			x[i] = jsonSafe(e)
		}
	}
	return v
}

// NewPostgresTask ~ PostgresOperator. In query mode the rows (max Limit) are
// XCom "return_value"; "rows_affected" is pushed too. In insert mode the rows
// of the upstream task are inserted into Table in one transaction.
func (d *DAG) NewPostgresTask(id string, cfg PostgresConfig, opts ...TaskOption) *Task {
	if cfg.ConnID == "" {
		d.buildErr(fmt.Errorf("task %s: postgres needs conn_id", id))
	}
	insert := cfg.Table != ""
	if insert {
		if _, err := splitTable(cfg.Table); err != nil {
			d.buildErr(fmt.Errorf("task %s: %w", id, err))
		}
		if cfg.SQL != "" {
			d.buildErr(fmt.Errorf("task %s: use either sql or table, not both", id))
		}
		if cfg.OnConflict != "" && cfg.OnConflict != "do_nothing" {
			d.buildErr(fmt.Errorf("task %s: on_conflict must be empty or do_nothing", id))
		}
	} else if strings.TrimSpace(cfg.SQL) == "" {
		d.buildErr(fmt.Errorf("task %s: postgres needs sql, or table to insert upstream rows", id))
	}
	if cfg.Limit <= 0 {
		cfg.Limit = defaultRowLimit
	}
	var fields []TemplateField
	if !insert {
		fields = append(fields, TemplateField{Name: "sql", Raw: cfg.SQL, Lang: "sql"})
	}
	opts = append([]TaskOption{WithOperatorName("PostgresOperator"), WithTemplateFields(fields...)}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		conn, ok := GetConnection(cfg.ConnID)
		if !ok {
			return fmt.Errorf("connection %q not found", cfg.ConnID)
		}
		dsn, err := PostgresDSN(conn)
		if err != nil {
			return fmt.Errorf("connection %q: %w", cfg.ConnID, err)
		}
		pg, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer pg.Close(context.Background())
		if insert {
			return pgInsert(ctx, pg, cfg)
		}
		query, err := ctx.RenderTemplateField("sql")
		if err != nil {
			return err
		}
		ctx.Logf("running SQL: %s", truncate(strings.TrimSpace(query), 2000))
		rows, err := pg.Query(ctx, query, pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			return err
		}
		defer rows.Close()
		names := []string{}
		for _, f := range rows.FieldDescriptions() {
			names = append(names, f.Name)
		}
		out := []any{}
		more := false
		for rows.Next() {
			if len(out) >= cfg.Limit {
				more = true
				break
			}
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			m := make(map[string]any, len(names))
			for i, n := range names {
				m[n] = jsonSafe(vals[i])
			}
			out = append(out, m)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		affected := rows.CommandTag().RowsAffected()
		if more {
			ctx.Logf("WARNING: result cut at %d rows (set a higher limit)", cfg.Limit)
		}
		ctx.Logf("%d rows returned, %d affected", len(out), affected)
		_ = ctx.XComPush("rows_affected", affected)
		return ctx.XComPush("return_value", out)
	}, opts...)
}

func pgInsert(ctx *TaskContext, pg *pgx.Conn, cfg PostgresConfig) error {
	from, err := upstreamInput(ctx, cfg.RowsFrom)
	if err != nil {
		return err
	}
	v, ok := ctx.XComLookup(from, "return_value")
	if !ok {
		return fmt.Errorf("task %q produced no output", from)
	}
	list, err := asRows(v)
	if err != nil {
		return err
	}
	if len(list) == 0 {
		ctx.Logf("no rows to insert")
		return ctx.XComPush("rows_affected", int64(0))
	}
	colset := map[string]bool{}
	rows := make([]map[string]any, 0, len(list))
	for i, r := range list {
		m, ok := r.(map[string]any)
		if !ok {
			return fmt.Errorf("row %d is %T, rows must be objects", i, r)
		}
		for k := range m {
			colset[k] = true
		}
		rows = append(rows, m)
	}
	cols := make([]string, 0, len(colset))
	for c := range colset {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	tbl, err := splitTable(cfg.Table)
	if err != nil {
		return err
	}
	quoted := make([]string, len(cols))
	ph := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = pgx.Identifier{c}.Sanitize()
		ph[i] = "$" + strconv.Itoa(i+1)
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tbl.Sanitize(), strings.Join(quoted, ", "), strings.Join(ph, ", "))
	if cfg.OnConflict == "do_nothing" {
		stmt += " ON CONFLICT DO NOTHING"
	}
	ctx.Logf("inserting %d rows into %s (%d columns)", len(rows), cfg.Table, len(cols))
	tx, err := pg.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var affected int64
	for start := 0; start < len(rows); start += insertChunk {
		end := min(start+insertChunk, len(rows))
		batch := &pgx.Batch{}
		for _, m := range rows[start:end] {
			args := make([]any, len(cols))
			for i, c := range cols {
				switch x := m[c].(type) {
				case map[string]any, []any:
					b, _ := json.Marshal(x)
					args[i] = string(b)
				default:
					args[i] = x
				}
			}
			batch.Queue(stmt, args...)
		}
		br := tx.SendBatch(ctx, batch)
		for range rows[start:end] {
			tag, err := br.Exec()
			if err != nil {
				_ = br.Close()
				return err
			}
			affected += tag.RowsAffected()
		}
		if err := br.Close(); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	ctx.Logf("%d rows inserted", affected)
	_ = ctx.XComPush("rows_affected", affected)
	return ctx.XComPush("return_value", map[string]any{"rows_inserted": affected})
}
