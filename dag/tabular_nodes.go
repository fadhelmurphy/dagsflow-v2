package dag

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// snakeKeys renames every key of a row to a snake_case name (letters and
// underscores only). Keys that collapse to the same name get a letter suffix;
// the order is the sorted order of the original keys, so it is deterministic.
func snakeKeys(m map[string]any) map[string]any {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = NormalizeHeader(k)
		if names[i] == "" {
			names[i] = "column"
		}
	}
	names = uniqueNames(names, true)
	out := make(map[string]any, len(m))
	for i, k := range keys {
		out[names[i]] = m[k]
	}
	return out
}

// ---- file_read ------------------------------------------------------------------

// FileReadConfig reads a CSV / TSV / Excel file (an upload, or a path on the
// worker) into rows. Range uses A1 notation to pick the start row and columns,
// e.g. "B3:F" = header on row 3, columns B to F, to the last row.
type FileReadConfig struct {
	File, Format, Sheet, Range, Delimiter string
	NoHeader, KeepHeaders                 bool
	Limit                                 int
}

func (d *DAG) NewFileReadTask(id string, cfg FileReadConfig, opts ...TaskOption) *Task {
	if strings.TrimSpace(cfg.File) == "" {
		d.buildErr(fmt.Errorf("task %s: file_read needs file", id))
	}
	if _, _, err := parseA1(cfg.Range); err != nil && !strings.Contains(cfg.Range, "{{") {
		d.buildErr(fmt.Errorf("task %s: %v", id, err))
	}
	if cfg.Limit <= 0 {
		cfg.Limit = defaultRowLimit
	}
	opts = append([]TaskOption{WithOperatorName("FileReadOperator"),
		WithTemplateFields(TemplateField{Name: "file", Raw: cfg.File, Lang: "text"}, TemplateField{Name: "range", Raw: cfg.Range, Lang: "text"})}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		name, err := ctx.RenderTemplateField("file")
		if err != nil {
			return err
		}
		rng, err := ctx.RenderTemplateField("range")
		if err != nil {
			return err
		}
		path, err := ResolveFile(name)
		if err != nil {
			return err
		}
		a1, sheetInRange, err := parseA1(rng)
		if err != nil {
			return err
		}
		sheet := firstNonEmpty(cfg.Sheet, sheetInRange)
		grid, info, err := ReadGrid(path, cfg.Format, sheet, cfg.Delimiter)
		if err != nil {
			return err
		}
		grid = a1.apply(grid)
		rows := tableRows(grid, !cfg.NoHeader, !cfg.KeepHeaders)
		if len(rows) > cfg.Limit {
			ctx.Logf("WARNING: file cut at %d rows (set a higher limit)", cfg.Limit)
			rows = rows[:cfg.Limit]
		}
		where := name
		if len(info.Sheets) > 0 {
			where += fmt.Sprintf(" (sheets: %s)", strings.Join(info.Sheets, ", "))
		}
		ctx.Logf("%d rows read from %s", len(rows), where)
		return ctx.XComPush("return_value", rows)
	}, opts...)
}

// ---- merge ----------------------------------------------------------------------

// MergeConfig combines the rows of several nodes: union (append them) or join
// (two inputs, matched on key columns; "left_col=right_col" when the names differ).
type MergeConfig struct {
	Mode     string   // union (default) | join
	Inputs   []string // task ids; default: all upstream nodes, in connection order
	On       []string
	JoinType string // inner (default) | left
}

func joinKey(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	if f, ok := num(v); ok {
		if _, isStr := v.(string); !isStr || strings.TrimSpace(v.(string)) != "" {
			return strconv.FormatFloat(f, 'f', -1, 64), true
		}
	}
	s := fmt.Sprint(v)
	return s, s != ""
}

func (d *DAG) NewMergeTask(id string, cfg MergeConfig, opts ...TaskOption) *Task {
	mode := firstNonEmpty(cfg.Mode, "union")
	if mode != "union" && mode != "join" {
		d.buildErr(fmt.Errorf("task %s: merge mode must be union or join", id))
	}
	if mode == "join" {
		if len(cfg.On) == 0 {
			d.buildErr(fmt.Errorf("task %s: join needs key column(s) in on", id))
		}
		if jt := firstNonEmpty(cfg.JoinType, "inner"); jt != "inner" && jt != "left" {
			d.buildErr(fmt.Errorf("task %s: join_type must be inner or left", id))
		}
	}
	opts = append([]TaskOption{WithOperatorName("MergeOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		ids := cfg.Inputs
		if len(ids) == 0 {
			for _, u := range ctx.Task.Upstream() {
				ids = append(ids, u.ID)
			}
		}
		if len(ids) < 2 {
			return errors.New("merge needs at least two input nodes: connect more nodes to it")
		}
		lists := make([][]any, len(ids))
		for i, tid := range ids {
			v, ok := ctx.XComLookup(tid, "return_value")
			if !ok {
				return fmt.Errorf("task %q produced no output", tid)
			}
			rows, err := asRows(v)
			if err != nil {
				return fmt.Errorf("input %q: %w", tid, err)
			}
			lists[i] = rows
		}
		var out []any
		if mode == "union" {
			out = []any{}
			for _, l := range lists {
				out = append(out, l...)
			}
			ctx.Logf("union of %d inputs: %d rows", len(lists), len(out))
			return ctx.XComPush("return_value", out)
		}
		if len(lists) != 2 {
			return fmt.Errorf("join takes exactly two inputs, got %d", len(lists))
		}
		var lk, rk []string
		for _, e := range cfg.On {
			l, r, found := strings.Cut(e, "=")
			if !found {
				r = l
			}
			lk, rk = append(lk, strings.TrimSpace(l)), append(rk, strings.TrimSpace(r))
		}
		sameKey := map[string]bool{}
		for i := range lk {
			if lk[i] == rk[i] {
				sameKey[lk[i]] = true
			}
		}
		keyOf := func(m map[string]any, cols []string) (string, bool) {
			parts := make([]string, len(cols))
			for i, c := range cols {
				k, ok := joinKey(m[c])
				if !ok {
					return "", false
				}
				parts[i] = k
			}
			return strings.Join(parts, "\x00"), true
		}
		index := map[string][]map[string]any{}
		for _, r := range lists[1] {
			m, ok := r.(map[string]any)
			if !ok {
				return fmt.Errorf("rows must be objects, got %T", r)
			}
			if k, ok := keyOf(m, rk); ok {
				index[k] = append(index[k], m)
			}
		}
		left := cfg.JoinType == "left"
		out = []any{}
		for _, r := range lists[0] {
			m, ok := r.(map[string]any)
			if !ok {
				return fmt.Errorf("rows must be objects, got %T", r)
			}
			k, ok := keyOf(m, lk)
			matches := index[k]
			if !ok || len(matches) == 0 {
				if left {
					out = append(out, m)
				}
				continue
			}
			for _, rm := range matches {
				row := make(map[string]any, len(m)+len(rm))
				for c, v := range m {
					row[c] = v
				}
				for c, v := range rm {
					if sameKey[c] {
						continue // a key with the same name on both sides is already there
					}
					if _, clash := row[c]; clash {
						c += "_right"
					}
					row[c] = v
				}
				out = append(out, row)
			}
		}
		ctx.Logf("join (%s): %d left × %d right -> %d rows", firstNonEmpty(cfg.JoinType, "inner"), len(lists[0]), len(lists[1]), len(out))
		return ctx.XComPush("return_value", out)
	}, opts...)
}

// Preview is what a file node would read: column names and a few rows.
type Preview struct {
	Columns []string
	Sample  []any
	Count   int
}

// PreviewRows applies a range to a grid the way the node does.
func PreviewRows(grid [][]any, rng string, noHeader, keepHeaders bool, sample int) (Preview, error) {
	a1, _, err := parseA1(rng)
	if err != nil {
		return Preview{}, err
	}
	rows := tableRows(a1.apply(grid), !noHeader, !keepHeaders)
	p := Preview{Count: len(rows), Columns: []string{}}
	g := a1.apply(grid)
	if len(g) > 0 {
		width := 0
		for _, r := range g {
			width = max(width, len(r))
		}
		if noHeader {
			for i := 0; i < width; i++ {
				p.Columns = append(p.Columns, "col_"+strconv.Itoa(i+1))
			}
		} else {
			p.Columns = headerNames(g[0], width, !keepHeaders)
		}
	}
	p.Sample = rows[:min(sample, len(rows))]
	return p, nil
}
