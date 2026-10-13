package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ---- BigQuery: read rows / insert rows -----------------------------------------

type bqField struct {
	Name   string    `json:"name"`
	Type   string    `json:"type"`
	Mode   string    `json:"mode"`
	Fields []bqField `json:"fields"`
}

type bqResult struct {
	JobComplete  bool `json:"jobComplete"`
	JobReference struct {
		JobID    string `json:"jobId"`
		Location string `json:"location"`
	} `json:"jobReference"`
	Schema struct {
		Fields []bqField `json:"fields"`
	} `json:"schema"`
	Rows      []json.RawMessage `json:"rows"`
	PageToken string            `json:"pageToken"`
}

func bqValue(f bqField, v any) any {
	if f.Mode == "REPEATED" {
		list, _ := v.([]any)
		out := make([]any, 0, len(list))
		single := f
		single.Mode = ""
		for _, e := range list {
			m, _ := e.(map[string]any)
			out = append(out, bqValue(single, m["v"]))
		}
		return out
	}
	if v == nil {
		return nil
	}
	if f.Type == "RECORD" || f.Type == "STRUCT" {
		m, _ := v.(map[string]any)
		cells, _ := m["f"].([]any)
		obj := map[string]any{}
		for i, sub := range f.Fields {
			if i < len(cells) {
				c, _ := cells[i].(map[string]any)
				obj[sub.Name] = bqValue(sub, c["v"])
			}
		}
		return obj
	}
	s, _ := v.(string)
	switch f.Type {
	case "INTEGER", "INT64":
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	case "FLOAT", "FLOAT64":
		if n, err := strconv.ParseFloat(s, 64); err == nil {
			return n
		}
	case "BOOLEAN", "BOOL":
		return s == "true"
	case "TIMESTAMP":
		if sec, err := strconv.ParseFloat(s, 64); err == nil {
			whole := int64(sec)
			return time.Unix(whole, int64((sec-float64(whole))*1e9)).UTC().Format(time.RFC3339Nano)
		}
	}
	return s // NUMERIC keeps its precision as text; DATE, DATETIME, STRING, JSON as is
}

func bqRows(schema []bqField, raw []json.RawMessage) ([]any, error) {
	out := make([]any, 0, len(raw))
	for _, r := range raw {
		var row struct {
			F []map[string]any `json:"f"`
		}
		if err := json.Unmarshal(r, &row); err != nil {
			return nil, err
		}
		obj := map[string]any{}
		for i, f := range schema {
			if i < len(row.F) {
				obj[f.Name] = bqValue(f, row.F[i]["v"])
			}
		}
		out = append(out, obj)
	}
	return out, nil
}

// BigQueryQueryConfig: run a query and return its rows.
type BigQueryQueryConfig struct {
	ConnID, ProjectID, Location, SQL string
	Limit                            int
}

// NewBigQueryQueryTask runs the query and returns up to Limit rows as the
// task output (XCom return_value, a JSON array of objects).
func (d *DAG) NewBigQueryQueryTask(id string, cfg BigQueryQueryConfig, opts ...TaskOption) *Task {
	if strings.TrimSpace(cfg.SQL) == "" {
		d.buildErr(fmt.Errorf("task %s: bigquery_query needs sql", id))
	}
	if cfg.Limit <= 0 {
		cfg.Limit = defaultRowLimit
	}
	opts = append([]TaskOption{WithOperatorName("BigQueryQueryOperator"),
		WithTemplateFields(TemplateField{Name: "sql", Raw: cfg.SQL, Lang: "sql"})}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		conn, _ := googleConn(cfg.ConnID)
		project := firstNonEmpty(cfg.ProjectID, str(ctx.Params["project_id"]), conn.String("project_id"))
		if k, _ := googleCreds(conn); project == "" && k != nil {
			project = k.ProjectID
		}
		if project == "" {
			return errBQProject
		}
		query, err := ctx.RenderTemplateField("sql")
		if err != nil {
			return err
		}
		ctx.Logf("BigQuery query on %s:\n%s", project, truncate(strings.TrimSpace(query), 2000))
		rows, err := bqQuery(ctx, conn, project, firstNonEmpty(cfg.Location, conn.String("location")), query, cfg.Limit, ctx.Logf)
		if err != nil {
			return err
		}
		ctx.Logf("%d rows returned", len(rows))
		return ctx.XComPush("return_value", rows)
	}, opts...)
}

var errBQProject = fmt.Errorf("project_id is empty (set it on the node, in dag params, or on the connection)")

func bqQuery(ctx context.Context, conn Connection, project, location, sql string, limit int, logf func(string, ...any)) ([]any, error) {
	page := min(limit, 10000)
	body := map[string]any{"query": sql, "useLegacySql": false, "maxResults": page, "timeoutMs": 10000}
	if location != "" {
		body["location"] = location
	}
	var res bqResult
	if err := googleCall(ctx, conn, scopeBigQuery, "POST", fmt.Sprintf("%s/projects/%s/queries", bqBase, url.PathEscape(project)), body, &res); err != nil {
		return nil, fmt.Errorf("start query: %w", err)
	}
	for !res.JobComplete {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
		q := url.Values{"timeoutMs": {"10000"}, "maxResults": {strconv.Itoa(page)}}
		if loc := firstNonEmpty(res.JobReference.Location, location); loc != "" {
			q.Set("location", loc)
		}
		var next bqResult
		if err := googleCall(ctx, conn, scopeBigQuery, "GET", fmt.Sprintf("%s/projects/%s/queries/%s?%s", bqBase, url.PathEscape(project), url.PathEscape(res.JobReference.JobID), q.Encode()), nil, &next); err != nil {
			return nil, fmt.Errorf("wait for query: %w", err)
		}
		if next.JobReference.JobID == "" {
			next.JobReference = res.JobReference
		}
		res = next
	}
	schema := res.Schema.Fields
	out := []any{}
	for {
		rows, err := bqRows(schema, res.Rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		if res.PageToken == "" {
			break
		}
		if len(out) >= limit {
			logf("WARNING: result cut at %d rows (set a higher limit)", limit)
			break
		}
		q := url.Values{"pageToken": {res.PageToken}, "maxResults": {strconv.Itoa(page)}}
		if loc := firstNonEmpty(res.JobReference.Location, location); loc != "" {
			q.Set("location", loc)
		}
		jobRef := res.JobReference
		var next bqResult
		if err := googleCall(ctx, conn, scopeBigQuery, "GET", fmt.Sprintf("%s/projects/%s/queries/%s?%s", bqBase, url.PathEscape(project), url.PathEscape(jobRef.JobID), q.Encode()), nil, &next); err != nil {
			return nil, fmt.Errorf("read next page: %w", err)
		}
		next.JobReference = jobRef
		if len(next.Schema.Fields) == 0 {
			next.Schema = res.Schema
		}
		res = next
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// BigQueryInsertConfig: stream the rows of an upstream task into a table.
type BigQueryInsertConfig struct {
	ConnID, ProjectID, Dataset, Table, RowsFrom string
}

// NewBigQueryInsertTask inserts the upstream rows with the streaming API
// (tabledata.insertAll). The table must exist; unknown columns are an error.
func (d *DAG) NewBigQueryInsertTask(id string, cfg BigQueryInsertConfig, opts ...TaskOption) *Task {
	if cfg.Dataset == "" || cfg.Table == "" {
		d.buildErr(fmt.Errorf("task %s: bigquery_insert needs dataset and table", id))
	}
	opts = append([]TaskOption{WithOperatorName("BigQueryInsertRowsOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		conn, _ := googleConn(cfg.ConnID)
		project := firstNonEmpty(cfg.ProjectID, str(ctx.Params["project_id"]), conn.String("project_id"))
		if k, _ := googleCreds(conn); project == "" && k != nil {
			project = k.ProjectID
		}
		if project == "" {
			return errBQProject
		}
		rows, err := inputRowList(ctx, cfg.RowsFrom)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			ctx.Logf("no rows to insert")
			return ctx.XComPush("rows_affected", int64(0))
		}
		ctx.Logf("inserting %d rows into %s.%s.%s", len(rows), project, cfg.Dataset, cfg.Table)
		u := fmt.Sprintf("%s/projects/%s/datasets/%s/tables/%s/insertAll", bqBase, url.PathEscape(project), url.PathEscape(cfg.Dataset), url.PathEscape(cfg.Table))
		for start := 0; start < len(rows); start += insertChunk {
			end := min(start+insertChunk, len(rows))
			items := make([]any, 0, end-start)
			for _, r := range rows[start:end] {
				items = append(items, map[string]any{"json": r})
			}
			var res struct {
				InsertErrors []struct {
					Index  int `json:"index"`
					Errors []struct {
						Reason  string `json:"reason"`
						Message string `json:"message"`
					} `json:"errors"`
				} `json:"insertErrors"`
			}
			if err := googleCall(ctx, conn, scopeBigQuery, "POST", u, map[string]any{"rows": items}, &res); err != nil {
				return err
			}
			if len(res.InsertErrors) > 0 {
				e := res.InsertErrors[0]
				msg := ""
				if len(e.Errors) > 0 {
					msg = e.Errors[0].Reason + ": " + e.Errors[0].Message
				}
				return fmt.Errorf("%d rows were rejected, first is row %d: %s", len(res.InsertErrors), start+e.Index, msg)
			}
		}
		ctx.Logf("%d rows inserted", len(rows))
		_ = ctx.XComPush("rows_affected", int64(len(rows)))
		return ctx.XComPush("return_value", map[string]any{"rows_inserted": len(rows)})
	}, opts...)
}

// inputRowList reads the rows of the upstream task.
func inputRowList(ctx *TaskContext, from string) ([]any, error) {
	id, err := upstreamInput(ctx, from)
	if err != nil {
		return nil, err
	}
	v, ok := ctx.XComLookup(id, "return_value")
	if !ok {
		return nil, fmt.Errorf("task %q produced no output", id)
	}
	return asRows(v)
}

// ---- Google Sheets ---------------------------------------------------------------

var (
	sheetIDRe = regexp.MustCompile(`/spreadsheets/d/([A-Za-z0-9_-]+)`)
	a1Re      = regexp.MustCompile(`^\$?[A-Za-z]{1,3}\$?\d*(:\$?[A-Za-z]{1,3}\$?\d*)?$`)
)

// SheetID accepts a spreadsheet id or its full URL.
func SheetID(s string) string {
	s = strings.TrimSpace(s)
	if m := sheetIDRe.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return s
}

func sheetTab(rng string) string {
	if i := strings.LastIndex(rng, "!"); i >= 0 {
		return rng[:i+1]
	}
	if rng != "" && !a1Re.MatchString(rng) {
		return rng + "!" // a bare tab name
	}
	return ""
}

func sheetURL(id, rng, suffix string) string {
	return fmt.Sprintf("%s/%s/values/%s%s", sheetsBase, url.PathEscape(id), url.PathEscape(rng), suffix)
}

// GSheetReadConfig: read a range into rows.
type GSheetReadConfig struct {
	ConnID, SpreadsheetID, Range string
	NoHeader, KeepHeaders        bool
	Limit                        int
}

// NewGSheetReadTask reads a range. With a header row (default) every following
// row becomes an object keyed by the header; otherwise keys are col_1, col_2…
func (d *DAG) NewGSheetReadTask(id string, cfg GSheetReadConfig, opts ...TaskOption) *Task {
	if SheetID(cfg.SpreadsheetID) == "" {
		d.buildErr(fmt.Errorf("task %s: gsheet_read needs spreadsheet_id", id))
	}
	if strings.TrimSpace(cfg.Range) == "" {
		d.buildErr(fmt.Errorf("task %s: gsheet_read needs range, e.g. Sheet1 or Sheet1!A1:F", id))
	}
	if cfg.Limit <= 0 {
		cfg.Limit = defaultRowLimit
	}
	opts = append([]TaskOption{WithOperatorName("GoogleSheetsReadOperator"),
		WithTemplateFields(TemplateField{Name: "range", Raw: cfg.Range, Lang: "text"})}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		conn, _ := googleConn(cfg.ConnID)
		rng, err := ctx.RenderTemplateField("range")
		if err != nil {
			return err
		}
		var res struct {
			Values [][]any `json:"values"`
		}
		u := sheetURL(SheetID(cfg.SpreadsheetID), rng, "?majorDimension=ROWS&valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING")
		ctx.Logf("reading %s of %s", rng, SheetID(cfg.SpreadsheetID))
		if err := googleCall(ctx, conn, scopeSheetsRead, "GET", u, nil, &res); err != nil {
			return err
		}
		rows := sheetRows(res.Values, !cfg.NoHeader, !cfg.KeepHeaders)
		if len(rows) > cfg.Limit {
			ctx.Logf("WARNING: sheet cut at %d rows (set a higher limit)", cfg.Limit)
			rows = rows[:cfg.Limit]
		}
		ctx.Logf("%d rows read", len(rows))
		return ctx.XComPush("return_value", rows)
	}, opts...)
}

func sheetRows(values [][]any, header, normalize bool) []any {
	return tableRows(values, header, normalize)
}

// GSheetWriteConfig: append the upstream rows to a sheet, or replace its content.
type GSheetWriteConfig struct {
	ConnID, SpreadsheetID, Range, RowsFrom string
	Mode                                   string // append (default) | overwrite
	Columns                                []string
	UserEntered                            bool // parse values like typing in the sheet (dates, formulas)
}

// NewGSheetWriteTask writes the rows of the upstream task. append adds rows
// below the existing data (writing the header too when the sheet is empty);
// overwrite clears the range first and writes header + rows.
func (d *DAG) NewGSheetWriteTask(id string, cfg GSheetWriteConfig, opts ...TaskOption) *Task {
	if SheetID(cfg.SpreadsheetID) == "" {
		d.buildErr(fmt.Errorf("task %s: gsheet_write needs spreadsheet_id", id))
	}
	if strings.TrimSpace(cfg.Range) == "" {
		d.buildErr(fmt.Errorf("task %s: gsheet_write needs range, e.g. Sheet1 or Sheet1!A1", id))
	}
	if cfg.Mode != "" && cfg.Mode != "append" && cfg.Mode != "overwrite" {
		d.buildErr(fmt.Errorf("task %s: write_mode must be append or overwrite", id))
	}
	opts = append([]TaskOption{WithOperatorName("GoogleSheetsWriteOperator"),
		WithTemplateFields(TemplateField{Name: "range", Raw: cfg.Range, Lang: "text"})}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		conn, _ := googleConn(cfg.ConnID)
		rng, err := ctx.RenderTemplateField("range")
		if err != nil {
			return err
		}
		list, err := inputRowList(ctx, cfg.RowsFrom)
		if err != nil {
			return err
		}
		if len(list) == 0 {
			ctx.Logf("no rows to write")
			return ctx.XComPush("rows_affected", int64(0))
		}
		cols := cfg.Columns
		if len(cols) == 0 {
			set := map[string]bool{}
			for _, r := range list {
				m, ok := r.(map[string]any)
				if !ok {
					return fmt.Errorf("rows must be objects, got %T", r)
				}
				for k := range m {
					set[k] = true
				}
			}
			for k := range set {
				cols = append(cols, k)
			}
			sort.Strings(cols)
		}
		values := make([][]any, 0, len(list)+1)
		for _, r := range list {
			m, ok := r.(map[string]any)
			if !ok {
				return fmt.Errorf("rows must be objects, got %T", r)
			}
			line := make([]any, len(cols))
			for i, c := range cols {
				switch x := m[c].(type) {
				case map[string]any, []any:
					b, _ := json.Marshal(x)
					line[i] = string(b)
				case nil:
					line[i] = ""
				default:
					line[i] = x
				}
			}
			values = append(values, line)
		}
		header := make([]any, len(cols))
		for i, c := range cols {
			header[i] = c
		}
		id := SheetID(cfg.SpreadsheetID)
		input := "RAW"
		if cfg.UserEntered {
			input = "USER_ENTERED"
		}
		if cfg.Mode == "overwrite" {
			ctx.Logf("clearing %s and writing %d rows", rng, len(values))
			if err := googleCall(ctx, conn, scopeSheets, "POST", sheetURL(id, rng, ":clear"), map[string]any{}, nil); err != nil {
				return err
			}
			body := map[string]any{"values": append([][]any{header}, values...)}
			if err := googleCall(ctx, conn, scopeSheets, "PUT", sheetURL(id, rng, "?valueInputOption="+input), body, nil); err != nil {
				return err
			}
		} else {
			var probe struct {
				Values [][]any `json:"values"`
			}
			first := sheetTab(rng) + "A1:A1"
			if err := googleCall(ctx, conn, scopeSheetsRead, "GET", sheetURL(id, first, ""), nil, &probe); err != nil {
				return err
			}
			out := values
			if len(probe.Values) == 0 {
				out = append([][]any{header}, values...) // empty sheet: write the header too
			}
			ctx.Logf("appending %d rows to %s", len(values), rng)
			u := sheetURL(id, rng, ":append?valueInputOption="+input+"&insertDataOption=INSERT_ROWS")
			if err := googleCall(ctx, conn, scopeSheets, "POST", u, map[string]any{"values": out}, nil); err != nil {
				return err
			}
		}
		_ = ctx.XComPush("rows_affected", int64(len(values)))
		return ctx.XComPush("return_value", map[string]any{"rows_written": len(values)})
	}, opts...)
}
