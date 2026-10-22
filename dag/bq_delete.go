package dag

import (
	"fmt"
	"net/url"
	"strings"
)

// "BigQuery delete table": deletes tables, views or external tables through the
// BigQuery API (tables.delete). A table that is not there is skipped, unless
// FailIfMissing is set. Names are templated: dataset.orders_{{ .ds_nodash }}.

// BigQueryDeleteConfig configures bigquery_delete_table.
type BigQueryDeleteConfig struct {
	ConnID, ProjectID string
	Tables            []string // dataset.table or project.dataset.table
	FailIfMissing     bool
}

// NewBigQueryDeleteTableTask deletes the tables.
func (d *DAG) NewBigQueryDeleteTableTask(id string, cfg BigQueryDeleteConfig, opts ...TaskOption) *Task {
	if len(cfg.Tables) == 0 {
		d.buildErr(fmt.Errorf("task %s: bigquery_delete_table needs tables", id))
	}
	for _, t := range cfg.Tables {
		if !strings.Contains(t, "{{") && !bqTableRe.MatchString(strings.TrimSpace(t)) {
			d.buildErr(fmt.Errorf("task %s: %q must be dataset.table or project.dataset.table", id, t))
		}
	}
	opts = append([]TaskOption{WithOperatorName("BigQueryDeleteTableOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		conn, _ := googleConn(cfg.ConnID)
		project := firstNonEmpty(cfg.ProjectID, str(ctx.Params["project_id"]), conn.String("project_id"))
		if k, _ := googleCreds(conn); project == "" && k != nil {
			project = k.ProjectID
		}
		var deleted, missing []string
		for _, raw := range cfg.Tables {
			name, err := ctx.Render(raw, nil)
			if err != nil {
				return fmt.Errorf("table %q: %w", raw, err)
			}
			name = strings.Trim(strings.TrimSpace(name), "`")
			m := bqTableRe.FindStringSubmatch(name)
			if m == nil {
				return fmt.Errorf("%q is not dataset.table or project.dataset.table", name)
			}
			p := firstNonEmpty(m[1], project)
			if p == "" {
				return errBQProject
			}
			full := p + "." + m[2] + "." + m[3]
			u := fmt.Sprintf("%s/projects/%s/datasets/%s/tables/%s", bqBase, url.PathEscape(p), url.PathEscape(m[2]), url.PathEscape(m[3]))
			err = googleCall(ctx, conn, scopeBigQuery, "DELETE", u, nil, nil)
			switch {
			case err == nil:
				ctx.Logf("deleted %s", full)
				deleted = append(deleted, full)
			case strings.HasPrefix(err.Error(), "HTTP 404"):
				if cfg.FailIfMissing {
					return fmt.Errorf("%s not found", full)
				}
				ctx.Logf("%s not found, skipped", full)
				missing = append(missing, full)
			default:
				return fmt.Errorf("delete %s: %w", full, err)
			}
		}
		_ = ctx.XComPush("rows_affected", int64(len(deleted)))
		return ctx.XComPush("return_value", map[string]any{"deleted": anyList(deleted), "missing": anyList(missing)})
	}, opts...)
}

func anyList(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
