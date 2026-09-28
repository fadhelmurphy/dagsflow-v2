package dag

import (
	"bufio"
	"dagsflow-go/operator"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// NewEmptyTask ~ EmptyOperator (useful as start/end/join points).
func (d *DAG) NewEmptyTask(id string, opts ...TaskOption) *Task {
	opts = append([]TaskOption{WithOperatorName("EmptyOperator")}, opts...)
	return d.addTask(id, func(*TaskContext) error { return nil }, nil, nil, opts)
}

// ---- BigQuery ------------------------------------------------------------------

// NewBigQueryTask ~ BigQueryInsertJobOperator. The SQL file and every string
// param are rendered as templates (see TaskContext.Render).
// Special params: project_id, location, gcp_conn_id (default google_cloud_default),
// sql (inline query instead of sqlPath).
func (d *DAG) NewBigQueryTask(id, sqlPath string, params map[string]any, opts ...TaskOption) *Task {
	// params (minus the inline "sql" itself) are rendered first and exposed to the SQL template
	vars := map[string]any{}
	for k, v := range params {
		if k != "sql" {
			vars[k] = v
		}
	}
	sqlField := TemplateField{Name: "sql", File: sqlPath, Raw: str(params["sql"]), Lang: "sql", Vars: vars}
	fields := []TemplateField{sqlField}
	for _, k := range []string{"project_id", "location"} {
		if s := str(params[k]); s != "" {
			fields = append(fields, TemplateField{Name: k, Raw: s, Lang: "text"})
		}
	}
	opts = append([]TaskOption{WithTemplateFields(fields...), WithOperatorName("BigQueryInsertJobOperator"),
		WithExtraLink("BigQuery Job Detail", "bigquery_job_url"), WithExtraLink("BigQuery Table", "bigquery_table_url")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		rendered, err := renderMap(ctx, vars)
		if err != nil {
			return err
		}
		if p, ds, tb := str(rendered["project_id"]), str(rendered["destination_dataset"]), str(rendered["destination_table"]); p != "" && ds != "" && tb != "" {
			_ = ctx.XComPush("bigquery_table_url", fmt.Sprintf("https://console.cloud.google.com/bigquery?project=%s&ws=!1m5!1m4!4m3!1s%s!2s%s!3s%s", p, p, ds, tb))
		}
		for _, k := range []string{"project_id", "location"} {
			if s := str(rendered[k]); s != "" {
				ctx.RecordRendered(k, s)
			}
		}
		query, err := ctx.RenderTemplateField("sql")
		if err != nil {
			return err
		}
		connID := firstNonEmpty(str(rendered["gcp_conn_id"]), "google_cloud_default")
		conn, _ := GetConnection(connID)
		op := &operator.BigQueryOperator{
			TaskID:          id,
			Query:           query,
			ProjectID:       firstNonEmpty(str(rendered["project_id"]), str(ctx.Params["project_id"]), conn.String("project_id")),
			Location:        firstNonEmpty(str(rendered["location"]), conn.String("location")),
			CredentialsFile: conn.String("credentials_path"),
			Logf:            ctx.Logf,
		}
		op.OnJob = func(jobID, location string) {
			_ = ctx.XComPush("job_id", jobID)
			_ = ctx.XComPush("bigquery_job_url", fmt.Sprintf("https://console.cloud.google.com/bigquery?project=%s&j=bq:%s:%s&page=queryresults", op.ProjectID, location, jobID))
		}
		return op.Execute(ctx)
	}, opts...)
}

// ---- TriggerDagRun ---------------------------------------------------------------

// NewTriggerDagRunTask ~ TriggerDagRunOperator. String values in conf are templated.
func (d *DAG) NewTriggerDagRunTask(id, targetDagID string, conf map[string]any, waitForCompletion bool, opts ...TaskOption) *Task {
	if conf == nil {
		conf = map[string]any{}
	}
	opts = append([]TaskOption{WithTemplateFields(TemplateField{Name: "conf", Map: conf, Lang: "json"}), WithOperatorName("TriggerDagRunOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		c, err := renderMap(ctx, conf)
		if err != nil {
			return err
		}
		if b, err := json.MarshalIndent(c, "", "  "); err == nil {
			ctx.RecordRendered("conf", string(b))
		}
		runID, err := ctx.TriggerDagRun(targetDagID, c, waitForCompletion)
		if err != nil {
			return err
		}
		ctx.Logf("triggered %s run_id=%s", targetDagID, runID)
		return ctx.XComPush("trigger_run_id", runID)
	}, opts...)
}

// ---- Bash ----------------------------------------------------------------------

// NewBashTask ~ BashOperator. command and env values are templated. Output goes
// to the task log; the last stdout line is pushed as XCom "return_value".
// The process (group) is killed on timeout / scheduler shutdown.
func (d *DAG) NewBashTask(id, command string, env map[string]string, opts ...TaskOption) *Task {
	fields := []TemplateField{{Name: "bash_command", Raw: command, Lang: "bash"}}
	if len(env) > 0 {
		m := map[string]any{}
		for k, v := range env {
			m[k] = v
		}
		fields = append(fields, TemplateField{Name: "env", Map: m, Lang: "json"})
	}
	opts = append([]TaskOption{WithTemplateFields(fields...), WithOperatorName("BashOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		cmdStr, err := ctx.RenderTemplateField("bash_command")
		if err != nil {
			return err
		}
		if len(env) > 0 {
			_, _ = ctx.RenderTemplateField("env")
		}
		cmd := shellCommand(ctx, cmdStr)
		cmd.Env = append(os.Environ(),
			"DAGSFLOW_CTX_DAG_ID="+ctx.DAG.ID, "DAGSFLOW_CTX_TASK_ID="+ctx.Task.ID,
			"DAGSFLOW_CTX_RUN_ID="+ctx.RunID, "DAGSFLOW_CTX_DS="+ctx.LogicalDate.Format("2006-01-02"),
			fmt.Sprintf("DAGSFLOW_CTX_TRY_NUMBER=%d", ctx.TryNumber),
			fmt.Sprintf("DAGSFLOW_CTX_MAP_INDEX=%d", ctx.MapIndex))
		for k, v := range env {
			rv, err := ctx.Render(v, nil)
			if err != nil {
				return fmt.Errorf("render env %s: %w", k, err)
			}
			cmd.Env = append(cmd.Env, k+"="+rv)
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		cmd.Stderr = ctx.LogWriter()
		ctx.Logf("running: %s", cmdStr)
		if err := cmd.Start(); err != nil {
			return err
		}
		last := ""
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			line := sc.Text()
			fmt.Fprintln(ctx.LogWriter(), line)
			if strings.TrimSpace(line) != "" {
				last = line
			}
		}
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("command failed: %w", err)
		}
		return ctx.XComPush("return_value", last)
	}, opts...)
}

// ---- HTTP ----------------------------------------------------------------------

// HTTPRequest ~ HttpOperator. URL (or Endpoint appended to the connection's
// "host"), Headers and Body are templated. A connection may set "host" and
// "token" (sent as Bearer).
type HTTPRequest struct {
	Method       string
	URL          string
	Endpoint     string
	ConnID       string
	Headers      map[string]string
	Body         string
	ExpectStatus []int // default: any 2xx
	Timeout      time.Duration
}

// NewHTTPTask sends a request; the response body (max 64KB) is XCom "return_value".
func (d *DAG) NewHTTPTask(id string, req HTTPRequest, opts ...TaskOption) *Task {
	opts = append([]TaskOption{WithTemplateFields(httpTemplateFields(req)...), WithOperatorName("HttpOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		code, body, err := doHTTP(ctx, req)
		if err != nil {
			return err
		}
		ctx.Logf("HTTP %d", code)
		if !statusOK(code, req.ExpectStatus) {
			return fmt.Errorf("unexpected status %d: %s", code, truncate(body, 500))
		}
		return ctx.XComPush("return_value", body)
	}, opts...)
}

func httpTemplateFields(req HTTPRequest) []TemplateField {
	var f []TemplateField
	if req.ConnID != "" {
		f = append(f, TemplateField{Name: "endpoint", Raw: req.Endpoint, Lang: "text"})
	} else {
		f = append(f, TemplateField{Name: "url", Raw: req.URL, Lang: "text"})
	}
	if req.Body != "" {
		f = append(f, TemplateField{Name: "data", Raw: req.Body, Lang: "json"})
	}
	if len(req.Headers) > 0 {
		m := map[string]any{}
		for k, v := range req.Headers {
			m[k] = v
		}
		f = append(f, TemplateField{Name: "headers", Map: m, Lang: "json"})
	}
	return f
}

func doHTTP(ctx *TaskContext, req HTTPRequest) (int, string, error) {
	url := req.URL
	var conn Connection
	if req.ConnID != "" {
		c, ok := GetConnection(req.ConnID)
		if !ok {
			return 0, "", fmt.Errorf("connection %q not found", req.ConnID)
		}
		conn = c
		url = strings.TrimRight(c.String("host"), "/") + "/" + strings.TrimLeft(req.Endpoint, "/")
	}
	url, err := ctx.Render(url, nil)
	if err != nil {
		return 0, "", err
	}
	if req.ConnID != "" {
		ctx.RecordRendered("endpoint", strings.TrimPrefix(url, strings.TrimRight(conn.String("host"), "/")+"/"))
	} else {
		ctx.RecordRendered("url", url)
	}
	body, err := ctx.Render(req.Body, nil)
	if err != nil {
		return 0, "", err
	}
	if req.Body != "" {
		ctx.RecordRendered("data", body)
	}
	method := firstNonEmpty(strings.ToUpper(req.Method), http.MethodGet)
	hreq, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	if tok := conn.String("token"); tok != "" {
		hreq.Header.Set("Authorization", "Bearer "+tok)
	}
	rendered := map[string]string{}
	for k, v := range req.Headers {
		rv, err := ctx.Render(v, nil)
		if err != nil {
			return 0, "", err
		}
		hreq.Header.Set(k, rv)
		rendered[k] = rv
	}
	if len(rendered) > 0 {
		if b, err := json.MarshalIndent(rendered, "", "  "); err == nil {
			ctx.RecordRendered("headers", string(b))
		}
	}
	client := &http.Client{Timeout: req.Timeout}
	if client.Timeout == 0 {
		client.Timeout = 5 * time.Minute
	}
	ctx.Logf("%s %s", method, url)
	resp, err := client.Do(hreq)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	return resp.StatusCode, string(b), nil
}

func statusOK(code int, expect []int) bool {
	if len(expect) == 0 {
		return code >= 200 && code < 300
	}
	for _, e := range expect {
		if e == code {
			return true
		}
	}
	return false
}

// ---- helpers --------------------------------------------------------------------

func renderMap(ctx *TaskContext, in map[string]any) (map[string]any, error) {
	out := map[string]any{}
	for k, v := range in {
		if s, ok := v.(string); ok {
			r, err := ctx.Render(s, nil)
			if err != nil {
				return nil, fmt.Errorf("render %s: %w", k, err)
			}
			v = r
		}
		out[k] = v
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func str(v any) string { s, _ := v.(string); return s }

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
