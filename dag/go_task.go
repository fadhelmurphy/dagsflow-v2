package dag

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var goVersionRe = regexp.MustCompile(`go(\d+\.\d+)`)

// goBinary is the Go toolchain used by Go code nodes (DAGSFLOW_GO overrides).
func goBinary() string {
	if g := os.Getenv("DAGSFLOW_GO"); g != "" {
		return g
	}
	return "go"
}

// NewGoTask runs a standalone Go program (package main with func main) with
// `go run`. Standard library only: the program lives in a throw-away module.
// Like the shell node, the last non-empty stdout line becomes return_value,
// the task context is passed as DAGSFLOW_CTX_* variables and env values are
// templated ({{ xcom_pull "task" "return_value" }} hands data in).
// The worker needs the Go toolchain installed.
func (d *DAG) NewGoTask(id, code string, env map[string]string, opts ...TaskOption) *Task {
	fields := []TemplateField{}
	if len(env) > 0 {
		m := map[string]any{}
		for k, v := range env {
			m[k] = v
		}
		fields = append(fields, TemplateField{Name: "env", Map: m, Lang: "json"})
	}
	opts = append([]TaskOption{WithTemplateFields(fields...), WithOperatorName("GoOperator")}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		if len(env) > 0 {
			_, _ = ctx.RenderTemplateField("env")
		}
		dir, err := os.MkdirTemp("", "dagsflow-gotask-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		goBin := goBinary()
		ver := "1.21"
		if out, err := exec.Command(goBin, "env", "GOVERSION").Output(); err == nil {
			if m := goVersionRe.FindStringSubmatch(string(out)); m != nil {
				ver = m[1]
			}
		} else {
			return fmt.Errorf("the Go toolchain was not found (%v): install Go on the worker or set DAGSFLOW_GO", err)
		}
		mod := "module dagsflowtask\n\ngo " + ver + "\n"
		if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(mod), 0o644); err != nil {
			return err
		}
		src := strings.ReplaceAll(code, "\r\n", "\n")
		if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src), 0o644); err != nil {
			return err
		}
		cmd := CommandContext(ctx, goBin, "run", ".")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GO111MODULE=on",
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
		ctx.Logf("running Go code (%s run)", goBin)
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
			return fmt.Errorf("go program failed: %w", err)
		}
		return ctx.XComPush("return_value", last)
	}, opts...)
}
