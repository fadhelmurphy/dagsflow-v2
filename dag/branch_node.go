package dag

import (
	"encoding/json"
	"fmt"
	"strings"
)

// IfConfig is the builder's "If" node (YAML `operator: branch`): it checks a
// value and runs the nodes in Then when the check is true, the nodes in Else
// otherwise. The nodes of the other side are skipped, like a BranchPythonOperator.
type IfConfig struct {
	// Input is the upstream node whose output (return_value) is checked.
	// Default: the only upstream node. Ignored when Left is set.
	Input string
	// Path digs into that output: "0.status", "meta.count". A last segment
	// "length" gives the number of items (rows, keys or characters).
	Path string
	// Left is a template checked instead of an upstream output, e.g. {{ .params.mode }}.
	Left string
	// Op: == != > >= < <= contains not_contains empty not_empty exists not_exists.
	Op string
	// Value is the right-hand side (templates work when it is a string).
	Value any
	Then  []string
	Else  []string
}

var ifOps = map[string]bool{"==": true, "!=": true, ">": true, ">=": true, "<": true, "<=": true,
	"contains": true, "not_contains": true, "empty": true, "not_empty": true, "exists": true, "not_exists": true}

// IfOps lists the operators of an If node (for the builder).
var IfOps = []string{"not_empty", "empty", "==", "!=", ">", ">=", "<", "<=", "contains", "not_contains", "exists", "not_exists"}

func (c IfConfig) validate() error {
	if !ifOps[c.op()] {
		return fmt.Errorf("unknown op %q (%s)", c.Op, strings.Join(IfOps, ", "))
	}
	if len(c.Then) == 0 && len(c.Else) == 0 {
		return fmt.Errorf("connect at least one node to the true or false side")
	}
	seen := map[string]bool{}
	for _, id := range c.Then {
		seen[id] = true
	}
	for _, id := range c.Else {
		if seen[id] {
			return fmt.Errorf("node %q is on both the true and the false side", id)
		}
	}
	return nil
}

func (c IfConfig) op() string {
	if c.Op == "" {
		return "not_empty"
	}
	return c.Op
}

// NewIfTask adds an If node. Then/Else must be direct downstream tasks.
func (d *DAG) NewIfTask(id string, cfg IfConfig, opts ...TaskOption) *Task {
	if err := cfg.validate(); err != nil {
		d.buildErr(fmt.Errorf("if node %s: %w", id, err))
	}
	opts = append([]TaskOption{WithOperatorName("BranchOperator")}, opts...)
	if cfg.Left != "" {
		opts = append([]TaskOption{WithTemplateField("left", cfg.Left, "text")}, opts...)
	}
	return d.NewBranchTask(id, func(ctx *TaskContext) ([]string, error) {
		got, desc, err := cfg.value(ctx)
		if err != nil {
			return nil, err
		}
		want := cfg.Value
		if s, ok := want.(string); ok && strings.Contains(s, "{{") {
			r, err := ctx.Render(s, nil)
			if err != nil {
				return nil, fmt.Errorf("value: %w", err)
			}
			want = r
		}
		// text from a shell or an HTTP body usually ends with a newline
		if s, isStr := got.(string); isStr {
			got = strings.TrimSpace(s)
		}
		if s, isStr := want.(string); isStr {
			want = strings.TrimSpace(s)
		}
		ok := evalIf(got, cfg.op(), want)
		run, skip := cfg.Then, cfg.Else
		if !ok {
			run, skip = cfg.Else, cfg.Then
		}
		ctx.Logf("%s = %s; %s %s → %v; run %v, skip %v", desc, short(got), cfg.op(), short(want), ok, run, skip)
		if err := ctx.XComPush("return_value", ok); err != nil {
			return nil, err
		}
		if run == nil {
			run = []string{}
		}
		return run, nil
	}, opts...)
}

func (c IfConfig) value(ctx *TaskContext) (any, string, error) {
	if c.Left != "" {
		v, err := ctx.RenderTemplateField("left")
		if err != nil {
			return nil, "", fmt.Errorf("left: %w", err)
		}
		return v, "value", nil
	}
	from, err := upstreamInput(ctx, c.Input)
	if err != nil {
		return nil, "", err
	}
	v, _ := ctx.XComLookup(from, "return_value")
	if s, ok := v.(string); ok {
		var parsed any
		if t := strings.TrimSpace(s); t != "" && strings.ContainsAny(t[:1], "[{\"") && json.Unmarshal([]byte(t), &parsed) == nil {
			v = parsed
		}
	}
	desc := from
	if c.Path == "" {
		return v, desc, nil
	}
	desc += "." + c.Path
	path, length := c.Path, false
	if path == "length" || strings.HasSuffix(path, ".length") {
		path, length = strings.TrimSuffix(strings.TrimSuffix(path, "length"), "."), true
	}
	if path != "" {
		if v, err = dig(v, path); err != nil {
			if c.op() == "exists" || c.op() == "not_exists" {
				return nil, desc, nil
			}
			return nil, desc, err
		}
	}
	if length {
		switch x := v.(type) {
		case []any:
			v = len(x)
		case map[string]any:
			v = len(x)
		case string:
			v = len([]rune(x))
		case nil:
			v = 0
		default:
			return nil, desc, fmt.Errorf("%s: length of %T", desc, v)
		}
	}
	return v, desc, nil
}

func evalIf(got any, op string, want any) bool {
	switch op {
	case "empty", "not_empty":
		empty := false
		switch x := got.(type) {
		case nil:
			empty = true
		case string:
			empty = strings.TrimSpace(x) == ""
		case []any:
			empty = len(x) == 0
		case map[string]any:
			empty = len(x) == 0
		case bool:
			empty = !x
		}
		return empty == (op == "empty")
	case "not_contains":
		return !compare(got, "contains", want)
	}
	return compare(got, op, want)
}

func short(v any) string {
	b, err := json.Marshal(v)
	s := string(b)
	if err != nil {
		s = fmt.Sprint(v)
	}
	if len(s) > 120 {
		s = s[:117] + "..."
	}
	return s
}
