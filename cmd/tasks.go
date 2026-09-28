package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"dagsflow-go/dag"
	"dagsflow-go/engine"

	"github.com/spf13/cobra"
)

func tasksCmd() *cobra.Command {
	c := &cobra.Command{Use: "tasks", Short: "Manage task instances (states, clear, test, logs)"}

	list := &cobra.Command{
		Use: "list <dag_id>", Short: "List tasks of a DAG", Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			w := table()
			fmt.Fprintln(w, "TASK_ID\tTRIGGER_RULE\tRETRIES\tTIMEOUT\tUPSTREAM")
			for _, t := range d.Tasks() {
				var up []string
				for _, u := range t.Upstream() {
					up = append(up, u.ID)
				}
				fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%v\n", t.ID, t.TriggerRule(), t.Retries(), t.Timeout(), up)
			}
			w.Flush()
		},
	}

	states := &cobra.Command{
		Use: "states <dag_id> <run_id>", Short: "Task instance states of a DagRun", Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			r, err := newStore().LoadRun(d.ID, args[1])
			if err != nil {
				exitf("%v", err)
			}
			printStates(d, r)
		},
	}

	clear := &cobra.Command{
		Use:   "clear <dag_id> <run_id>",
		Short: "Clear task instances so the scheduler reruns them (like `airflow tasks clear`)",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			st := newStore()
			r, err := st.LoadRun(d.ID, args[1])
			if err != nil {
				exitf("%v", err)
			}
			ids, _ := cmd.Flags().GetStringSlice("task")
			up, _ := cmd.Flags().GetBool("upstream")
			down, _ := cmd.Flags().GetBool("downstream")
			onlyFailed, _ := cmd.Flags().GetBool("only-failed")
			mi, _ := cmd.Flags().GetInt("map-index")
			cleared, err := engine.ClearTaskInstances(d, r, engine.ClearOptions{TaskIDs: ids, Upstream: up, Downstream: down, OnlyFailed: onlyFailed, MapIndex: mi})
			if err != nil {
				exitf("%v", err)
			}
			if len(cleared) == 0 {
				fmt.Println("nothing to clear")
				return
			}
			if now, _ := cmd.Flags().GetBool("run-now"); now {
				if err := st.TakeOwnership(r); err != nil {
					exitf("%v", err)
				}
				fmt.Printf("cleared %v in %s/%s\n", cleared, d.ID, r.RunID)
				loadConnections()
				runAndReport(d, st, r)
				return
			}
			if err := st.SaveRun(r); err != nil {
				exitf("%v", err)
			}
			fmt.Printf("cleared %v in %s/%s (run re-queued)\n", cleared, d.ID, r.RunID)
			if _, alive := engine.SchedulerInfo(st); !alive {
				fmt.Println("note: scheduler is not running; start it, or use --run-now")
			}
		},
	}
	clear.Flags().StringSliceP("task", "t", nil, "task id(s) to clear (default: all)")
	clear.Flags().Bool("upstream", false, "also clear upstream tasks")
	clear.Flags().Bool("downstream", false, "also clear downstream tasks")
	clear.Flags().Bool("only-failed", false, "only clear failed/upstream_failed tasks")
	clear.Flags().Int("map-index", -1, "clear only this mapped instance of the (single) --task")
	clear.Flags().Bool("run-now", false, "execute the cleared tasks in this process instead of via the scheduler")

	test := &cobra.Command{
		Use: "test <dag_id> <task_id>", Short: "Run a single task in isolation (no deps, nothing persisted)", Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			t, ok := d.Task(args[1])
			if !ok {
				exitf("task %s not found", args[1])
			}
			conf, _ := cmd.Flags().GetString("conf")
			xcomJSON, _ := cmd.Flags().GetString("xcom")
			itemJSON, _ := cmd.Flags().GetString("item")
			mem := &memRun{xcom: map[string]map[string]any{}}
			if xcomJSON != "" {
				if err := json.Unmarshal([]byte(xcomJSON), &mem.xcom); err != nil {
					exitf("--xcom must be JSON like {\"task\":{\"key\":1}}: %v", err)
				}
			}
			now := time.Now().UTC()
			info := dag.RunInfo{RunID: "tasks_test", RunType: "test", LogicalDate: now,
				DataIntervalStart: now, DataIntervalEnd: now, Conf: parseConf(conf), TryNumber: 1, MapIndex: -1}
			if itemJSON != "" {
				if err := json.Unmarshal([]byte(itemJSON), &info.Item); err != nil {
					exitf("--item must be JSON: %v", err)
				}
				info.MapIndex = 0
			}
			ctx, cancel := signalContext()
			defer cancel()
			if t.Timeout() > 0 {
				ctx, cancel = context.WithTimeout(ctx, t.Timeout())
				defer cancel()
			}
			tctx := dag.NewTaskContext(ctx, t, info, mem, os.Stdout)
			var sel []string
			var err error
			if sn := t.Sensor(); sn != nil {
				var ok bool
				ok, err = sn.Poke(tctx)
				fmt.Println("sensor condition met:", ok)
			} else {
				sel, err = t.Run(tctx)
			}
			if sel != nil {
				fmt.Println("branch selected:", sel)
			}
			if len(mem.xcom) > 0 {
				b, _ := json.Marshal(mem.xcom)
				fmt.Println("xcom:", string(b))
			}
			if len(mem.rendered) > 0 {
				fmt.Println("rendered template fields:")
				fmt.Println(strings.Join(mem.rendered, "\n"))
			}
			if err != nil {
				exitf("task failed: %v", err)
			}
			fmt.Println("task succeeded")
		},
	}
	test.Flags().String("conf", "", "JSON conf")
	test.Flags().String("xcom", "", `pre-seed upstream XComs, e.g. '{"start":{"val":99}}'`)
	test.Flags().String("item", "", "JSON item for a mapped task (ctx.Item)")

	logs := &cobra.Command{
		Use: "logs <dag_id> <run_id> <task_id>", Short: "Show a task instance log", Args: cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			st := newStore()
			try, _ := cmd.Flags().GetInt("try")
			idx, _ := cmd.Flags().GetInt("map-index")
			if try == 0 {
				r, err := st.LoadRun(args[0], args[1])
				if err != nil {
					exitf("%v", err)
				}
				ti := r.Tasks[args[2]]
				if idx >= 0 && idx < len(r.Mapped[args[2]]) {
					ti = r.Mapped[args[2]][idx]
				}
				if ti == nil || ti.TryNumber == 0 {
					exitf("task %s has no attempts in this run", args[2])
				}
				try = ti.TryNumber
			}
			follow, _ := cmd.Flags().GetBool("follow")
			tailLog(st, engine.LogKey{DagID: args[0], RunID: args[1], TaskID: args[2], MapIndex: idx, Try: try}, follow)
		},
	}
	logs.Flags().Int("try", 0, "attempt number (default: latest)")
	logs.Flags().BoolP("follow", "f", false, "follow the log")
	logs.Flags().Int("map-index", -1, "map index of a mapped task")

	render := &cobra.Command{
		Use: "render <dag_id> <task_id> <run_id>", Short: "Show the rendered template fields of a task instance", Args: cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			r, err := newStore().LoadRun(d.ID, args[2])
			if err != nil {
				exitf("%v", err)
			}
			idx, _ := cmd.Flags().GetInt("map-index")
			v, err := engine.RenderedFields(newStore(), d, r, args[1], idx)
			if err != nil {
				exitf("%v", err)
			}
			if v.Note != "" {
				fmt.Fprintln(os.Stderr, "#", v.Note)
			}
			if len(v.Fields) == 0 {
				fmt.Println("(no template fields)")
			}
			for _, f := range v.Fields {
				fmt.Printf("# ----------------------------------------------------------\n# property: %s\n# ----------------------------------------------------------\n", f.Name)
				if f.Error != "" {
					fmt.Println("ERROR:", f.Error)
					continue
				}
				fmt.Println(f.Value)
			}
		},
	}
	render.Flags().Int("map-index", -1, "map index of a mapped task")
	c.AddCommand(list, states, clear, test, logs, render)
	return c
}

// tailLog prints a task log (from files or the database); with follow it
// keeps printing new output until interrupted.
func tailLog(st *engine.Store, k engine.LogKey, follow bool) {
	printed := 0
	for {
		b, err := st.ReadTaskLog(k)
		if err != nil && !follow {
			exitf("no log for %s try %d: %v", k.TaskID, k.Try, err)
		}
		if len(b) > printed {
			os.Stdout.Write(b[printed:])
			printed = len(b)
		}
		if !follow {
			return
		}
		time.Sleep(time.Second)
	}
}

// memRun is an in-memory RunState for `tasks test`.
type memRun struct {
	xcom     map[string]map[string]any
	rendered []string
}

func (m *memRun) SetOutletExtra(_ string, _ int, uri string, extra map[string]any) error {
	b, _ := json.Marshal(extra)
	m.rendered = append(m.rendered, fmt.Sprintf("--- dataset extra %s ---\n%s", uri, b))
	return nil
}

func (m *memRun) SetRendered(taskID string, _ int, field, value string) error {
	m.rendered = append(m.rendered, fmt.Sprintf("--- %s ---\n%s", field, value))
	return nil
}

func (m *memRun) ExternalTaskState(dagID, taskID string, _ time.Time) (dag.State, bool, error) {
	return "", false, fmt.Errorf("external task lookup is not supported in `tasks test`")
}

func (m *memRun) XComPush(taskID string, _ int, key string, v any) error {
	if m.xcom[taskID] == nil {
		m.xcom[taskID] = map[string]any{}
	}
	m.xcom[taskID][key] = v
	return nil
}

func (m *memRun) XComPull(taskID, key string) (any, bool) {
	if taskID != "" {
		v, ok := m.xcom[taskID][key]
		return v, ok
	}
	for _, kv := range m.xcom {
		if v, ok := kv[key]; ok {
			return v, true
		}
	}
	return nil, false
}

func (m *memRun) TriggerDagRun(context.Context, string, map[string]any, bool) (string, error) {
	return "", fmt.Errorf("triggering other DAGs is not supported in `tasks test`; use `dags test`")
}
