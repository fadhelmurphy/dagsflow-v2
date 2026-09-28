package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"dagsflow-go/dag"
	"dagsflow-go/engine"

	"github.com/spf13/cobra"
)

func dagsCmd() *cobra.Command {
	c := &cobra.Command{Use: "dags", Short: "Manage DAGs (list, trigger, test, pause, backfill, ...)"}

	list := &cobra.Command{
		Use: "list", Short: "List DAGs",
		Run: func(cmd *cobra.Command, args []string) {
			st := newStore()
			w := table()
			fmt.Fprintln(w, "DAG_ID\tSCHEDULE\tPAUSED\tNEXT_INTERVAL_START\tLAST_RUN\tLAST_STATE")
			for _, d := range dag.List() {
				m := st.GetDagMeta(d.ID)
				last, lastState := "-", "-"
				if runs, _ := st.ListRuns(d.ID); len(runs) > 0 {
					r := runs[len(runs)-1]
					last, lastState = r.RunID, r.State
				}
				sch := d.Schedule
				if d.IsDatasetScheduled() {
					sch = "datasets"
				} else if sch == "" {
					sch = "None"
				}
				fmt.Fprintf(w, "%s\t%s\t%v\t%s\t%s\t%s\n", d.ID, sch, m.IsPaused, fmtTime(m.NextDataIntervalStart), last, lastState)
			}
			w.Flush()
			for id, err := range dag.ImportErrors() {
				fmt.Printf("\nIMPORT ERROR %s: %v\n", id, err)
			}
		},
	}

	setPaused := func(paused bool) func(*cobra.Command, []string) {
		return func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			st := newStore()
			m := st.GetDagMeta(d.ID)
			m.IsPaused = paused
			if err := st.SaveDagMeta(m); err != nil {
				exitf("%v", err)
			}
			fmt.Printf("dag %s paused=%v\n", d.ID, paused)
		}
	}
	pause := &cobra.Command{Use: "pause <dag_id>", Short: "Pause a DAG", Args: cobra.ExactArgs(1), Run: setPaused(true)}
	unpause := &cobra.Command{Use: "unpause <dag_id>", Short: "Unpause a DAG", Args: cobra.ExactArgs(1), Run: setPaused(false)}

	trigger := &cobra.Command{
		Use: "trigger <dag_id>", Short: "Queue a manual DagRun (executed by the scheduler)", Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			conf, _ := cmd.Flags().GetString("conf")
			runID, _ := cmd.Flags().GetString("run-id")
			st := newStore()
			now := time.Now().UTC()
			r := engine.NewDagRun(d, engine.RunTypeManual, runID, now, now, now, parseConf(conf))
			if st.RunExists(d.ID, r.RunID) {
				exitf("run %s already exists", r.RunID)
			}
			if err := st.SaveRun(r); err != nil {
				exitf("%v", err)
			}
			fmt.Printf("queued %s/%s\n", d.ID, r.RunID)
			if _, alive := engine.SchedulerInfo(st); !alive {
				fmt.Println("note: scheduler is not running; start it with `dagsflow-go scheduler -D`")
			} else if st.GetDagMeta(d.ID).IsPaused {
				fmt.Println("note: dag is paused; the run stays queued until you unpause it")
			}
		},
	}
	trigger.Flags().String("conf", "", `JSON conf, e.g. '{"param2": 42}'`)
	trigger.Flags().String("run-id", "", "custom run id")

	test := &cobra.Command{
		Use: "test <dag_id>", Short: "Run one DagRun now in the foreground (no scheduler needed)", Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			conf, _ := cmd.Flags().GetString("conf")
			ld, _ := cmd.Flags().GetString("logical-date")
			logical := time.Now().UTC()
			if ld != "" {
				t, err := time.Parse("2006-01-02", ld)
				if err != nil {
					exitf("--logical-date must be YYYY-MM-DD")
				}
				logical = t
			}
			loadConnections()
			st := newStore()
			r := engine.NewDagRun(d, engine.RunTypeManual, "", logical, logical, logical, parseConf(conf))
			r.RunID = "test__" + time.Now().UTC().Format(time.RFC3339Nano)
			if err := st.TakeOwnership(r); err != nil { // so a live scheduler won't also pick it up
				exitf("%v", err)
			}
			runAndReport(d, st, r)
		},
	}
	test.Flags().String("conf", "", "JSON conf")
	test.Flags().String("logical-date", "", "logical date YYYY-MM-DD (default now)")

	graph := &cobra.Command{
		Use: "graph <dag_id>", Short: "Print the DAG graph (optionally with states of a run)", Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			var states map[string]dag.State
			if runID, _ := cmd.Flags().GetString("run-id"); runID != "" {
				r, err := newStore().LoadRun(d.ID, runID)
				if err != nil {
					exitf("%v", err)
				}
				states = map[string]dag.State{}
				for id, ti := range r.Tasks {
					states[id] = ti.State
				}
			}
			d.PrintGraph(os.Stdout, states)
		},
	}
	graph.Flags().String("run-id", "", "annotate with task states of this run")

	listRuns := &cobra.Command{
		Use: "list-runs <dag_id>", Short: "List DagRuns", Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			runs, err := newStore().ListRuns(d.ID)
			if err != nil {
				exitf("%v", err)
			}
			limit, _ := cmd.Flags().GetInt("limit")
			if limit > 0 && len(runs) > limit {
				runs = runs[len(runs)-limit:]
			}
			w := table()
			fmt.Fprintln(w, "RUN_ID\tTYPE\tSTATE\tLOGICAL_DATE\tSTART\tEND")
			for _, r := range runs {
				ld := r.LogicalDate
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", r.RunID, r.RunType, r.State, fmtTime(&ld), fmtTime(r.StartDate), fmtTime(r.EndDate))
			}
			w.Flush()
		},
	}
	listRuns.Flags().Int("limit", 20, "show last N runs (0 = all)")

	backfill := &cobra.Command{
		Use: "backfill <dag_id>", Short: "Run the DAG for every interval between --start and --end (locally)", Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			if d.IsManualOnly() {
				exitf("dag %s has no schedule", d.ID)
			}
			s, _ := cmd.Flags().GetString("start")
			e, _ := cmd.Flags().GetString("end")
			rerunFailed, _ := cmd.Flags().GetBool("rerun-failed")
			start, err1 := time.ParseInLocation("2006-01-02", s, time.Local)
			end, err2 := time.ParseInLocation("2006-01-02", e, time.Local)
			if err1 != nil || err2 != nil {
				exitf("--start and --end must be YYYY-MM-DD")
			}
			loadConnections()
			st := newStore()
			sch := d.CronSchedule()
			endExcl := end.AddDate(0, 0, 1) // --end is inclusive (whole day)
			for iv := sch.Next(start.Add(-time.Nanosecond)); iv.Before(endExcl); iv = sch.Next(iv) {
				runID := "backfill__" + iv.UTC().Format(time.RFC3339)
				r, err := st.LoadRun(d.ID, runID)
				switch {
				case err != nil:
					r = engine.NewDagRun(d, engine.RunTypeBackfill, runID, iv, iv, sch.Next(iv), nil)
				case r.State == engine.RunSuccess:
					fmt.Printf("skip %s (already success)\n", runID)
					continue
				case rerunFailed:
					if _, err := engine.ClearTasks(d, r, nil, false, false, true); err != nil {
						exitf("%v", err)
					}
				default:
					fmt.Printf("skip %s (state %s, use --rerun-failed)\n", runID, r.State)
					continue
				}
				if err := st.TakeOwnership(r); err != nil {
					exitf("%v", err)
				}
				if !runAndReport(d, st, r) {
					return
				}
			}
		},
	}
	backfill.Flags().String("start", "", "start date YYYY-MM-DD")
	backfill.Flags().String("end", "", "end date YYYY-MM-DD (inclusive)")
	backfill.Flags().Bool("rerun-failed", false, "clear and rerun failed tasks of existing backfill runs")
	_ = backfill.MarkFlagRequired("start")
	_ = backfill.MarkFlagRequired("end")

	reparse := &cobra.Command{
		Use: "reparse <dag_id>", Short: "Reload the DAG's file now (scheduler/webservers pick it up within seconds)", Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			d := mustDAG(args[0])
			if err := reparseDag(d); err != nil {
				exitf("%v", err)
			}
			nd, ok := dag.Get(d.ID)
			if !ok {
				exitf("%s no longer loads: %v", d.ID, dag.ImportErrors()[d.Source])
			}
			newStore().Audit(engine.AuditEvent{User: "cli", Event: "reparse", DagID: d.ID, Extra: d.Source})
			fmt.Printf("reparsed %s (%s) at %s\n", d.ID, d.Source, nd.ParsedAt.Format(time.DateTime))
		},
	}
	c.AddCommand(list, pause, unpause, trigger, test, graph, listRuns, backfill, reparse)
	return c
}

// runAndReport executes a run in this process and prints the task states.
// Returns false if interrupted.
func runAndReport(d *dag.DAG, st *engine.Store, r *engine.DagRun) bool {
	ctx, cancel := signalContext()
	defer cancel()
	e := engine.NewEngine(st, parallelism)
	fmt.Printf("▶ %s/%s\n", d.ID, r.RunID)
	_ = e.Execute(ctx, r)
	e.Wait() // runs triggered by tasks (non-blocking) in this process + callbacks
	printStates(d, r)
	return ctx.Err() == nil
}

func printStates(d *dag.DAG, r *engine.DagRun) {
	w := table()
	fmt.Fprintln(w, "TASK_ID\tSTATE\tTRY\tSTART\tEND\tERROR")
	for _, t := range d.Tasks() {
		ti := r.Tasks[t.ID]
		if ti == nil {
			continue
		}
		msg := ti.Error
		if i := strings.IndexByte(msg, '\n'); i >= 0 {
			msg = msg[:i] + " …"
		}
		id := t.ID
		if kids := r.Mapped[t.ID]; len(kids) > 0 {
			id = fmt.Sprintf("%s [%d mapped]", t.ID, len(kids))
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\n", id, ti.State, ti.TryNumber, fmtTime(ti.StartDate), fmtTime(ti.EndDate), msg)
	}
	w.Flush()
	for _, m := range r.SLAMisses {
		fmt.Printf("SLA MISS: %s (deadline %s)\n", m.TaskID, m.Deadline.Local().Format(time.DateTime))
	}
	fmt.Printf("DagRun %s → %s\n", r.RunID, r.State)
}
