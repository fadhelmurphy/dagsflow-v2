package builtindags

import (
	"time"

	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("custom_dag", "@every 5m",
		dag.WithParams(map[string]any{
			"threshold":  99,
			"message":    "Hello from config",
			"project_id": "fadhel-project",
		}),
		dag.WithDefaultArgs(dag.DefaultArgs{Retries: 1, RetryDelay: 10 * time.Second, Timeout: 15 * time.Minute}),
		dag.WithMaxActiveRuns(1),
		dag.WithTags("example"),
	)

	start := d.NewTask("start", func(ctx *dag.TaskContext) error {
		threshold, err := dag.ParamAs[int](ctx, "threshold")
		if err != nil {
			return err
		}
		ctx.Logf("start: threshold=%d, logical_date=%s", threshold, ctx.LogicalDate.Format(time.RFC3339))
		if err := ctx.XComPush("val", threshold); err != nil {
			return err
		}
		return ctx.XComPush("project_id", ctx.Params["project_id"])
	})

	branch := d.NewBranchTask("branch", func(ctx *dag.TaskContext) ([]string, error) {
		val, err := dag.XComPullAs[int](ctx, "start", "val")
		if err != nil {
			return nil, err
		}
		if val > 50 {
			return []string{"print_A"}, nil
		}
		return []string{"print_B"}, nil
	})

	printA := d.NewTask("print_A", func(ctx *dag.TaskContext) error { ctx.Logf("Print A"); return nil })
	printB := d.NewTask("print_B", func(ctx *dag.TaskContext) error { ctx.Logf("Print B"); return nil })
	printC := d.NewTask("print_C", func(ctx *dag.TaskContext) error { ctx.Logf("Print C"); return nil })
	finishA := d.NewTask("finishA", func(ctx *dag.TaskContext) error { ctx.Logf("Finish job A"); return nil })
	finishB := d.NewTask("finishB", func(ctx *dag.TaskContext) error { ctx.Logf("Finish job B"); return nil })

	bqJob := d.NewBigQueryTask("bq_task_1", "dags/sql/dw_to_tmp.sql", map[string]any{
		"project_id":     `{{ xcom_pull "start" "project_id" }}`,
		"target_dataset": "dw_ext_data",
		"target_table":   "kemdikbud_ref_satpen_npsn",
	}, dag.WithRetries(0, 0))

	// TriggerDagRunOperator: blocking (wait_for_completion=True) and fire-and-forget
	triggerDag1 := d.NewTriggerDagRunTask("trigger_dag1", "dag1",
		map[string]any{"param1": "{{ .ds }}", "param2": 42}, true)
	triggerBranch := d.NewTriggerDagRunTask("trigger_dag_branch", "dag_branch",
		map[string]any{"val": 10}, false)

	start.Then(branch)
	branch.ThenAll(printA, printB)
	printA.Then(printC).Then(finishA)
	printB.Then(finishB)
	finishA.Then(bqJob).Then(triggerDag1).Then(triggerBranch)

	dag.Register(d)
}
