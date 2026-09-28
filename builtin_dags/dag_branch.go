package builtindags

import (
	"dagsflow-go/dag"
)

func init() {
	// schedule "" = manual only (schedule=None); triggered from custom_dag
	d := dag.NewDAG("dag_branch", "", dag.WithParams(map[string]any{"val": 99}))

	start := d.NewTask("start", func(ctx *dag.TaskContext) error {
		val, err := dag.ParamAs[int](ctx, "val")
		if err != nil {
			return err
		}
		return ctx.XComPush("val", val)
	})

	branch := d.NewBranchTask("branch", func(ctx *dag.TaskContext) ([]string, error) {
		val, err := dag.XComPullAs[int](ctx, "start", "val")
		if err != nil {
			return nil, err
		}
		if val > 50 {
			return []string{"high"}, nil
		}
		return []string{"low"}, nil
	})

	high := d.NewTask("high", func(ctx *dag.TaskContext) error { ctx.Logf("Running HIGH path"); return nil })
	low := d.NewTask("low", func(ctx *dag.TaskContext) error { ctx.Logf("Running LOW path"); return nil })

	// join after a branch must tolerate the skipped path
	join := d.NewEmptyTask("join", dag.WithTriggerRule(dag.NoneFailedMinOneSuccess))

	start.Then(branch)
	branch.ThenAll(high, low)
	high.Then(join)
	low.Then(join)

	dag.Register(d)
}
