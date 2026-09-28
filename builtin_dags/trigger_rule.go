package builtindags

import (
	"errors"

	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("trigger_rule_dag", "@every 5m")

	jobA := d.NewTask("job_a", func(ctx *dag.TaskContext) error {
		ctx.Logf("Job A running (success)")
		return nil
	})
	jobB := d.NewTask("job_b", func(ctx *dag.TaskContext) error {
		return errors.New("simulasi error di Job B")
	})

	logf := func(msg string) dag.TaskFunc {
		return func(ctx *dag.TaskContext) error { ctx.Logf(msg); return nil }
	}

	// all_success: A -> runs, B -> upstream_failed
	jobA.Then(d.NewTask("job_a_all_success", logf("upstream sukses")))
	jobB.Then(d.NewTask("job_b_all_success", logf("tidak akan jalan")))

	// all_failed: A -> skipped, B -> runs
	jobA.Then(d.NewTask("job_a_all_failed", logf("tidak akan jalan"), dag.WithTriggerRule(dag.AllFailed)))
	jobB.Then(d.NewTask("job_b_all_failed", logf("upstream gagal semua"), dag.WithTriggerRule(dag.AllFailed)))

	// all_done: runs after upstream finishes, whatever the result
	jobA.Then(d.NewTask("job_a_all_done", logf("upstream selesai"), dag.WithTriggerRule(dag.AllDone)))
	jobB.Then(d.NewTask("job_b_all_done", logf("upstream selesai"), dag.WithTriggerRule(dag.AllDone)))

	dag.Register(d)
}
