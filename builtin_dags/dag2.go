package builtindags

import (
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("dag2", "*/2 * * * *")

	x := d.NewTask("x", func(ctx *dag.TaskContext) error {
		ctx.Logf("[DAG2] Run X for interval %s → %s", ctx.DataIntervalStart, ctx.DataIntervalEnd)
		return ctx.XComPush("key", "value")
	})
	y := d.NewTask("y", func(ctx *dag.TaskContext) error {
		ctx.Logf("%v << xcom from x", ctx.XComPull("x", "key"))
		return nil
	})

	x.Then(y)
	dag.Register(d)
}
