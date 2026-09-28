package builtindags

import (
	"errors"
	"time"

	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("dag1", "@every 1m",
		dag.WithParams(map[string]any{"param2": 0}),
		dag.WithDefaultArgs(dag.DefaultArgs{Retries: 2, RetryDelay: 2 * time.Second}),
	)

	a := d.NewTask("a", func(ctx *dag.TaskContext) error {
		ctx.Logf("[DAG1] Run A, try %d", ctx.TryNumber)
		if ctx.TryNumber == 1 {
			return errors.New("simulated flaky failure (will be retried)")
		}
		return ctx.XComPush("sebuah-key", "sebuah-nilai")
	})

	b := d.NewTask("b", func(ctx *dag.TaskContext) error {
		p2, err := dag.ParamAs[int](ctx, "param2") // from dag params or dag_run.conf
		if err != nil {
			return err
		}
		v, err := dag.XComPullAs[string](ctx, "a", "sebuah-key")
		if err != nil {
			return err
		}
		ctx.Logf("param2=%d, xcom from a=%s", p2, v)
		return nil
	})

	a.Then(b)
	dag.Register(d)
}
