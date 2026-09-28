//go:build dagsflow

// Go DAG loaded from the DAG folder at runtime: the scheduler compiles it on
// the fly (no rebuild of dagsflow-go). The build tag only keeps `go build ./...`
// of the dagsflow-go repo from treating this file as part of the module.
package main

import (
	"fmt"
	"time"

	"dagsflow-go/dag"
)

// Showcases: pools, priority, dynamic task mapping, sensors, SLA,
// callbacks (Google Chat) and datasets (data-aware scheduling).
func init() {
	alert := dag.GoogleChatNotifier("gchat_alerts") // connection type google_chat, Config.webhook_url

	producer := dag.NewDAG("sales_ingest", "@hourly",
		dag.WithDefaultArgs(dag.DefaultArgs{
			Retries: 1, RetryDelay: 30 * time.Second, Pool: "default_pool",
			OnFailure: []dag.Callback{alert},
		}),
		dag.WithOnFailure(alert),
		dag.WithSLAMiss(alert),
		dag.WithTags("example", "datasets"),
	)

	// sensor in reschedule mode: frees its worker slot between pokes
	waitFile := producer.NewTimeDeltaSensor("wait_for_upstream_window", 0,
		dag.SensorConfig{PokeInterval: 2 * time.Second, Timeout: time.Minute, Mode: dag.ModeReschedule})

	list := producer.NewTask("list_branches", func(ctx *dag.TaskContext) error {
		return ctx.XComPush("return_value", []string{"jakarta", "bandung", "surabaya"})
	})

	// dynamic task mapping: one instance per branch, limited by pool "bigquery"
	load := producer.NewTask("load_branch", func(ctx *dag.TaskContext) error {
		branch, err := dag.ItemAs[string](ctx)
		if err != nil {
			return err
		}
		ctx.Logf("loading %s for %s", branch, ctx.LogicalDate.Format("2006-01-02 15:04"))
		time.Sleep(300 * time.Millisecond)
		return ctx.XComPush("rows", len(branch)*100)
	}, dag.Expand(dag.FromXCom("list_branches", "return_value")),
		dag.WithPool("bigquery"), dag.WithPriorityWeight(5), dag.WithSLA(30*time.Minute))

	publish := producer.NewTask("publish", func(ctx *dag.TaskContext) error {
		rows, err := dag.XComPullAs[[]int](ctx, "load_branch", "rows") // list, one per mapped instance
		if err != nil {
			return err
		}
		total := 0
		for _, r := range rows {
			total += r
		}
		ctx.Logf("published %d rows from %d branches", total, len(rows))
		return nil
	}, dag.WithOutlets("bq://fadhel-project.dw.sales"))

	waitFile.Then(list).Then(load).Then(publish)
	dag.Register(producer)

	// runs whenever bq://fadhel-project.dw.sales is updated
	consumer := dag.NewDAG("sales_report", "",
		dag.WithDatasetSchedule("bq://fadhel-project.dw.sales"),
		dag.WithTags("example", "datasets"))
	consumer.NewTask("build_report", func(ctx *dag.TaskContext) error {
		ctx.Logf("triggered by datasets: %v", ctx.Conf["triggering_dataset_events"] != nil)
		fmt.Fprintln(ctx.LogWriter(), "report built")
		return nil
	})
	dag.Register(consumer)
}
