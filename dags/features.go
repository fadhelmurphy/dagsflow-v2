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
const salesDataset = "bq://fadhel-project.dw.sales"

// salesInfo is the extra that sales_ingest attaches to the dataset event.
type salesInfo struct {
	Rows     int `json:"rows"`
	Branches int `json:"branches"`
}

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

	// Typed TaskFlow style: return values are outputs, passing an output to
	// the next task wires the dependency and hands over the value.
	branches := dag.Fn(producer, "list_branches", func(ctx *dag.TaskContext) ([]string, error) {
		return []string{"jakarta", "bandung", "surabaya"}, nil
	})

	// dynamic task mapping: one instance per branch, limited by pool "bigquery"
	rows := dag.Map(producer, "load_branch", branches, func(ctx *dag.TaskContext, branch string) (int, error) {
		ctx.Logf("loading %s for %s", branch, ctx.LogicalDate.Format("2006-01-02 15:04"))
		time.Sleep(300 * time.Millisecond)
		return len(branch) * 100, nil
	}, dag.WithPool("bigquery"), dag.WithPriorityWeight(5), dag.WithSLA(30*time.Minute))

	publish := dag.Consume(producer, "publish", rows, func(ctx *dag.TaskContext, rows []int) error {
		total := 0
		for _, r := range rows {
			total += r
		}
		ctx.Logf("published %d rows from %d branches", total, len(rows))
		// metadata for consumers of the dataset (Airflow: outlet_events[...].extra)
		return ctx.SetOutletExtra(salesDataset, map[string]any{"rows": total, "branches": len(rows)})
	}, dag.WithOutlets(salesDataset))

	// data dependencies above are wired automatically; add ordering-only ones:
	dag.Chain(waitFile, branches) // wait_for_upstream_window >> list_branches
	_ = publish
	dag.Register(producer)

	// runs whenever bq://fadhel-project.dw.sales is updated
	consumer := dag.NewDAG("sales_report", "",
		dag.WithDatasetSchedule(salesDataset),
		dag.WithTags("example", "datasets"))
	consumer.NewTask("build_report", func(ctx *dag.TaskContext) error {
		// extra of the latest event, decoded into a struct
		info, ok, err := dag.DatasetExtraAs[salesInfo](ctx, salesDataset)
		if err != nil {
			return err
		}
		if !ok {
			ctx.Logf("not triggered by %s (manual run?): nothing to report", salesDataset)
			return nil
		}
		ctx.Logf("report: %d rows from %d branches", info.Rows, info.Branches)

		// all events since the previous report (the producer may have run several times)
		events, err := ctx.TriggeringDatasetEvents()
		if err != nil {
			return err
		}
		for _, ev := range events[salesDataset] {
			fmt.Fprintf(ctx.LogWriter(), "  %s by %s.%s: %v\n", ev.Timestamp.Format(time.DateTime), ev.SourceDagID, ev.SourceTask, ev.Extra)
		}
		return nil
	})
	dag.Register(consumer)
}
