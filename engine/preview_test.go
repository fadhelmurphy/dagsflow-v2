package engine

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"
)

const previewYAML = `dag_id: pv
tasks:
  - id: src
    operator: bash
    command: echo '[{"a":1},{"a":2}]'
  - id: shape
    operator: transform
    upstream: [src]
    steps:
      - {op: filter, field: a, cmp: ">", value: 1}
`

func TestRunPreviewLocal(t *testing.T) {
	// src is not "safe": without its output the preview asks for it
	r := RunPreview(context.Background(), PreviewRequest{DagID: "pv", YAML: []byte(previewYAML), Node: "shape"})
	if r.Status != "needs_input" || !strings.Contains(r.Error, "src has no output yet") {
		t.Fatalf("%+v", r)
	}
	// marked safe, it runs first
	r = RunPreview(context.Background(), PreviewRequest{DagID: "pv", YAML: []byte(previewYAML), Node: "shape", Safe: []string{"src"}})
	rows, _ := r.Output.([]any)
	if r.Status != "success" || len(rows) != 1 || len(r.Ran) != 1 {
		t.Fatalf("%+v", r)
	}
	// seeded output is used as is
	r = RunPreview(context.Background(), PreviewRequest{DagID: "pv", YAML: []byte(previewYAML), Node: "shape",
		Seed: map[string]map[string]any{"src": {"return_value": []any{map[string]any{"a": 5.0}, map[string]any{"a": 9.0}}}}})
	if rows, _ := r.Output.([]any); r.Status != "success" || len(rows) != 2 || len(r.Ran) != 0 {
		t.Fatalf("%+v", r)
	}
	// timeout
	slow := "dag_id: s\ntasks:\n  - {id: z, operator: bash, command: 'sleep 5'}\n"
	start := time.Now()
	r = RunPreview(context.Background(), PreviewRequest{DagID: "s", YAML: []byte(slow), Node: "z", TimeoutMS: 300})
	if r.Status != "failed" || !strings.Contains(r.Error, "preview limit") || time.Since(start) > 3*time.Second {
		t.Fatalf("%+v after %s", r, time.Since(start))
	}
}

func TestPreviewOnWorkerPG(t *testing.T) {
	resetPG(t)
	st := pgStore(t)
	if st.PreviewWorkers("default") {
		t.Fatal("no worker yet")
	}
	wst := pgStore(t)
	w := &Worker{Store: wst, Queues: []string{"default"}, Concurrency: 2, Log: log.New(io.Discard, "", 0)}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	defer func() { stop(); <-done }()
	deadline := time.Now().Add(10 * time.Second)
	for !st.PreviewWorkers("default") {
		if time.Now().After(deadline) {
			t.Fatal("worker not seen")
		}
		time.Sleep(100 * time.Millisecond)
	}
	r, err := st.RunPreviewOnWorker(context.Background(), "default", PreviewRequest{DagID: "pv", YAML: []byte(previewYAML),
		Node: "shape", Safe: []string{"src"}, TimeoutMS: 20000})
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := r.Output.([]any)
	if r.Status != "success" || len(rows) != 1 || r.Where != wst.ID {
		t.Fatalf("%+v (worker %s)", r, wst.ID)
	}
	if runs, _ := st.ListRuns("pv"); len(runs) != 0 {
		t.Fatal("a preview must not create runs")
	}
}
