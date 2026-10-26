package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"dagsflow-go/dag"
)

func TestPGXComBlob(t *testing.T) {
	t.Setenv("DAGSFLOW_XCOM_INLINE_BYTES", "")
	resetPG(t)
	st := pgStore(t)
	pg := st.b.(*pgBackend)
	ctx := context.Background()
	d := dag.NewDAG("blob_dag", "")
	d.NewTask("read", func(*dag.TaskContext) error { return nil })
	r := NewDagRun(d, "manual", "r1", time.Now(), time.Now(), time.Now(), nil)
	big := make([]any, 5000)
	for i := range big {
		big[i] = map[string]any{"id": float64(i), "name": strings.Repeat("x", 20)}
	}
	r.XCom["read"] = map[string]any{"return_value": big, "rows_affected": float64(5000)}
	if _, err := st.CreateRun(r); err != nil {
		t.Fatal(err)
	}
	var docLen, blobs int
	_ = pg.pool.QueryRow(ctx, `SELECT length(doc::text) FROM dag_run WHERE dag_id='blob_dag'`).Scan(&docLen)
	_ = pg.pool.QueryRow(ctx, `SELECT count(*) FROM xcom_blob WHERE dag_id='blob_dag'`).Scan(&blobs)
	if docLen > 10000 || blobs != 1 {
		t.Fatalf("row %d bytes, %d blobs", docLen, blobs)
	}
	// lists keep the reference; a run load / claim gets the value
	runs, _ := st.ListRuns("blob_dag")
	if !IsXComRef(runs[0].XCom["read"]["return_value"]) || runs[0].XCom["read"]["rows_affected"] != float64(5000) {
		t.Fatalf("list: %v", runs[0].XCom["read"]["rows_affected"])
	}
	got, err := st.LoadRun("blob_dag", "r1")
	if err != nil || len(got.XCom["read"]["return_value"].([]any)) != 5000 {
		t.Fatalf("load: %v", err)
	}
	claimed, err := st.ClaimRun("blob_dag", "r1", 0)
	if err != nil || claimed == nil || len(claimed.XCom["read"]["return_value"].([]any)) != 5000 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	// saving a run read from a list keeps the blob
	runs, _ = st.ListRuns("blob_dag")
	runs[0].Note = "hi"
	runs[0].owned = false
	_ = pg.pool.QueryRow(ctx, `UPDATE dag_run SET owner=NULL, lease_until=NULL WHERE dag_id='blob_dag' RETURNING version`).Scan(&runs[0].version)
	if err := st.SaveRun(runs[0]); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.LoadRun("blob_dag", "r1"); len(got.XCom["read"]["return_value"].([]any)) != 5000 || got.Note != "hi" {
		t.Fatal("blob lost after saving a listed run")
	}
	// clearing the value removes the blob
	got, _ = st.LoadRun("blob_dag", "r1")
	delete(got.XCom, "read")
	if err := st.SaveRun(got); err != nil {
		t.Fatal(err)
	}
	_ = pg.pool.QueryRow(ctx, `SELECT count(*) FROM xcom_blob WHERE dag_id='blob_dag'`).Scan(&blobs)
	if blobs != 0 {
		t.Fatalf("stale blobs: %d", blobs)
	}
	// deleting the run removes its blobs
	got.XCom["read"] = map[string]any{"return_value": big}
	if err := st.SaveRun(got); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteRun("blob_dag", "r1"); err != nil {
		t.Fatal(err)
	}
	_ = pg.pool.QueryRow(ctx, `SELECT count(*) FROM xcom_blob WHERE dag_id='blob_dag'`).Scan(&blobs)
	if blobs != 0 {
		t.Fatalf("blobs after delete: %d", blobs)
	}
}

func TestPGCompactXCom(t *testing.T) {
	resetPG(t)
	st := pgStore(t)
	pg := st.b.(*pgBackend)
	d := dag.NewDAG("compact_dag", "")
	d.NewTask("read", func(*dag.TaskContext) error { return nil })
	big := make([]any, 5000)
	for i := range big {
		big[i] = map[string]any{"id": float64(i), "name": strings.Repeat("y", 20)}
	}
	t.Setenv("DAGSFLOW_XCOM_INLINE_BYTES", "1000000000") // an old binary: everything inline
	r := NewDagRun(d, "manual", "old", time.Now(), time.Now(), time.Now(), nil)
	r.State = RunSuccess
	r.XCom["read"] = map[string]any{"return_value": big}
	if _, err := st.CreateRun(r); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DAGSFLOW_XCOM_INLINE_BYTES", "")
	n, err := st.CompactXCom(t.Logf)
	if err != nil || n != 1 {
		t.Fatalf("compacted %d %v", n, err)
	}
	var docLen int
	_ = pg.pool.QueryRow(context.Background(), `SELECT length(doc::text) FROM dag_run WHERE dag_id='compact_dag'`).Scan(&docLen)
	if docLen > 10000 {
		t.Fatalf("row still %d bytes", docLen)
	}
	if got, _ := st.LoadRun("compact_dag", "old"); len(got.XCom["read"]["return_value"].([]any)) != 5000 {
		t.Fatal("value lost")
	}
	if n, _ := st.CompactXCom(t.Logf); n != 0 {
		t.Fatalf("second pass rewrote %d", n)
	}
}
