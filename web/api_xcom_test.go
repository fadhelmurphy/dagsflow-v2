package web

import (
	"net/http"
	"testing"
	"time"

	"dagsflow-go/dag"
	"dagsflow-go/engine"
)

func TestRunXComPreview(t *testing.T) {
	b := newBld(t)
	d := dag.NewDAG("xprev", "")
	d.NewTask("read", func(*dag.TaskContext) error { return nil })
	dag.Register(d)
	r := engine.NewDagRun(d, "manual", "r1", time.Now(), time.Now(), time.Now(), nil)
	big := make([]any, 4000)
	for i := range big {
		big[i] = map[string]any{"id": float64(i), "name": "abcdefghijabcdefghij"}
	}
	r.XCom["read"] = map[string]any{"return_value": big, "n": float64(4000)}
	if _, err := b.st.CreateRun(r); err != nil {
		t.Fatal(err)
	}
	code, out := b.do("GET", "/dags/xprev/dagRuns/r1", nil)
	if code != 200 {
		t.Fatalf("%d %v", code, out)
	}
	rv := out["xcom"].(map[string]any)["read"].(map[string]any)["return_value"].(map[string]any)
	if rv["rows"] != float64(4000) || len(rv["sample"].([]any)) != 50 || rv["$xcom_ref"] == nil {
		t.Fatalf("preview: %v", rv["rows"])
	}
	if out["xcom"].(map[string]any)["read"].(map[string]any)["n"] != float64(4000) {
		t.Fatal("small values must stay")
	}
	req, _ := http.NewRequest("GET", b.srv.URL+"/api/v1/dags/xprev/dagRuns/r1/xcom?slot=read&key=return_value", nil)
	req.Header.Set("X-Dagsflow", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 || resp.ContentLength == 0 {
		t.Fatalf("full value: %v %v", resp, err)
	}
	resp.Body.Close()
	if code, _ := b.do("GET", "/dags/xprev/dagRuns/r1/xcom?slot=nope", nil); code != 404 {
		t.Fatalf("missing: %d", code)
	}
}
