package web

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"dagsflow-go/dag"
)

func (b *bld) upload(name, content string) (int, map[string]any) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("file", name)
	_, _ = fw.Write([]byte(content))
	_ = mw.Close()
	req, _ := http.NewRequest("POST", b.srv.URL+"/api/v1/builder/files", &buf)
	req.Header.Set("X-Dagsflow", "1")
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if b.user != "" {
		req.SetBasicAuth(b.user, "password123")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		b.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestBuilderFiles(t *testing.T) {
	b := newBld(t)
	dag.UploadsDir = filepath.Join(b.root, "builder", "files")
	t.Cleanup(func() { dag.UploadsDir = "" })
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(b.st.CreateUser("root", "password123", "Admin"))
	must(b.st.CreateUser("viewer", "password123", "Viewer"))

	b.user = "viewer"
	if c, _ := b.upload("a.csv", "x\n1\n"); c != 403 {
		t.Fatalf("viewer upload: %d", c)
	}
	if c, _ := b.do("GET", "/builder/files", nil); c != 403 {
		t.Fatalf("viewer list: %d", c)
	}

	b.user = "root"
	c, r := b.upload("Sales Jan.csv", "Order ID;Customer Name\n1;ana\n2;bob\n")
	if c != 201 || r["name"] != "Sales_Jan.csv" || r["format"] != "csv" {
		t.Fatalf("upload: %d %v", c, r)
	}
	if c, _ := b.upload("evil.exe", "MZ"); c != 400 {
		t.Fatalf("bad extension: %d", c)
	}
	if c, _ := b.upload("broken.xlsx", "this is not a zip"); c != 422 {
		t.Fatalf("broken xlsx: %d", c)
	}
	if _, err := os.Stat(filepath.Join(dag.UploadsDir, "broken.xlsx")); err == nil {
		t.Fatal("a file that cannot be read must not be kept")
	}
	c, r = b.do("GET", "/builder/files", nil)
	files, _ := r["files"].([]any)
	if c != 200 || len(files) != 1 || files[0].(map[string]any)["name"] != "Sales_Jan.csv" {
		t.Fatalf("list: %d %v", c, r)
	}
	c, r = b.do("GET", "/builder/files/Sales_Jan.csv/preview?range=A1", nil)
	cols, _ := r["columns"].([]any)
	if c != 200 || len(cols) != 2 || cols[0] != "order_id" || cols[1] != "customer_name" || r["row_count"] != float64(2) {
		t.Fatalf("preview: %d %v", c, r)
	}
	c, r = b.do("GET", "/builder/files/Sales_Jan.csv/preview?range=B1:B&keep_headers=1", nil)
	cols, _ = r["columns"].([]any)
	if len(cols) != 1 || cols[0] != "Customer Name" {
		t.Fatalf("preview keep headers: %v", r)
	}
	if c, r = b.do("GET", "/builder/files/Sales_Jan.csv/preview?range=A1:B2:C", nil); r["range_error"] == nil {
		t.Fatalf("bad range must be reported: %d %v", c, r)
	}
	for _, p := range []string{"/builder/files/..%2F..%2Fetc%2Fpasswd/preview", "/builder/files/nothere.csv/preview"} {
		if c, _ := b.do("GET", p, nil); c != 400 && c != 404 {
			t.Fatalf("%s: %d", p, c)
		}
	}
	if c, _ := b.do("DELETE", "/builder/files/Sales_Jan.csv", nil); c != 204 {
		t.Fatalf("delete: %d", c)
	}
	if c, _ := b.do("DELETE", "/builder/files/Sales_Jan.csv", nil); c != 404 {
		t.Fatalf("delete twice: %d", c)
	}

	// a saved workflow using the file validates and runs through the builder API
	c, _ = b.upload("u.csv", "User ID,Name\n1,ana\n")
	g := map[string]any{"dag_id": "file_wf", "settings": map[string]any{},
		"nodes": []any{map[string]any{"id": "f", "type": "file_read", "x": 0, "y": 0, "config": map[string]any{"file": "u.csv", "range": "A1"}},
			map[string]any{"id": "m", "type": "merge", "x": 300, "y": 0, "config": map[string]any{}}},
		"edges": []any{map[string]any{"from": "f", "to": "m"}}}
	if c, r := b.do("POST", "/builder/validate", g); c != 200 || r["ok"] == true {
		t.Fatalf("merge with one input must be flagged: %d %v", c, r)
	}
}
