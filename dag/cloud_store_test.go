package dag

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEncodeRows(t *testing.T) {
	rows := []any{
		map[string]any{"id": float64(1), "name": "a,b", "meta": map[string]any{"k": 1.0}, "note": nil},
		map[string]any{"id": float64(2), "name": "say \"hi\"", "extra": true},
	}
	b, ct, ext, err := encodeRows(rows, "", nil, "", false)
	if err != nil || ct != "text/csv" || ext != ".csv" {
		t.Fatal(err, ct, ext)
	}
	want := "extra,id,meta,name,note\n,1,\"{\"\"k\"\":1}\",\"a,b\",\n" + "true,2,,\"say \"\"hi\"\"\",\n"
	if string(b) != want {
		t.Fatalf("csv:\n%q\nwant\n%q", b, want)
	}
	b, _, _, _ = encodeRows(rows, "csv", []string{"name", "id"}, "semicolon", false)
	if string(b) != "name;id\na,b;1\nsay \"hi\";2\n" && !strings.HasPrefix(string(b), "name;id\n") {
		t.Fatalf("columns: %q", b)
	}
	b, ct, ext, _ = encodeRows(rows, "ndjson", nil, "", false)
	if ct != "application/x-ndjson" || ext != ".ndjson" || strings.Count(string(b), "\n") != 2 {
		t.Fatalf("ndjson: %q", b)
	}
	b, _, ext, _ = encodeRows(rows, "json", nil, "", true)
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil || ext != ".json.gz" {
		t.Fatal(err, ext)
	}
	plain, _ := io.ReadAll(zr)
	if !strings.HasPrefix(string(plain), `[{"id":1`) {
		t.Fatalf("gzip json: %s", plain)
	}
	if _, _, _, err := encodeRows(rows, "parquet", nil, "", false); err == nil {
		t.Fatal("unknown format")
	}
	if _, _, _, err := encodeRows([]any{"x"}, "csv", nil, "", false); err == nil {
		t.Fatal("rows must be objects")
	}
}

// The examples published in the AWS Signature Version 4 documentation.
func TestSigV4AWSVectors(t *testing.T) {
	cr := awsCreds{Key: "AKIAIOSFODNN7EXAMPLE", Secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
	now := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	empty := sha256Hex(nil)
	got := signV4("GET", "/test.txt", "", map[string]string{"host": "examplebucket.s3.amazonaws.com", "range": "bytes=0-9",
		"x-amz-content-sha256": empty, "x-amz-date": "20130524T000000Z"}, empty, "us-east-1", "s3", cr, now)
	if !strings.HasSuffix(got, "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41") {
		t.Fatalf("GET vector: %s", got)
	}
	ph := sha256Hex([]byte("Welcome to Amazon S3."))
	got = signV4("PUT", "/test$file.text", "", map[string]string{"date": "Fri, 24 May 2013 00:00:00 GMT", "host": "examplebucket.s3.amazonaws.com",
		"x-amz-content-sha256": ph, "x-amz-date": "20130524T000000Z", "x-amz-storage-class": "REDUCED_REDUNDANCY"}, ph, "us-east-1", "s3", cr, now)
	if !strings.HasSuffix(got, "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd") {
		t.Fatalf("PUT vector: %s", got)
	}
}

func TestGCSWrite(t *testing.T) {
	f := setupGoogle(t)
	UploadsDir = t.TempDir()
	t.Cleanup(func() { UploadsDir = "" })
	_ = os.WriteFile(filepath.Join(UploadsDir, "u.csv"), []byte("User ID,Full Name\n1,ana\n2,bob\n"), 0o600)
	yml := `dag_id: g
tasks:
  - id: src
    operator: file_read
    file: u.csv
  - id: dump
    operator: gcs_write
    bucket: bkt
    object: exports/{{ .ds }}/users
    gzip: true
    upstream: [src]
`
	dags, err := ParseYAML("g.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs)
	name := "exports/2026-01-31/users.csv.gz"
	data, ok := f.objects[name]
	if !ok || f.octypes[name] != "application/gzip" {
		t.Fatalf("objects: %v %v", f.objects, f.octypes)
	}
	zr, _ := gzip.NewReader(bytes.NewReader(data))
	plain, _ := io.ReadAll(zr)
	if string(plain) != "full_name,user_id\nana,1\nbob,2\n" {
		t.Fatalf("content: %q", plain)
	}
	v, _ := rs.XComPull("dump", "return_value")
	if m := v.(map[string]any); m["uri"] != "gs://bkt/"+name || m["rows"] != float64(2) {
		t.Fatalf("output: %v", v)
	}
}

func TestS3Write(t *testing.T) {
	var mu sync.Mutex
	got := map[string][]byte{}
	var hdrs http.Header
	cr := awsCreds{Key: "AKID", Secret: "SECRET", Token: "TOK"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// recompute the signature from the request exactly like a server would
		date := r.Header.Get("X-Amz-Date")
		ts, _ := time.Parse("20060102T150405Z", date)
		signed := map[string]string{}
		for _, h := range strings.Split(strings.Split(strings.Split(r.Header.Get("Authorization"), "SignedHeaders=")[1], ",")[0], ";") {
			if h == "host" {
				signed[h] = r.Host
			} else {
				signed[h] = r.Header.Get(h)
			}
		}
		want := signV4(r.Method, r.URL.Path, r.URL.RawQuery, signed, sha256Hex(body), "eu-west-1", "s3", cr, ts)
		mu.Lock()
		defer mu.Unlock()
		hdrs = r.Header
		if r.Header.Get("Authorization") != want || r.Header.Get("X-Amz-Content-Sha256") != sha256Hex(body) {
			w.WriteHeader(403)
			_, _ = w.Write([]byte("<Error><Code>SignatureDoesNotMatch</Code><Message>bad signature</Message></Error>"))
			return
		}
		got[r.URL.Path] = body
	}))
	defer srv.Close()
	t.Setenv("AWS_ACCESS_KEY_ID", cr.Key)
	t.Setenv("AWS_SECRET_ACCESS_KEY", cr.Secret)
	t.Setenv("AWS_SESSION_TOKEN", cr.Token)
	t.Setenv("AWS_ENDPOINT_URL_S3", srv.URL)

	d := NewDAG("s3", "")
	src := d.NewTask("src", func(c *TaskContext) error {
		return c.XComPush("return_value", []any{map[string]any{"id": 1.0, "name": "a b"}})
	})
	d.NewS3WriteTask("dump", StoreWriteConfig{Bucket: "my-bkt", Object: "out dir/{{ .ds }}/rows.ndjson", Region: "eu-west-1", Format: "ndjson"}).After(src)
	rs := &memRun{}
	runDAG(t, d, rs)
	body, ok := got["/my-bkt/out dir/2026-01-31/rows.ndjson"]
	if !ok || string(body) != "{\"id\":1,\"name\":\"a b\"}\n" {
		t.Fatalf("stored: %v", got)
	}
	if hdrs.Get("X-Amz-Security-Token") != "TOK" || hdrs.Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("headers: %v", hdrs)
	}

	// a rejected signature is reported with S3's own message
	t.Setenv("AWS_SECRET_ACCESS_KEY", "WRONG")
	d2 := NewDAG("s3b", "")
	s2 := d2.NewTask("src", func(c *TaskContext) error { return c.XComPush("return_value", []any{map[string]any{"a": 1.0}}) })
	d2.NewS3WriteTask("dump", StoreWriteConfig{Bucket: "b", Object: "x.csv", Region: "eu-west-1"}).After(s2)
	_ = d2.validate()
	runTask := func(id string) error {
		task, _ := d2.Task(id)
		ctx := NewTaskContext(t.Context(), task, RunInfo{RunID: "r", LogicalDate: time.Now(), MapIndex: -1}, rs, io.Discard)
		_, err := task.Run(ctx)
		return err
	}
	_ = runTask("src")
	if err := runTask("dump"); err == nil || !strings.Contains(err.Error(), "bad signature") {
		t.Fatalf("expected the signature error, got %v", err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	if err := runTask("dump"); err == nil || !strings.Contains(err.Error(), "no AWS credentials") {
		t.Fatalf("expected the credentials error, got %v", err)
	}
}

func TestStoreBuildErrors(t *testing.T) {
	for _, y := range []string{
		"dag_id: a\ntasks:\n  - {id: x, operator: gcs_write, object: o}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: gcs_write, bucket: b}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: s3_write, bucket: b}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: s3_write, bucket: b, key: k, format: parquet}\n",
	} {
		dd, err := ParseYAML("a.yaml", []byte(y))
		if err == nil {
			err = dd[0].validate()
		}
		if err == nil {
			t.Errorf("expected an error for %s", y)
		}
	}
	if u, _ := s3URL(awsCreds{Region: "eu-west-1"}, "my.dotted.bkt", "a/b.csv"); u.String() != "https://s3.eu-west-1.amazonaws.com/my.dotted.bkt/a/b.csv" {
		t.Fatal(u)
	}
	if u, _ := s3URL(awsCreds{Region: "eu-west-1"}, "bkt", "a/b.csv"); u.String() != "https://bkt.s3.eu-west-1.amazonaws.com/a/b.csv" {
		t.Fatal(u)
	}
}
