package dag

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeGoogle serves the token endpoint (verifying the RS256 JWT), BigQuery and Sheets.
type fakeGoogle struct {
	mu      sync.Mutex
	t       *testing.T
	pub     *rsa.PublicKey
	tokens  int
	polled  bool
	inserts []any
	objects map[string][]byte
	octypes map[string]string
	sheet   [][]any
	calls   []string
	queries []string
}

func (f *fakeGoogle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	f.calls = append(f.calls, r.Method+" "+r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/token" {
		form, _ := url.ParseQuery(string(body))
		parts := strings.Split(form.Get("assertion"), ".")
		if len(parts) != 3 {
			http.Error(w, `{"error_description":"bad jwt"}`, 400)
			return
		}
		sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
		h := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
		if rsa.VerifyPKCS1v15(f.pub, crypto.SHA256, h[:], sig) != nil {
			http.Error(w, `{"error_description":"bad signature"}`, 400)
			return
		}
		f.tokens++
		_, _ = w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
		return
	}
	if r.Header.Get("Authorization") != "Bearer tok" {
		http.Error(w, `{"error":{"message":"unauthenticated"}}`, 401)
		return
	}
	p := r.URL.Path
	switch {
	case strings.HasSuffix(p, "/projects/prj/queries") && r.Method == "POST":
		var q struct{ Query string }
		_ = json.Unmarshal(body, &q)
		f.queries = append(f.queries, q.Query)
		if strings.Contains(q.Query, "INFORMATION_SCHEMA") { // table metadata for bigquery_load_table
			rows := ""
			if strings.Contains(q.Query, "table_name = 'ext'") {
				rows = `,"rows":[{"f":[{"v":"order_id"},{"v":"STRING"},{"v":"EXTERNAL"}]},{"f":[{"v":"status"},{"v":"STRING"},{"v":"EXTERNAL"}]}]`
			}
			_, _ = w.Write([]byte(`{"jobComplete":true,"jobReference":{"jobId":"js"},"schema":{"fields":[{"name":"column_name","type":"STRING"},{"name":"data_type","type":"STRING"},{"name":"table_type","type":"STRING"}]}` + rows + `}`))
			return
		}
		_, _ = w.Write([]byte(`{"jobComplete":false,"jobReference":{"jobId":"j1","location":"US"}}`))
	case strings.HasSuffix(p, "/queries/j1") && r.URL.Query().Get("pageToken") == "":
		f.polled = true
		_, _ = w.Write([]byte(`{"jobComplete":true,"jobReference":{"jobId":"j1"},"pageToken":"p2",
"schema":{"fields":[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"},{"name":"ok","type":"BOOLEAN"}]},
"rows":[{"f":[{"v":"1"},{"v":"ann"},{"v":"true"}]}]}`))
	case strings.HasSuffix(p, "/queries/j1"):
		_, _ = w.Write([]byte(`{"jobComplete":true,"rows":[{"f":[{"v":"2"},{"v":null},{"v":"false"}]}]}`))
	case r.Method == "DELETE" && strings.HasSuffix(p, "/projects/prj/datasets/stg/tables/old_20260131"):
		w.WriteHeader(204)
	case r.Method == "GET" && strings.Contains(p, "/datasets/") && strings.Contains(p, "/tables/"): // tables.get
		_, _ = w.Write([]byte(`{"schema":{"fields":[{"name":"order_id","type":"STRING"},{"name":"qty","type":"INTEGER"},{"name":"tags","type":"STRING","mode":"REPEATED"}]}}`))
	case strings.HasSuffix(p, "/tables/t/insertAll"):
		var in struct {
			Rows []struct {
				JSON any `json:"json"`
			} `json:"rows"`
		}
		_ = json.Unmarshal(body, &in)
		for _, x := range in.Rows {
			f.inserts = append(f.inserts, x.JSON)
		}
		_, _ = w.Write([]byte(`{}`))
	case strings.HasPrefix(p, "/upload/storage/v1/b/bkt/o"):
		if f.objects == nil {
			f.objects, f.octypes = map[string][]byte{}, map[string]string{}
		}
		name := r.URL.Query().Get("name")
		f.objects[name] = body
		f.octypes[name] = r.Header.Get("Content-Type")
		_, _ = w.Write([]byte(`{"name":"` + name + `","bucket":"bkt"}`))
	case strings.Contains(p, "/spreadsheets/SHEET/values/"):
		switch {
		case strings.HasSuffix(p, ":clear"):
			f.sheet = nil
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(p, ":append"):
			var in struct{ Values [][]any }
			_ = json.Unmarshal(body, &in)
			f.sheet = append(f.sheet, in.Values...)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == "PUT":
			var in struct{ Values [][]any }
			_ = json.Unmarshal(body, &in)
			f.sheet = in.Values
			_, _ = w.Write([]byte(`{}`))
		default:
			if strings.Contains(p, "A1:A1") {
				if len(f.sheet) == 0 {
					_, _ = w.Write([]byte(`{}`))
				} else {
					_, _ = w.Write([]byte(`{"values":[["x"]]}`))
				}
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"values": f.sheet})
		}
	default:
		http.Error(w, `{"error":{"message":"not found: `+p+`"}}`, 404)
	}
}

func setupGoogle(t *testing.T) *fakeGoogle {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeGoogle{t: t, pub: &key.PublicKey}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: mustPKCS8(t, key)})
	js, _ := json.Marshal(map[string]string{"type": "service_account", "client_email": "sa@prj.iam", "private_key": string(pemKey),
		"token_uri": srv.URL + "/token", "project_id": "prj"})
	path := filepath.Join(t.TempDir(), "sa.json")
	_ = os.WriteFile(path, js, 0o600)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
	ob, os2, om, og := bqBase, sheetsBase, metadataURL, gcsBase
	gcsBase = srv.URL
	bqBase, sheetsBase, metadataURL = srv.URL+"/bigquery/v2", srv.URL+"/v4/spreadsheets", srv.URL+"/nometa"
	tokMu.Lock()
	tokens = map[string]cachedToken{}
	tokMu.Unlock()
	t.Cleanup(func() { bqBase, sheetsBase, metadataURL, gcsBase = ob, os2, om, og })
	return f
}

func mustPKCS8(t *testing.T, k *rsa.PrivateKey) []byte {
	b, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBigQueryReadAndInsert(t *testing.T) {
	f := setupGoogle(t)
	yml := `dag_id: bq
tasks:
  - id: read
    operator: bigquery_query
    sql: SELECT id, name, ok FROM x
  - id: put
    operator: bigquery_insert
    dataset: ds
    table: t
    upstream: [read]
`
	// dataset "ds" path: fake matches only /tables/t/insertAll suffix
	dags, err := ParseYAML("bq.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs)
	v, _ := rs.XComPull("read", "return_value")
	rows, _ := v.([]any)
	if len(rows) != 2 || rows[0].(map[string]any)["id"] != float64(1) || rows[0].(map[string]any)["ok"] != true ||
		rows[1].(map[string]any)["name"] != nil || rows[1].(map[string]any)["ok"] != false {
		t.Fatalf("rows: %v", v)
	}
	if !f.polled || len(f.inserts) != 2 {
		t.Fatalf("polled=%v inserts=%v", f.polled, f.inserts)
	}
	if f.tokens != 1 {
		t.Fatalf("token should be cached, exchanged %d times", f.tokens)
	}
}

func TestSheetsReadWrite(t *testing.T) {
	f := setupGoogle(t)
	f.sheet = [][]any{{"id", "name"}, {1.0, "ann"}, {2.0}}
	yml := `dag_id: gs
tasks:
  - id: read
    operator: gsheet_read
    spreadsheet_id: https://docs.google.com/spreadsheets/d/SHEET/edit#gid=0
    range: Sheet1
  - id: out
    operator: gsheet_write
    spreadsheet_id: SHEET
    range: Out
    write_mode: overwrite
    upstream: [read]
`
	dags, err := ParseYAML("gs.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs)
	v, _ := rs.XComPull("read", "return_value")
	rows, _ := v.([]any)
	if len(rows) != 2 || rows[1].(map[string]any)["name"] != nil {
		t.Fatalf("rows: %v", v)
	}
	if len(f.sheet) != 3 || f.sheet[0][0] != "id" || f.sheet[1][1] != "ann" {
		t.Fatalf("sheet: %v", f.sheet)
	}

	// append into an empty sheet writes the header too; into a filled one it does not
	f.sheet = nil
	yml = strings.Replace(strings.Replace(yml, "write_mode: overwrite", "write_mode: append", 1), "range: Sheet1", "range: Sheet1", 1)
	dags, _ = ParseYAML("gs.yaml", []byte(yml))
	runDAG(t, dags[0], &memRun{x: map[string]map[string]any{}})
	// read was the same fake data, now empty -> nothing written
	if len(f.sheet) != 0 {
		t.Fatalf("empty input should write nothing: %v", f.sheet)
	}
}

func TestGoogleErrors(t *testing.T) {
	setupGoogle(t)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	d := NewDAG("e", "")
	d.NewGSheetReadTask("r", GSheetReadConfig{SpreadsheetID: "SHEET", Range: "A"})
	_ = d.validate()
	task, _ := d.Task("r")
	ctx := NewTaskContext(t.Context(), task, RunInfo{RunID: "r", MapIndex: -1}, &memRun{}, io.Discard)
	if _, err := task.Run(ctx); err == nil || !strings.Contains(err.Error(), "no Google credentials") {
		t.Fatalf("err: %v", err)
	}
	for _, y := range []string{
		"dag_id: a\ntasks:\n  - {id: x, operator: gsheet_read, range: A}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_query}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: bigquery_insert, table: t}\n",
	} {
		dd, err := ParseYAML("a.yaml", []byte(y))
		if err == nil {
			err = dd[0].validate()
		}
		if err == nil {
			t.Fatalf("expected error for %s", y)
		}
	}
	if SheetID("https://docs.google.com/spreadsheets/d/abc_1-X/edit") != "abc_1-X" {
		t.Fatal("SheetID")
	}
}

func TestGoogleCredsShapes(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	key := map[string]any{"type": "service_account", "client_email": "sa@p.iam", "private_key": "-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n", "project_id": "p"}
	keyJS, _ := json.Marshal(key)
	path := filepath.Join(t.TempDir(), "k.json")
	_ = os.WriteFile(path, keyJS, 0o600)
	for name, cfg := range map[string]map[string]any{
		"key pasted as the config":   key,
		"credentials_json object":    {"credentials_json": key},
		"credentials_json string":    {"credentials_json": string(keyJS)},
		"airflow keyfile_dict":       {"keyfile_dict": string(keyJS)},
		"airflow prefixed key_path":  {"extra__google_cloud_platform__key_path": path},
		"airflow extra string":       {"extra": `{"keyfile_dict":` + strconv.Quote(string(keyJS)) + `}`},
		"airflow extra object, path": {"extra": map[string]any{"key_path": path}},
	} {
		k, err := googleCreds(Connection{Type: "google_cloud_platform", Config: cfg})
		if err != nil || k == nil || k.ClientEmail != "sa@p.iam" || k.ProjectID != "p" {
			t.Errorf("%s: key=%v err=%v", name, k, err)
		}
	}
	if k, err := googleCreds(Connection{Config: map[string]any{"project_id": "p"}}); k != nil || err != nil {
		t.Errorf("no key: want metadata fallback, got %v %v", k, err)
	}
}
