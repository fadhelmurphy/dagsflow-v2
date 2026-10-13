package dag

import (
	"archive/zip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeHeader(t *testing.T) {
	for in, want := range map[string]string{
		"Customer Name":    "customer_name",
		"  Total (Rp) ":    "total_rp",
		"orderID":          "order_id",
		"HTTPServer":       "http_server",
		"Tanggal Lahir 2":  "tanggal_lahir",
		"Café-Crème":       "cafe_creme",
		"first_name":       "first_name",
		"__a__b__":         "a_b",
		"No.":              "no",
		"2024":             "",
		"Nama   Lengkap!!": "nama_lengkap",
	} {
		if got := NormalizeHeader(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
	got := headerNames([]any{"Name", "name", "NAME", "", "", "Qty 1", "Qty 2"}, 7, true)
	want := []string{"name", "name_b", "name_c", "column", "column_b", "qty", "qty_b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%v", got)
	}
	for _, n := range got {
		if strings.Trim(n, "abcdefghijklmnopqrstuvwxyz_") != "" {
			t.Fatalf("%q has other characters", n)
		}
	}
}

func TestParseA1AndApply(t *testing.T) {
	grid := [][]any{
		{"junk"}, {"junk"},
		{nil, nil, "id", "Full Name", "x", "y"},
		{nil, nil, 1, "ann", "a", "b"},
		{nil, nil, 2, "bob"},
		{nil, nil, 3, "cy", "c", "d"},
	}
	for rng, check := range map[string]func([]any){
		"C3:D": func(r []any) {
			if len(r) != 3 || r[1].(map[string]any)["full_name"] != "bob" {
				t.Errorf("C3:D %v", r)
			}
		},
		"C3:F5": func(r []any) {
			if len(r) != 2 || r[0].(map[string]any)["x"] != "a" {
				t.Errorf("C3:F5 %v", r)
			}
		},
		"Sheet1!C3": func(r []any) {
			if len(r) != 3 {
				t.Errorf("C3 %v", r)
			}
		},
		"E:F": func(r []any) { // header junk on row 1: only 'junk' column? rows 1-2 are narrower
			if len(r) == 0 {
				t.Errorf("E:F %v", r)
			}
		},
	} {
		a1, _, err := parseA1(rng)
		if err != nil {
			t.Fatal(rng, err)
		}
		check(tableRows(a1.apply(grid), true, true))
	}
	if _, sh, _ := parseA1("'My Sheet'!A1:B2"); sh != "My Sheet" {
		t.Fatal(sh)
	}
	for _, bad := range []string{"A1:B2:C3", "F5:B2", "$$", "A0:B"} {
		if _, _, err := parseA1(bad); err == nil {
			t.Errorf("%q should fail", bad)
		}
	}
}

func TestCSVRead(t *testing.T) {
	dir := t.TempDir()
	// BOM, semicolon delimiter, quoted field with a delimiter and a blank line
	body := "\xEF\xBB\xBFNama Lengkap;Umur;Catatan\nAna;30;\"a;b\"\n\nBudi;;ok\n"
	p := filepath.Join(dir, "d.csv")
	_ = os.WriteFile(p, []byte(body), 0o600)
	g, info, err := ReadGrid(p, "", "", "")
	if err != nil || info.Format != "csv" {
		t.Fatal(err, info)
	}
	rows := tableRows(g, true, true)
	if len(rows) != 2 {
		t.Fatalf("%v", rows)
	}
	r0, r1 := rows[0].(map[string]any), rows[1].(map[string]any)
	if r0["nama_lengkap"] != "Ana" || r0["catatan"] != "a;b" || r1["umur"] != nil {
		t.Fatalf("%v %v", r0, r1)
	}
	tsv := filepath.Join(dir, "d.tsv")
	_ = os.WriteFile(tsv, []byte("a\tb\n1\t2\n"), 0o600)
	g, _, _ = ReadGrid(tsv, "", "", "")
	if len(g[0]) != 2 {
		t.Fatalf("%v", g)
	}
	if _, _, err := ReadGrid(filepath.Join(dir, "none.csv"), "", "", ""); err == nil {
		t.Fatal("missing file")
	}
}

// writeXLSX builds a minimal workbook: two sheets, shared + inline strings, a date.
func writeXLSX(t *testing.T, path string) {
	t.Helper()
	f, _ := os.Create(path)
	zw := zip.NewWriter(f)
	add := func(name, body string) {
		w, _ := zw.Create(name)
		_, _ = w.Write([]byte(body))
	}
	add("xl/workbook.xml", `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Summary" sheetId="1" r:id="rId1"/><sheet name="Data Q1" sheetId="2" r:id="rId2"/></sheets></workbook>`)
	add("xl/_rels/workbook.xml.rels", `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Target="/xl/worksheets/sheet2.xml"/></Relationships>`)
	add("xl/sharedStrings.xml", `<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><si><t>Report</t></si><si><t>Order ID</t></si><si><r><t>Full </t></r><r><t>Name</t></r></si><si><t>Tgl Order</t></si><si><t>Total</t></si><si><t>ana</t></si></sst>`)
	add("xl/styles.xml", `<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><numFmts><numFmt numFmtId="164" formatCode="dd/mm/yyyy"/></numFmts><cellXfs><xf numFmtId="0"/><xf numFmtId="164"/></cellXfs></styleSheet>`)
	add("xl/worksheets/sheet1.xml", `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="s"><v>0</v></c></row></sheetData></worksheet>`)
	// title on rows 1-2, header on row 3 starting at column B, a gap row, a blank cell
	add("xl/worksheets/sheet2.xml", `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>
<row r="1"><c r="A1" t="inlineStr"><is><t>Sales Q1</t></is></c></row>
<row r="3"><c r="B3" t="s"><v>1</v></c><c r="C3" t="s"><v>2</v></c><c r="D3" t="s"><v>3</v></c><c r="E3" t="s"><v>4</v></c></row>
<row r="4"><c r="B4"><v>1001</v></c><c r="C4" t="s"><v>5</v></c><c r="D4" s="1"><v>45292</v></c><c r="E4"><v>12.5</v></c></row>
<row r="5"><c r="B5"><v>1002</v></c><c r="D5" s="1"><v>45293.5</v></c><c r="E5" t="b"><v>1</v></c></row>
</sheetData></worksheet>`)
	_ = zw.Close()
	_ = f.Close()
}

func TestXLSXRead(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.xlsx")
	writeXLSX(t, p)
	// by sheet name, with start row and columns
	g, info, err := ReadGrid(p, "", "data q1", "")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(info.Sheets, []string{"Summary", "Data Q1"}) {
		t.Fatalf("%v", info)
	}
	a1, _, _ := parseA1("B3:E")
	rows := tableRows(a1.apply(g), true, true)
	if len(rows) != 2 {
		t.Fatalf("%v", rows)
	}
	r0, r1 := rows[0].(map[string]any), rows[1].(map[string]any)
	if r0["order_id"] != int64(1001) || r0["full_name"] != "ana" || r0["tgl_order"] != "2024-01-01" || r0["total"] != 12.5 {
		t.Fatalf("%v", r0)
	}
	if r1["full_name"] != nil || r1["tgl_order"] != "2024-01-02 12:00:00" || r1["total"] != true {
		t.Fatalf("%v", r1)
	}
	// sheet by number, and the default (first) sheet
	if g2, _, err := ReadGrid(p, "", "2", ""); err != nil || len(g2) != 5 {
		t.Fatal(err, len(g2))
	}
	g1, _, _ := ReadGrid(p, "", "", "")
	if len(g1) != 1 || g1[0][0] != "Report" {
		t.Fatalf("%v", g1)
	}
	if _, inf, err := ReadGrid(p, "", "nope", ""); err == nil || !strings.Contains(err.Error(), "Data Q1") || len(inf.Sheets) != 2 {
		t.Fatalf("%v", err)
	}
	bad := filepath.Join(t.TempDir(), "x.xlsx")
	_ = os.WriteFile(bad, []byte("not a zip"), 0o600)
	if _, _, err := ReadGrid(bad, "", "", ""); err == nil {
		t.Fatal("expected error")
	}
}

func TestFileReadYAMLAndMerge(t *testing.T) {
	dir := t.TempDir()
	UploadsDir = dir
	t.Cleanup(func() { UploadsDir = "" })
	_ = os.WriteFile(filepath.Join(dir, "users.csv"), []byte("User ID,Name\n1,ana\n2,bob\n3,cy\n"), 0o600)
	_ = os.WriteFile(filepath.Join(dir, "orders.csv"), []byte("Order No,user_id,Amount\n10,1,5\n11,1,7\n12,3,9\n13,9,1\n"), 0o600)
	yml := `dag_id: m
tasks:
  - id: users
    operator: file_read
    file: users.csv
    keep_headers: false
  - id: orders
    operator: file_read
    file: orders.csv
  - id: j
    operator: merge
    mode: join
    join_type: left
    on: [user_id]
    upstream: [orders, users]
  - id: u
    operator: merge
    upstream: [users, orders]
`
	dags, err := ParseYAML("m.yaml", []byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	rs := &memRun{}
	runDAG(t, dags[0], rs)
	v, _ := rs.XComPull("users", "return_value")
	if us := v.([]any); us[0].(map[string]any)["user_id"] != "1" {
		t.Fatalf("%v", v)
	}
	v, _ = rs.XComPull("j", "return_value")
	js := v.([]any)
	if len(js) != 4 {
		t.Fatalf("left join rows: %v", js)
	}
	first := js[0].(map[string]any)
	if _, dup := first["user_id_right"]; dup {
		t.Fatalf("same-named join key must not be duplicated: %v", first)
	}
	if first["name"] != "ana" || first["amount"] != "5" {
		t.Fatalf("%v", first)
	}
	if last := js[3].(map[string]any); last["name"] != nil || last["order_no"] != "13" {
		t.Fatalf("unmatched left row: %v", last)
	}
	v, _ = rs.XComPull("u", "return_value")
	if len(v.([]any)) != 7 {
		t.Fatalf("union: %v", v)
	}

	for _, y := range []string{
		"dag_id: a\ntasks:\n  - {id: x, operator: file_read}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: file_read, file: a.csv, range: 'A1:B2:C'}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: merge, mode: join}\n",
		"dag_id: a\ntasks:\n  - {id: x, operator: merge, mode: zip}\n",
	} {
		dd, err := ParseYAML("a.yaml", []byte(y))
		if err == nil {
			err = dd[0].validate()
		}
		if err == nil {
			t.Errorf("expected an error for %s", y)
		}
	}
}

func TestSnakeCaseStep(t *testing.T) {
	rows := []any{map[string]any{"Full Name": "ana", "Order ID": 1, "order id": 2}}
	out, err := ApplyTransform(rows, []TransformStep{{Op: "snake_case"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := out[0].(map[string]any)
	if m["full_name"] != "ana" || m["order_id"] == nil || m["order_id_b"] == nil || len(m) != 3 {
		t.Fatalf("%v", m)
	}
}

func TestSafeUploadName(t *testing.T) {
	for in, want := range map[string]string{"Data Jan.csv": "Data_Jan.csv", "../../etc/x.xlsx": "x.xlsx", `C:\u\a b.TSV`: "a_b.TSV"} {
		if got, err := SafeUploadName(in); err != nil || got != want {
			t.Errorf("%q -> %q %v", in, got, err)
		}
	}
	for _, bad := range []string{"a.exe", ".csv", "noext", "../.."} {
		if _, err := SafeUploadName(bad); err == nil {
			t.Errorf("%q should be refused", bad)
		}
	}
}

func TestBuilderFileAndMergeNodes(t *testing.T) {
	g := Graph{DagID: "f_etl", Nodes: []GNode{
		{ID: "a", Type: "file_read", Config: map[string]any{"file": "a.csv", "range": "B3:F"}},
		{ID: "b", Type: "file_read", Config: map[string]any{"file": "b.xlsx", "sheet": "Data"}},
		{ID: "m", Type: "merge", Config: map[string]any{"mode": "join", "on": []any{"id"}}},
	}, Edges: []GEdge{{From: "a", To: "m"}, {From: "b", To: "m"}}}
	y, iss := g.Validate()
	for _, i := range iss {
		if !i.Warning {
			t.Fatalf("%v", iss)
		}
	}
	back, err := ParseGraph(y)
	if err != nil || len(back.Edges) != 2 || back.Nodes[2].Type != "merge" {
		t.Fatalf("%v %+v", err, back)
	}
	g.Edges = g.Edges[:1]
	_, iss = g.Validate()
	found := false
	for _, i := range iss {
		found = found || strings.Contains(i.Message, "at least 2")
	}
	if !found {
		t.Fatalf("expected the merge input error: %v", iss)
	}
}
