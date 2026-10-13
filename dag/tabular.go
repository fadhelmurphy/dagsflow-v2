package dag

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Tabular files (CSV / TSV / Excel .xlsx) as a source: read a sheet into a grid,
// cut it with an A1 range, and turn it into rows (objects keyed by the header).
// Only the standard library is used.

// UploadsDir is where files uploaded in the builder live. A file name without a
// path separator is looked up here; anything else is used as a path as is.
var UploadsDir = ""

// UploadsDirFn lets the host compute the folder lazily (the DAG folder is only
// known once flags and environment are read).
var UploadsDirFn func() string

// UploadsPath is the folder of uploaded files ("" if not configured).
func UploadsPath() string {
	if UploadsDir != "" {
		return UploadsDir
	}
	if UploadsDirFn != nil {
		return UploadsDirFn()
	}
	return ""
}

const (
	maxUploadBytes = 25 << 20
	maxGridRows    = 1_000_000
	maxGridCols    = 16384
)

var uploadExts = map[string]bool{".csv": true, ".tsv": true, ".txt": true, ".xlsx": true, ".xlsm": true}

// SafeUploadName reduces a client supplied name to a safe base name.
func SafeUploadName(name string) (string, error) {
	name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('_')
		}
	}
	out := strings.Trim(b.String(), ".")
	ext := strings.ToLower(filepath.Ext(out))
	if out == "" || !uploadExts[ext] {
		return "", errors.New("file must be .csv, .tsv, .txt or .xlsx")
	}
	return out, nil
}

// ResolveFile finds the file a node reads.
func ResolveFile(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("no file chosen")
	}
	if !strings.ContainsAny(name, "/\\") {
		dir := UploadsPath()
		if dir == "" {
			return "", errors.New("uploads are not configured on this server")
		}
		return filepath.Join(dir, name), nil
	}
	return name, nil
}

// ---- header names ---------------------------------------------------------------

var accentMap = map[rune]string{
	'à': "a", 'á': "a", 'â': "a", 'ã': "a", 'ä': "a", 'å': "a", 'ā': "a", 'ç': "c", 'è': "e", 'é': "e", 'ê': "e", 'ë': "e",
	'ì': "i", 'í': "i", 'î': "i", 'ï': "i", 'ñ': "n", 'ò': "o", 'ó': "o", 'ô': "o", 'õ': "o", 'ö': "o", 'ø': "o",
	'ù': "u", 'ú': "u", 'û': "u", 'ü': "u", 'ý': "y", 'ÿ': "y", 'ß': "ss", 'æ': "ae", 'œ': "oe",
}

// NormalizeHeader makes a column name snake_case: lowercase letters a-z and
// underscores only. camelCase is split, accents are dropped, and every other
// character (spaces, punctuation, digits) separates words.
func NormalizeHeader(s string) string {
	rs := []rune(strings.TrimSpace(s))
	var words []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			words = append(words, cur.String())
			cur.Reset()
		}
	}
	for i, r := range rs {
		if rep, ok := accentMap[unicode.ToLower(r)]; ok {
			if unicode.IsUpper(r) && i > 0 && unicode.IsLower(rs[i-1]) {
				flush()
			}
			cur.WriteString(rep)
			continue
		}
		switch {
		case r >= 'A' && r <= 'Z':
			if i > 0 {
				p := rs[i-1]
				if (p >= 'a' && p <= 'z') || (p >= 'A' && p <= 'Z' && i+1 < len(rs) && rs[i+1] >= 'a' && rs[i+1] <= 'z') {
					flush()
				}
			}
			cur.WriteRune(unicode.ToLower(r))
		case r >= 'a' && r <= 'z':
			cur.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return strings.Join(words, "_")
}

// letterSuffix: 1 -> "b", 2 -> "c" … 25 -> "z", 26 -> "ba" (no digits allowed).
func letterSuffix(n int) string {
	var b []byte
	for n > 0 {
		b = append([]byte{byte('a' + n%26)}, b...)
		n /= 26
	}
	return string(b)
}

// uniqueNames makes names unique; digits-free when letters is true.
func uniqueNames(names []string, letters bool) []string {
	seen := map[string]bool{}
	out := make([]string, len(names))
	for i, n := range names {
		cand := n
		for k := 1; seen[cand]; k++ {
			if letters {
				cand = n + "_" + letterSuffix(k)
			} else {
				cand = fmt.Sprintf("%s_%d", n, k+1)
			}
		}
		seen[cand] = true
		out[i] = cand
	}
	return out
}

// headerNames builds the column names from the header row.
func headerNames(first []any, width int, normalize bool) []string {
	names := make([]string, width)
	for i := range names {
		name := ""
		if i < len(first) && first[i] != nil {
			name = strings.TrimSpace(fmt.Sprint(first[i]))
		}
		if normalize {
			name = NormalizeHeader(name)
			if name == "" {
				name = "column"
			}
		} else if name == "" {
			name = "col_" + strconv.Itoa(i+1)
		}
		names[i] = name
	}
	return uniqueNames(names, normalize)
}

// tableRows turns a grid into objects. Empty cells become null.
func tableRows(values [][]any, header, normalize bool) []any {
	out := []any{}
	if len(values) == 0 {
		return out
	}
	width := 0
	for _, r := range values {
		width = max(width, len(r))
	}
	var cols []string
	body := values
	if header {
		cols = headerNames(values[0], width, normalize)
		body = values[1:]
	} else {
		for i := 0; i < width; i++ {
			cols = append(cols, "col_"+strconv.Itoa(i+1))
		}
	}
	for _, r := range body {
		obj := make(map[string]any, len(cols))
		empty := true
		for i, c := range cols {
			var v any
			if i < len(r) {
				v = r[i]
				if s, ok := v.(string); ok && s == "" {
					v = nil
				}
			}
			if v != nil {
				empty = false
			}
			obj[c] = v
		}
		if !empty { // fully blank lines are skipped
			out = append(out, obj)
		}
	}
	return out
}

// ---- A1 ranges ------------------------------------------------------------------

var a1Part = regexp.MustCompile(`^([A-Za-z]*)(\d*)$`)

func colIndex(letters string) int {
	n := 0
	for _, r := range strings.ToUpper(letters) {
		n = n*26 + int(r-'A') + 1
	}
	return n - 1
}

type a1Range struct{ c1, r1, c2, r2 int } // 0-based start, inclusive end; -1 = open

// parseA1 reads "C5:F", "C:F", "B3:E20" or "C5" (start cell, open ended). The
// sheet name before "!" is returned separately.
func parseA1(s string) (a1Range, string, error) {
	s = strings.TrimSpace(s)
	sheet := ""
	if i := strings.LastIndex(s, "!"); i >= 0 {
		sheet = strings.Trim(strings.TrimSpace(s[:i]), "'")
		s = strings.TrimSpace(s[i+1:])
	}
	rg := a1Range{0, 0, -1, -1}
	if s == "" {
		return rg, sheet, nil
	}
	parts := strings.Split(s, ":")
	if len(parts) > 2 {
		return rg, sheet, fmt.Errorf("bad range %q", s)
	}
	for i, p := range parts {
		m := a1Part.FindStringSubmatch(strings.TrimSpace(p))
		if m == nil || (m[1] == "" && m[2] == "") && !(i == 1 && p == "") {
			return rg, sheet, fmt.Errorf("bad range %q: use e.g. B3:F or C:F", s)
		}
		col, row := -1, -1
		if m[1] != "" {
			col = colIndex(m[1])
		}
		if m[2] != "" {
			rn, _ := strconv.Atoi(m[2])
			if rn < 1 {
				return rg, sheet, fmt.Errorf("bad range %q: rows start at 1", s)
			}
			row = rn - 1
		}
		if i == 0 {
			if col >= 0 {
				rg.c1 = col
			}
			if row >= 0 {
				rg.r1 = row
			}
		} else {
			rg.c2, rg.r2 = col, row
		}
	}
	if rg.c2 >= 0 && rg.c2 < rg.c1 || rg.r2 >= 0 && rg.r2 < rg.r1 {
		return rg, sheet, fmt.Errorf("bad range %q: the end is before the start", s)
	}
	return rg, sheet, nil
}

func (rg a1Range) apply(grid [][]any) [][]any {
	if rg.r1 >= len(grid) {
		return nil
	}
	end := len(grid)
	if rg.r2 >= 0 && rg.r2+1 < end {
		end = rg.r2 + 1
	}
	out := make([][]any, 0, end-rg.r1)
	width := 0
	for _, r := range grid[rg.r1:end] {
		hi := len(r)
		if rg.c2 >= 0 && rg.c2+1 < hi {
			hi = rg.c2 + 1
		}
		var cut []any
		if rg.c1 < hi {
			cut = r[rg.c1:hi]
		}
		out = append(out, cut)
		width = max(width, len(cut))
	}
	if rg.c2 >= 0 { // keep the requested width even if the right columns are empty
		width = max(width, rg.c2-rg.c1+1)
	}
	for i := range out {
		padded := make([]any, width)
		copy(padded, out[i])
		out[i] = padded
	}
	return out
}

// ---- reading files --------------------------------------------------------------

// FileInfo describes a readable file.
type FileInfo struct {
	Format string   `json:"format"`
	Sheets []string `json:"sheets,omitempty"`
}

func fileFormat(path, format string) (string, error) {
	f := strings.ToLower(strings.TrimSpace(format))
	if f == "" || f == "auto" {
		switch strings.ToLower(filepath.Ext(path)) {
		case ".xlsx", ".xlsm":
			return "xlsx", nil
		case ".csv", ".tsv", ".txt":
			return "csv", nil
		}
		return "", fmt.Errorf("cannot tell the format of %s: use .csv, .tsv or .xlsx", filepath.Base(path))
	}
	if f != "csv" && f != "xlsx" {
		return "", fmt.Errorf("unknown format %q (csv or xlsx)", format)
	}
	return f, nil
}

// ReadGrid reads the file (the chosen sheet for xlsx) as a dense grid.
func ReadGrid(path, format, sheet, delim string) ([][]any, FileInfo, error) {
	fm, err := fileFormat(path, format)
	if err != nil {
		return nil, FileInfo{}, err
	}
	st, err := os.Stat(path)
	if err != nil {
		return nil, FileInfo{}, fmt.Errorf("cannot open %s: %v", filepath.Base(path), errors.Unwrap(err))
	}
	if st.Size() > 4*maxUploadBytes {
		return nil, FileInfo{}, fmt.Errorf("%s is too large (%d MB)", filepath.Base(path), st.Size()>>20)
	}
	if fm == "xlsx" {
		g, names, err := readXLSX(path, sheet)
		return g, FileInfo{Format: fm, Sheets: names}, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, FileInfo{}, err
	}
	g, err := readCSV(b, delim, strings.ToLower(filepath.Ext(path)) == ".tsv")
	return g, FileInfo{Format: fm}, err
}

func sniffDelim(b []byte) rune {
	line := b
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		line = b[:i]
	}
	best, bestN := ',', 0
	for _, d := range []rune{',', ';', '\t', '|'} {
		n, inq := 0, false
		for _, r := range string(line) {
			if r == '"' {
				inq = !inq
			} else if r == d && !inq {
				n++
			}
		}
		if n > bestN {
			best, bestN = d, n
		}
	}
	return best
}

func readCSV(b []byte, delim string, tsv bool) ([][]any, error) {
	b = bytes.TrimPrefix(b, []byte{0xEF, 0xBB, 0xBF})
	if !utf8Valid(b) {
		b = []byte(strings.ToValidUTF8(string(b), "?"))
	}
	var d rune
	switch strings.ToLower(strings.TrimSpace(delim)) {
	case "":
		d = sniffDelim(b)
		if tsv && d == ',' {
			d = '\t'
		}
	case "tab", "\\t":
		d = '\t'
	case "comma":
		d = ','
	case "semicolon":
		d = ';'
	case "pipe":
		d = '|'
	default:
		rs := []rune(delim)
		if len(rs) != 1 {
			return nil, fmt.Errorf("delimiter must be one character (or tab, comma, semicolon, pipe), got %q", delim)
		}
		d = rs[0]
	}
	r := csv.NewReader(bytes.NewReader(b))
	r.Comma = d
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	var grid [][]any
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("csv: %v", err)
		}
		row := make([]any, len(rec))
		for i, v := range rec {
			row[i] = v
		}
		grid = append(grid, row)
		if len(grid) > maxGridRows {
			return nil, errors.New("csv has more than 1,000,000 lines")
		}
	}
	return grid, nil
}

func utf8Valid(b []byte) bool { return strings.ToValidUTF8(string(b), "") == string(b) }

// ---- xlsx -----------------------------------------------------------------------

const nsRel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

type xlsxBook struct {
	Sheets []struct {
		Name string `xml:"name,attr"`
		RID  string `xml:"http://schemas.openxmlformats.org/officeDocument/2006/relationships id,attr"`
		ID   string `xml:"id,attr"`
	} `xml:"sheets>sheet"`
	Pr struct {
		D1904 string `xml:"date1904,attr"`
	} `xml:"workbookPr"`
}

func zipFile(zr *zip.ReadCloser, name string) *zip.File {
	name = strings.TrimPrefix(name, "/")
	for _, f := range zr.File {
		if strings.EqualFold(f.Name, name) {
			return f
		}
	}
	return nil
}

func zipBytes(zr *zip.ReadCloser, name string) ([]byte, error) {
	f := zipFile(zr, name)
	if f == nil {
		return nil, fmt.Errorf("not an xlsx file: %s is missing", name)
	}
	if f.UncompressedSize64 > 400<<20 {
		return nil, errors.New("xlsx part is too large")
	}
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func readXLSX(path, sheet string) ([][]any, []string, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return nil, nil, errors.New("not a valid .xlsx file (old .xls and password protected files are not supported)")
	}
	defer zr.Close()
	wb, err := zipBytes(zr, "xl/workbook.xml")
	if err != nil {
		return nil, nil, err
	}
	var book xlsxBook
	if err := xml.Unmarshal(wb, &book); err != nil {
		return nil, nil, fmt.Errorf("workbook: %v", err)
	}
	if len(book.Sheets) == 0 {
		return nil, nil, errors.New("the workbook has no sheets")
	}
	names := make([]string, len(book.Sheets))
	for i, s := range book.Sheets {
		names[i] = s.Name
	}
	pick := 0
	if s := strings.TrimSpace(sheet); s != "" {
		pick = -1
		for i, n := range names {
			if strings.EqualFold(n, s) {
				pick = i
				break
			}
		}
		if pick < 0 {
			if n, err := strconv.Atoi(s); err == nil && n >= 1 && n <= len(names) {
				pick = n - 1
			}
		}
		if pick < 0 {
			return nil, names, fmt.Errorf("sheet %q not found; the file has: %s", s, strings.Join(names, ", "))
		}
	}
	rels, _ := zipBytes(zr, "xl/_rels/workbook.xml.rels")
	var rl struct {
		R []struct{ ID, Target string } `xml:"Relationship"`
	}
	_ = xml.Unmarshal(rels, &rl)
	target := ""
	for _, r := range rl.R {
		if r.ID == book.Sheets[pick].RID {
			target = r.Target
		}
	}
	if target == "" {
		target = fmt.Sprintf("worksheets/sheet%d.xml", pick+1)
	}
	if strings.HasPrefix(target, "/") {
		target = strings.TrimPrefix(target, "/")
	} else {
		target = "xl/" + target
	}
	shared := readSharedStrings(zr)
	dateStyle := readDateStyles(zr)
	data, err := zipBytes(zr, target)
	if err != nil {
		return nil, names, err
	}
	g, err := parseSheetXML(data, shared, dateStyle, book.Pr.D1904 == "1" || book.Pr.D1904 == "true")
	return g, names, err
}

func readSharedStrings(zr *zip.ReadCloser) []string {
	b, err := zipBytes(zr, "xl/sharedStrings.xml")
	if err != nil {
		return nil
	}
	dec := xml.NewDecoder(bytes.NewReader(b))
	var out []string
	var cur strings.Builder
	inSI, inT, skip := false, false, 0
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "si":
				inSI = true
				cur.Reset()
			case "rPh":
				skip++
			case "t":
				inT = inSI && skip == 0
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "si":
				out = append(out, cur.String())
				inSI = false
			case "rPh":
				skip--
			case "t":
				inT = false
			}
		case xml.CharData:
			if inT {
				cur.Write(t)
			}
		}
	}
	return out
}

var builtinDateFmts = map[int]bool{14: true, 15: true, 16: true, 17: true, 18: true, 19: true, 20: true, 21: true, 22: true,
	27: true, 28: true, 29: true, 30: true, 31: true, 32: true, 33: true, 34: true, 35: true, 36: true,
	45: true, 46: true, 47: true, 50: true, 51: true, 52: true, 53: true, 54: true, 55: true, 56: true, 57: true, 58: true}

var fmtQuoted = regexp.MustCompile(`"[^"]*"|\[[^\]]*\]|\\.|_.|\*.`)

func looksLikeDate(code string) bool {
	c := strings.ToLower(fmtQuoted.ReplaceAllString(code, ""))
	return strings.ContainsAny(c, "ymdhs") && !strings.Contains(c, "general")
}

// readDateStyles returns, per cell style index, whether it formats a date.
func readDateStyles(zr *zip.ReadCloser) []bool {
	b, err := zipBytes(zr, "xl/styles.xml")
	if err != nil {
		return nil
	}
	var st struct {
		Fmts []struct {
			ID   int    `xml:"numFmtId,attr"`
			Code string `xml:"formatCode,attr"`
		} `xml:"numFmts>numFmt"`
		Xfs []struct {
			ID int `xml:"numFmtId,attr"`
		} `xml:"cellXfs>xf"`
	}
	if xml.Unmarshal(b, &st) != nil {
		return nil
	}
	custom := map[int]bool{}
	for _, f := range st.Fmts {
		custom[f.ID] = looksLikeDate(f.Code)
	}
	out := make([]bool, len(st.Xfs))
	for i, x := range st.Xfs {
		out[i] = builtinDateFmts[x.ID] || custom[x.ID]
	}
	return out
}

func excelTime(serial float64, d1904 bool) string {
	base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
	if d1904 {
		base = time.Date(1904, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	days := math.Floor(serial)
	secs := math.Round((serial - days) * 86400)
	t := base.AddDate(0, 0, int(days)).Add(time.Duration(secs) * time.Second)
	if secs == 0 {
		return t.Format("2006-01-02")
	}
	return t.Format("2006-01-02 15:04:05")
}

func numberValue(f float64) any {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return int64(f)
	}
	return f
}

func parseSheetXML(data []byte, shared []string, dateStyle []bool, d1904 bool) ([][]any, error) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	var grid [][]any
	var (
		row, col       = -1, -1
		ctype, ref     string
		style          int
		inV, inT, inIS bool
		v, txt         strings.Builder
		curCell        bool
	)
	setCell := func(val any) error {
		if row < 0 || col < 0 || val == nil {
			return nil
		}
		if row >= maxGridRows || col >= maxGridCols {
			return errors.New("sheet is too large")
		}
		for len(grid) <= row {
			grid = append(grid, nil)
		}
		for len(grid[row]) <= col {
			grid[row] = append(grid[row], nil)
		}
		grid[row][col] = val
		return nil
	}
	nextRow := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("sheet: %v", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "row":
				row = nextRow
				for _, a := range t.Attr {
					if a.Name.Local == "r" {
						if n, err := strconv.Atoi(a.Value); err == nil && n >= 1 {
							row = n - 1
						}
					}
				}
				nextRow = row + 1
				col = -1
			case "c":
				curCell = true
				ctype, ref, style = "", "", -1
				v.Reset()
				txt.Reset()
				for _, a := range t.Attr {
					switch a.Name.Local {
					case "t":
						ctype = a.Value
					case "r":
						ref = a.Value
					case "s":
						style, _ = strconv.Atoi(a.Value)
					}
				}
				if ref != "" {
					letters := strings.TrimRightFunc(ref, unicode.IsDigit)
					col = colIndex(letters)
				} else {
					col++
				}
			case "v":
				inV = curCell
			case "is":
				inIS = true
			case "t":
				inT = inIS
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "v":
				inV = false
			case "t":
				inT = false
			case "is":
				inIS = false
			case "c":
				curCell = false
				raw := v.String()
				var val any
				switch ctype {
				case "s":
					if i, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && i >= 0 && i < len(shared) {
						val = shared[i]
					}
				case "inlineStr":
					val = txt.String()
				case "str", "d":
					val = raw
				case "b":
					val = strings.TrimSpace(raw) == "1"
				case "e":
					val = nil
				default:
					if strings.TrimSpace(raw) != "" {
						f, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
						switch {
						case err != nil:
							val = raw
						case style >= 0 && style < len(dateStyle) && dateStyle[style]:
							val = excelTime(f, d1904)
						default:
							val = numberValue(f)
						}
					}
				}
				if s, ok := val.(string); ok && s == "" {
					val = nil
				}
				if err := setCell(val); err != nil {
					return nil, err
				}
			}
		case xml.CharData:
			if inV {
				v.Write(t)
			}
			if inT {
				txt.Write(t)
			}
		}
	}
	return grid, nil
}
