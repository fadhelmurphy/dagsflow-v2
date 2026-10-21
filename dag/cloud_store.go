package dag

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Dump rows to object storage: Google Cloud Storage (service account, same
// credentials as BigQuery / Sheets) and Amazon S3 or any S3 compatible store
// (MinIO, Cloudflare R2, …), both over plain REST with the standard library.

const (
	scopeGCS      = "https://www.googleapis.com/auth/devstorage.read_write"
	defaultAWSCon = "aws_default"
)

var gcsBase = "https://storage.googleapis.com"

var storeHTTP = &http.Client{Timeout: 10 * time.Minute}

// ---- encoding rows --------------------------------------------------------------

func cellText(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case map[string]any, []any:
		b, _ := json.Marshal(x)
		return string(b)
	default:
		return fmt.Sprint(x)
	}
}

// encodeRows serialises rows as csv, json (one array) or ndjson (a JSON object
// per line). It returns the bytes, the content type and the file extension.
func encodeRows(rows []any, format string, cols []string, delim string, zip bool) ([]byte, string, string, error) {
	var buf bytes.Buffer
	var ctype, ext string
	switch format {
	case "", "csv", "tsv":
		d := ','
		if format == "tsv" {
			d = '\t'
		}
		if s := strings.ToLower(strings.TrimSpace(delim)); s != "" {
			switch s {
			case "tab", "\\t":
				d = '\t'
			case "semicolon":
				d = ';'
			case "pipe":
				d = '|'
			case "comma":
				d = ','
			default:
				if r := []rune(delim); len(r) == 1 {
					d = r[0]
				} else {
					return nil, "", "", fmt.Errorf("delimiter must be one character, got %q", delim)
				}
			}
		}
		if len(cols) == 0 {
			set := map[string]bool{}
			for _, r := range rows {
				m, ok := r.(map[string]any)
				if !ok {
					return nil, "", "", fmt.Errorf("rows must be objects, got %T", r)
				}
				for k := range m {
					set[k] = true
				}
			}
			for k := range set {
				cols = append(cols, k)
			}
			sort.Strings(cols)
		}
		w := csv.NewWriter(&buf)
		w.Comma = d
		_ = w.Write(cols)
		line := make([]string, len(cols))
		for _, r := range rows {
			m, ok := r.(map[string]any)
			if !ok {
				return nil, "", "", fmt.Errorf("rows must be objects, got %T", r)
			}
			for i, c := range cols {
				line[i] = cellText(m[c])
			}
			_ = w.Write(line)
		}
		w.Flush()
		ctype, ext = "text/csv", ".csv"
		if d == '\t' {
			ctype, ext = "text/tab-separated-values", ".tsv"
		}
	case "json":
		b, err := json.Marshal(rows)
		if err != nil {
			return nil, "", "", err
		}
		buf.Write(b)
		ctype, ext = "application/json", ".json"
	case "ndjson", "jsonl":
		enc := json.NewEncoder(&buf)
		for _, r := range rows {
			if err := enc.Encode(r); err != nil {
				return nil, "", "", err
			}
		}
		ctype, ext = "application/x-ndjson", ".ndjson"
	default:
		return nil, "", "", fmt.Errorf("unknown format %q (csv, tsv, json, ndjson)", format)
	}
	if !zip {
		return buf.Bytes(), ctype, ext, nil
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write(buf.Bytes())
	if err := zw.Close(); err != nil {
		return nil, "", "", err
	}
	return gz.Bytes(), "application/gzip", ext + ".gz", nil
}

// ---- shared task body -----------------------------------------------------------

// StoreWriteConfig configures gcs_write and s3_write.
type StoreWriteConfig struct {
	ConnID   string
	Bucket   string
	Object   string // object name / key, templated
	Region   string // s3
	Format   string // csv (default) | tsv | json | ndjson
	Columns  []string
	Delim    string
	Gzip     bool
	RowsFrom string
}

func (cfg StoreWriteConfig) check(d *DAG, id, op string) {
	if strings.TrimSpace(cfg.Bucket) == "" {
		d.buildErr(fmt.Errorf("task %s: %s needs bucket", id, op))
	}
	if strings.TrimSpace(cfg.Object) == "" {
		d.buildErr(fmt.Errorf("task %s: %s needs the object name (e.g. exports/{{ .ds }}/orders.csv)", id, op))
	}
	if _, _, _, err := encodeRows(nil, cfg.Format, cfg.Columns, cfg.Delim, false); err != nil {
		d.buildErr(fmt.Errorf("task %s: %v", id, err))
	}
}

// objectName renders the name and adds the extension for the format when the
// name has none (and .gz for gzip).
func (cfg StoreWriteConfig) objectName(ctx *TaskContext, ext string) (string, error) {
	name, err := ctx.RenderTemplateField("object")
	if err != nil {
		return "", err
	}
	name = strings.TrimLeft(strings.TrimSpace(name), "/")
	if name == "" || strings.HasSuffix(name, "/") {
		return "", errors.New("the object name is empty or ends with /")
	}
	if cfg.Gzip && !strings.HasSuffix(name, ".gz") {
		base := strings.TrimSuffix(ext, ".gz")
		if !strings.HasSuffix(name, base) {
			name += base
		}
		name += ".gz"
	}
	return name, nil
}

func (d *DAG) newStoreTask(id, op, opName, scheme string, cfg StoreWriteConfig,
	put func(ctx *TaskContext, name, ctype string, data []byte) (string, error), opts ...TaskOption) *Task {
	cfg.check(d, id, op)
	opts = append([]TaskOption{WithOperatorName(opName),
		WithTemplateFields(TemplateField{Name: "object", Raw: cfg.Object, Lang: "text"})}, opts...)
	return d.NewTask(id, func(ctx *TaskContext) error {
		rows, err := inputRowList(ctx, cfg.RowsFrom)
		if err != nil {
			return err
		}
		data, ctype, ext, err := encodeRows(rows, cfg.Format, cfg.Columns, cfg.Delim, cfg.Gzip)
		if err != nil {
			return err
		}
		name, err := cfg.objectName(ctx, ext)
		if err != nil {
			return err
		}
		ctx.Logf("uploading %d rows (%d bytes) to %s://%s/%s", len(rows), len(data), scheme, cfg.Bucket, name)
		uri, err := put(ctx, name, ctype, data)
		if err != nil {
			return err
		}
		ctx.Logf("written: %s", uri)
		_ = ctx.XComPush("rows_affected", int64(len(rows)))
		return ctx.XComPush("return_value", storeOutput(cfg, uri, rows, len(data)))
	}, opts...)
}

// ---- Google Cloud Storage ------------------------------------------------------

// NewGCSWriteTask dumps the rows of the upstream node to a GCS object.
func (d *DAG) NewGCSWriteTask(id string, cfg StoreWriteConfig, opts ...TaskOption) *Task {
	return d.newStoreTask(id, "gcs_write", "GCSWriteOperator", "gs", cfg,
		func(ctx *TaskContext, name, ctype string, data []byte) (string, error) {
			conn, _ := googleConn(cfg.ConnID)
			tok, err := googleToken(ctx, conn, scopeGCS)
			if err != nil {
				return "", err
			}
			u := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?uploadType=media&name=%s", gcsBase, url.PathEscape(cfg.Bucket), url.QueryEscape(name))
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(data))
			if err != nil {
				return "", err
			}
			req.Header.Set("Authorization", "Bearer "+tok)
			req.Header.Set("Content-Type", ctype)
			if err := doJSONWith(storeHTTP, req, nil); err != nil {
				return "", fmt.Errorf("upload to gs://%s/%s: %w", cfg.Bucket, name, err)
			}
			return "gs://" + cfg.Bucket + "/" + name, nil
		}, opts...)
}

// ---- Amazon S3 (AWS Signature V4) ------------------------------------------------

type awsCreds struct {
	Key, Secret, Token, Region, Endpoint string
}

// awsCredentials resolves the credentials of a connection (see aws_creds.go).
func awsCredentials(connID string) (awsCreds, error) {
	return awsCredentialsCtx(context.Background(), connID)
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func sha256Hex(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// uriEncode is the AWS flavour of percent-encoding (slash kept for paths).
func uriEncode(s string, keepSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && keepSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// signV4 returns the Authorization header value. headers holds every header to
// sign (lowercase names, including host and x-amz-date).
func signV4(method, path, rawQuery string, headers map[string]string, payloadHash, region, service string, cr awsCreds, now time.Time) string {
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var canon strings.Builder
	for _, k := range names {
		canon.WriteString(k + ":" + strings.Join(strings.Fields(headers[k]), " ") + "\n")
	}
	signed := strings.Join(names, ";")
	creq := strings.Join([]string{method, uriEncode(path, true), rawQuery, canon.String(), signed, payloadHash}, "\n")
	date := now.UTC().Format("20060102")
	scope := date + "/" + region + "/" + service + "/aws4_request"
	sts := "AWS4-HMAC-SHA256\n" + now.UTC().Format("20060102T150405Z") + "\n" + scope + "\n" + sha256Hex([]byte(creq))
	k := hmacSHA256([]byte("AWS4"+cr.Secret), date)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	k = hmacSHA256(k, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(k, sts))
	return fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s,SignedHeaders=%s,Signature=%s", cr.Key, scope, signed, sig)
}

// s3URL: virtual-hosted style on AWS, path style for custom endpoints and for
// bucket names with dots (they break the wildcard certificate).
func s3URL(cr awsCreds, bucket, key string) (*url.URL, error) {
	if cr.Endpoint != "" {
		base, err := url.Parse(strings.TrimRight(cr.Endpoint, "/"))
		if err != nil || base.Host == "" {
			return nil, fmt.Errorf("bad endpoint_url %q", cr.Endpoint)
		}
		base.Path = strings.TrimRight(base.Path, "/") + "/" + bucket + "/" + key
		return base, nil
	}
	host := "s3." + cr.Region + ".amazonaws.com"
	if strings.Contains(bucket, ".") {
		return &url.URL{Scheme: "https", Host: host, Path: "/" + bucket + "/" + key}, nil
	}
	return &url.URL{Scheme: "https", Host: bucket + "." + host, Path: "/" + key}, nil
}

func s3Put(ctx *TaskContext, cr awsCreds, bucket, key, ctype string, data []byte) error {
	u, err := s3URL(cr, bucket, key)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	ph := sha256Hex(data)
	hdr := map[string]string{"host": u.Host, "content-type": ctype, "x-amz-content-sha256": ph, "x-amz-date": now.Format("20060102T150405Z")}
	if cr.Token != "" {
		hdr["x-amz-security-token"] = cr.Token
	}
	auth := signV4(http.MethodPut, u.Path, "", hdr, ph, cr.Region, "s3", cr, now)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: uriEncode(u.Path, true)}).String(), bytes.NewReader(data))
	if err != nil {
		return err
	}
	for k, v := range hdr {
		if k != "host" {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Authorization", auth)
	resp, err := storeHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		msg := strings.TrimSpace(string(body))
		if i := strings.Index(msg, "<Message>"); i >= 0 {
			if j := strings.Index(msg[i:], "</Message>"); j > 0 {
				msg = msg[i+9 : i+j]
			}
		}
		hint := ""
		if resp.StatusCode == 301 || resp.StatusCode == 400 && strings.Contains(string(body), "AuthorizationHeaderMalformed") {
			hint = " (check the region of the connection)"
		}
		return fmt.Errorf("HTTP %d: %s%s", resp.StatusCode, truncate(msg, 300), hint)
	}
	return nil
}

// NewS3WriteTask dumps the rows of the upstream node to an S3 object.
func (d *DAG) NewS3WriteTask(id string, cfg StoreWriteConfig, opts ...TaskOption) *Task {
	return d.newStoreTask(id, "s3_write", "S3WriteOperator", "s3", cfg,
		func(ctx *TaskContext, name, ctype string, data []byte) (string, error) {
			cr, err := awsCredentialsCtx(ctx, cfg.ConnID)
			if err != nil {
				return "", err
			}
			cr.Region = firstNonEmpty(cfg.Region, cr.Region, "us-east-1")
			if err := s3Put(ctx, cr, cfg.Bucket, name, ctype, data); err != nil {
				return "", fmt.Errorf("upload to s3://%s/%s: %w", cfg.Bucket, name, err)
			}
			return "s3://" + cfg.Bucket + "/" + name, nil
		}, opts...)
}

// storeOutput is the XCom of a dump node: where the file is, plus its format and
// columns (in file order) with a type guessed from the data, so a following
// "BigQuery external table" node can describe the file.
func storeOutput(cfg StoreWriteConfig, uri string, rows []any, size int) map[string]any {
	format := strings.ToLower(strings.TrimSpace(cfg.Format))
	switch format {
	case "":
		format = "csv"
	case "jsonl":
		format = "ndjson"
	}
	cols := cfg.Columns
	if len(cols) == 0 || format != "csv" && format != "tsv" {
		cols = rowColumns(rows)
	}
	schema := make([]any, len(cols))
	for i, c := range cols {
		schema[i] = map[string]any{"name": c, "type": guessBQType(rows, c, format == "ndjson" || format == "json")}
	}
	out := map[string]any{"uri": uri, "rows": len(rows), "bytes": size, "format": format, "columns": schema, "gzip": cfg.Gzip}
	if format == "csv" || format == "tsv" {
		d := ","
		if format == "tsv" {
			d = "\t"
		}
		if s := strings.TrimSpace(cfg.Delim); s != "" {
			switch strings.ToLower(s) {
			case "tab", "\\t":
				d = "\t"
			case "semicolon":
				d = ";"
			case "pipe":
				d = "|"
			case "comma":
				d = ","
			default:
				d = cfg.Delim
			}
		}
		out["delimiter"] = d
	}
	return out
}

// rowColumns: every field of the rows, sorted (the order the csv writer uses).
func rowColumns(rows []any) []string {
	set := map[string]bool{}
	for _, r := range rows {
		if m, ok := r.(map[string]any); ok {
			for k := range m {
				set[k] = true
			}
		}
	}
	cols := make([]string, 0, len(set))
	for k := range set {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

// guessBQType: INT64, FLOAT64, BOOL, JSON (json files only) or STRING.
func guessBQType(rows []any, col string, jsonFile bool) string {
	t := ""
	for _, r := range rows {
		m, _ := r.(map[string]any)
		v, ok := m[col]
		if !ok || v == nil {
			continue
		}
		k := "STRING"
		switch x := v.(type) {
		case bool:
			k = "BOOL"
		case float64:
			k = "FLOAT64"
			if x == math.Trunc(x) && math.Abs(x) < 1<<53 {
				k = "INT64"
			}
		case int, int32, int64:
			k = "INT64"
		case map[string]any, []any:
			k = "STRING"
			if jsonFile {
				k = "JSON"
			}
		}
		switch {
		case t == "" || t == k:
			t = k
		case (t == "INT64" && k == "FLOAT64") || (t == "FLOAT64" && k == "INT64"):
			t = "FLOAT64"
		default:
			return "STRING"
		}
	}
	if t == "" {
		return "STRING"
	}
	return t
}
