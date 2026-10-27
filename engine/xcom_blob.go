package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"time"
)

// Large XCom values (Postgres metastore) are stored in xcom_blob, not in the
// run row: the row keeps a small reference {"$xcom_ref": size}. Lists of runs
// (DAG list, grid, scheduler scans) then read small rows; loading one run
// (task execution, run details, XCom tab) puts the values back.

const xcomRefKey = "$xcom_ref"

// xcomInlineMax: values whose JSON is larger go to xcom_blob (DAGSFLOW_XCOM_INLINE_BYTES, default 64 KB).
func xcomInlineMax() int {
	if n, err := strconv.Atoi(os.Getenv("DAGSFLOW_XCOM_INLINE_BYTES")); err == nil && n > 0 {
		return n
	}
	return 64 << 10
}

// IsXComRef reports whether v is the reference left in place of a large value.
func IsXComRef(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}
	_, ok = m[xcomRefKey]
	return ok
}

func blobKey(slot, key string) string { return slot + "\x00" + key }

// xblob is one large XCom value to store apart from the run.
type xblob struct {
	slot, key     string
	data, preview []byte
}

// blobSink stores large XComs: Postgres (xcom_blob) or the JSON store
// (files next to the run).
type blobSink interface {
	putBlob(r *DagRun, b xblob) error
	// dropBlobsExcept removes the run's blobs whose slot+key is not in keep
	dropBlobsExcept(r *DagRun, keep map[string]bool) error
}

// splitRun marshals the run with large XCom values replaced by references,
// storing those values (only the ones that changed) and dropping blobs no
// longer used.
func splitRun(r *DagRun, sink blobSink) ([]byte, error) {
	max := xcomInlineMax()
	var put []xblob
	sums := map[string]string{}
	var xc map[string]map[string]any
	if r.XCom != nil {
		xc = make(map[string]map[string]any, len(r.XCom))
	}
	for slot, kv := range r.XCom {
		m := make(map[string]any, len(kv))
		for k, v := range kv {
			bk := blobKey(slot, k)
			if IsXComRef(v) { // loaded without its large values: the blob is already stored
				m[k] = v
				sums[bk] = r.blobs[bk]
				continue
			}
			id := identity(v)
			if seen, ok := r.blobSeen[bk]; ok && id.ok && seen.ptr == id.ptr && seen.n == id.n && r.blobs[bk] != "" {
				// the same large value as at the last save: already stored
				sums[bk] = r.blobs[bk]
				m[k] = map[string]any{xcomRefKey: seen.size}
				continue
			}
			b, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			if len(b) <= max {
				m[k] = v
				continue
			}
			h := sha256.Sum256(b)
			sum := hex.EncodeToString(h[:])
			if r.blobs[bk] != sum {
				put = append(put, xblob{slot, k, b, previewOf(v)})
			}
			sums[bk] = sum
			if id.ok {
				if r.blobSeen == nil {
					r.blobSeen = map[string]valueID{}
				}
				id.size = len(b)
				r.blobSeen[bk] = id
			}
			m[k] = map[string]any{xcomRefKey: len(b)}
		}
		xc[slot] = m
	}
	for _, b := range put {
		if err := sink.putBlob(r, b); err != nil {
			return nil, err
		}
	}
	stale := false // blobs of values that were removed or became small
	for bk := range r.blobs {
		if _, ok := sums[bk]; !ok {
			stale = true
		}
	}
	if stale {
		keep := map[string]bool{}
		for bk := range sums {
			keep[bk] = true
		}
		if err := sink.dropBlobsExcept(r, keep); err != nil {
			return nil, err
		}
	}
	if len(sums) > 0 {
		r.blobs = sums
	} else {
		r.blobs = nil
	}
	cp := *r
	cp.XCom = xc
	return json.Marshal(&cp)
}

func splitBlobKey(bk string) (string, string) {
	for i := 0; i < len(bk); i++ {
		if bk[i] == 0 {
			return bk[:i], bk[i+1:]
		}
	}
	return bk, ""
}

// encodeRun: splitRun into xcom_blob.
func (p *pgBackend) encodeRun(ctx context.Context, r *DagRun) ([]byte, error) {
	return splitRun(r, pgBlobs{p, ctx})
}

type pgBlobs struct {
	p   *pgBackend
	ctx context.Context
}

func (s pgBlobs) putBlob(r *DagRun, b xblob) error {
	_, err := s.p.pool.Exec(s.ctx, `INSERT INTO xcom_blob (dag_id, run_id, slot, key, data, preview) VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (dag_id, run_id, slot, key) DO UPDATE SET data = EXCLUDED.data, preview = EXCLUDED.preview`, r.DagID, r.RunID, b.slot, b.key, b.data, b.preview)
	return err
}

func (s pgBlobs) dropBlobsExcept(r *DagRun, keep map[string]bool) error {
	var slots, keys []string
	for bk := range keep {
		sl, k := splitBlobKey(bk)
		slots, keys = append(slots, sl), append(keys, k)
	}
	_, err := s.p.pool.Exec(s.ctx, `DELETE FROM xcom_blob WHERE dag_id=$1 AND run_id=$2
		AND (slot, key) NOT IN (SELECT * FROM unnest($3::text[], $4::text[]))`, r.DagID, r.RunID, slots, keys)
	return err
}

// valueID identifies a JSON value without reading it: XCom values are
// replaced, never changed in place, so the same backing array or map (and
// length) means the same value.
type valueID struct {
	ptr  uintptr
	n    int
	size int
	ok   bool
	// keep holds the value itself: while it is referenced here its memory
	// can't be reused by a new value that would then look identical
	keep any
}

func identity(v any) valueID {
	switch x := v.(type) {
	case []any:
		if len(x) == 0 {
			return valueID{}
		}
		return valueID{ptr: reflect.ValueOf(x).Pointer(), n: len(x), ok: true, keep: v}
	case map[string]any:
		if len(x) == 0 {
			return valueID{}
		}
		return valueID{ptr: reflect.ValueOf(x).Pointer(), n: len(x), ok: true, keep: v}
	}
	return valueID{}
}

// PreviewRows is how many rows of a large XCom the run details show.
const PreviewRows = 50

// previewOf: {"rows": n, "sample": first rows} for a list, else {}.
func previewOf(v any) []byte {
	out := map[string]any{}
	if list, ok := v.([]any); ok {
		out["rows"] = len(list)
		out["sample"] = list[:min(len(list), PreviewRows)]
	}
	b, _ := json.Marshal(out)
	return b
}

// resolveXCom puts the large values of a run back in place of their references.
func (p *pgBackend) resolveXCom(ctx context.Context, r *DagRun) error {
	has := false
	for _, kv := range r.XCom {
		for _, v := range kv {
			if IsXComRef(v) {
				has = true
			}
		}
	}
	if !has {
		return nil
	}
	rows, err := p.pool.Query(ctx, `SELECT slot, key, data FROM xcom_blob WHERE dag_id=$1 AND run_id=$2`, r.DagID, r.RunID)
	if err != nil {
		return err
	}
	defer rows.Close()
	r.blobs = map[string]string{}
	for rows.Next() {
		var slot, key string
		var data []byte
		if err := rows.Scan(&slot, &key, &data); err != nil {
			return err
		}
		if kv := r.XCom[slot]; kv != nil && IsXComRef(kv[key]) {
			var v any
			if err := json.Unmarshal(data, &v); err != nil {
				return err
			}
			kv[key] = v
			b, _ := json.Marshal(v) // checksum of the normalized JSON, as encodeRun computes it
			h := sha256.Sum256(b)
			r.blobs[blobKey(slot, key)] = hex.EncodeToString(h[:])
			if id := identity(v); id.ok { // saves of this run won't encode it again
				if r.blobSeen == nil {
					r.blobSeen = map[string]valueID{}
				}
				id.size = len(b)
				r.blobSeen[blobKey(slot, key)] = id
			}
		}
	}
	return rows.Err()
}

// LoadRunLite loads a run without its large XCom values (kept as
// references): for pages that show states, not values.
func (s *Store) LoadRunLite(dagID, runID string) (*DagRun, error) {
	switch b := s.be().(type) {
	case *pgBackend:
		return b.loadRunLite(dagID, runID)
	case *jsonBackend:
		return b.loadRunLite(dagID, runID)
	}
	return s.LoadRun(dagID, runID)
}

// XComPreviews: for each large XCom of a run (slot+"\x00"+key), its row count
// and first rows, without reading the values.
func (s *Store) XComPreviews(r *DagRun) (map[string]map[string]any, error) {
	if j, ok := s.be().(*jsonBackend); ok {
		return j.xcomPreviews(r)
	}
	p, ok := s.be().(*pgBackend)
	if !ok {
		return nil, nil
	}
	ctx, cancel := pgctx()
	defer cancel()
	// blobs written before previews were stored: compute theirs once
	if _, err := p.pool.Exec(ctx, `UPDATE xcom_blob SET preview = CASE WHEN jsonb_typeof(data) = 'array'
		THEN jsonb_build_object('rows', jsonb_array_length(data), 'sample', jsonb_path_query_array(data, '$[0 to 49]')) ELSE '{}'::jsonb END
		WHERE dag_id=$1 AND run_id=$2 AND preview IS NULL`, r.DagID, r.RunID); err != nil {
		return nil, err
	}
	rows, err := p.pool.Query(ctx, `SELECT slot, key, preview FROM xcom_blob WHERE dag_id=$1 AND run_id=$2`, r.DagID, r.RunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]map[string]any{}
	for rows.Next() {
		var slot, key string
		var b []byte
		if err := rows.Scan(&slot, &key, &b); err != nil {
			return nil, err
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		out[blobKey(slot, key)] = m
	}
	return out, rows.Err()
}

// XComValue reads one XCom value whole (the only large one read).
func (s *Store) XComValue(dagID, runID, slot, key string) (any, bool, error) {
	r, err := s.LoadRunLite(dagID, runID)
	if err != nil {
		return nil, false, err
	}
	v, ok := r.XCom[slot][key]
	if !ok || !IsXComRef(v) {
		return v, ok, nil
	}
	if j, ok := s.be().(*jsonBackend); ok {
		out, err := j.xcomValue(dagID, runID, slot, key)
		return out, err == nil, err
	}
	p := s.be().(*pgBackend)
	ctx, cancel := pgctx()
	defer cancel()
	var b []byte
	if err := p.pool.QueryRow(ctx, `SELECT data FROM xcom_blob WHERE dag_id=$1 AND run_id=$2 AND slot=$3 AND key=$4`, dagID, runID, slot, key).Scan(&b); err != nil {
		return nil, false, err
	}
	var out any
	return out, true, json.Unmarshal(b, &out)
}

// ResolveXCom loads the large XCom values of a run read from a list of runs.
func (s *Store) ResolveXCom(r *DagRun) error {
	if j, ok := s.be().(*jsonBackend); ok {
		return j.resolveXCom(r)
	}
	if p, ok := s.be().(*pgBackend); ok {
		ctx, cancel := pgctx()
		defer cancel()
		return p.resolveXCom(ctx, r)
	}
	return nil
}

// CompactXCom moves large XCom values still stored inside run rows (runs saved
// before xcom_blob existed) out to xcom_blob. Running runs are skipped; it is
// safe to run any time and to run again. Returns the runs rewritten.
func (s *Store) CompactXCom(logf func(string, ...any)) (int, error) {
	if j, ok := s.be().(*jsonBackend); ok {
		return j.compactXCom(logf)
	}
	p, ok := s.be().(*pgBackend)
	if !ok {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT dag_id, run_id FROM dag_run
		WHERE owner IS NULL AND octet_length(coalesce(doc->'xcom', '{}'::jsonb)::text) > $1`, xcomInlineMax())
	if err != nil {
		return 0, err
	}
	var keys [][2]string
	for rows.Next() {
		var d, r string
		if err := rows.Scan(&d, &r); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, [2]string{d, r})
	}
	rows.Close()
	n := 0
	for _, k := range keys {
		run, err := s.LoadRun(k[0], k[1])
		if err != nil {
			continue
		}
		if err := s.SaveRun(run); err != nil {
			logf("skip %s/%s: %v", k[0], k[1], err)
			continue
		}
		n++
		if n%100 == 0 {
			logf("%d / %d runs", n, len(keys))
		}
	}
	return n, nil
}
