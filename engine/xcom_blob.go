package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
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

// encodeRun marshals the run for its row, writing large XCom values to
// xcom_blob (only the ones that changed) and dropping blobs no longer used.
func (p *pgBackend) encodeRun(ctx context.Context, r *DagRun) ([]byte, error) {
	max := xcomInlineMax()
	type blob struct {
		slot, key string
		data      []byte
	}
	var put []blob
	sums := map[string]string{}
	var xc map[string]map[string]any
	if r.XCom != nil {
		xc = make(map[string]map[string]any, len(r.XCom))
	}
	for slot, kv := range r.XCom {
		m := make(map[string]any, len(kv))
		for k, v := range kv {
			bk := blobKey(slot, k)
			if IsXComRef(v) { // loaded from a list of runs: the blob is already stored
				m[k] = v
				sums[bk] = r.blobs[bk]
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
				put = append(put, blob{slot, k, b})
			}
			sums[bk] = sum
			m[k] = map[string]any{xcomRefKey: len(b)}
		}
		xc[slot] = m
	}
	for _, b := range put {
		if _, err := p.pool.Exec(ctx, `INSERT INTO xcom_blob (dag_id, run_id, slot, key, data) VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (dag_id, run_id, slot, key) DO UPDATE SET data = EXCLUDED.data`, r.DagID, r.RunID, b.slot, b.key, b.data); err != nil {
			return nil, err
		}
	}
	if len(r.blobs) > 0 { // drop blobs of values that were removed or became small
		var slots, keys []string
		for bk := range sums {
			for i := 0; i < len(bk); i++ {
				if bk[i] == 0 {
					slots, keys = append(slots, bk[:i]), append(keys, bk[i+1:])
					break
				}
			}
		}
		stale := false
		for bk := range r.blobs {
			if _, ok := sums[bk]; !ok {
				stale = true
			}
		}
		if stale {
			if _, err := p.pool.Exec(ctx, `DELETE FROM xcom_blob WHERE dag_id=$1 AND run_id=$2
				AND (slot, key) NOT IN (SELECT * FROM unnest($3::text[], $4::text[]))`, r.DagID, r.RunID, slots, keys); err != nil {
				return nil, err
			}
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
		}
	}
	return rows.Err()
}

// ResolveXCom loads the large XCom values of a run read from a list of runs.
func (s *Store) ResolveXCom(r *DagRun) error {
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
