package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// JSON store: large XCom values are files next to the run, under
// state/xcom/<dag>/<run>/, so run files stay small and lists of runs (DAG
// list, grid) don't read them.

func (j *jsonBackend) xcomDir(dagID, runID string) string {
	return j.state("xcom", dagID, safeName(runID))
}

// blobFile: file name of one slot+key.
func blobFile(bk string) string {
	h := sha256.Sum256([]byte(bk))
	return hex.EncodeToString(h[:12])
}

type jsonBlobMeta struct {
	Slot    string          `json:"slot"`
	Key     string          `json:"key"`
	Preview json.RawMessage `json:"preview,omitempty"`
}

func writeFileAtomic(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp%d", path, time.Now().UnixNano())
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return renameReplace(tmp, path)
}

type jsonBlobs struct{ j *jsonBackend }

func (s jsonBlobs) putBlob(r *DagRun, b xblob) error {
	dir, f := s.j.xcomDir(r.DagID, r.RunID), blobFile(blobKey(b.slot, b.key))
	if err := writeFileAtomic(filepath.Join(dir, f+".json"), b.data); err != nil {
		return err
	}
	meta, _ := json.Marshal(jsonBlobMeta{Slot: b.slot, Key: b.key, Preview: b.preview})
	return writeFileAtomic(filepath.Join(dir, f+".meta"), meta)
}

func (s jsonBlobs) dropBlobsExcept(r *DagRun, keep map[string]bool) error {
	files := map[string]bool{}
	for bk := range keep {
		files[blobFile(bk)] = true
	}
	dir := s.j.xcomDir(r.DagID, r.RunID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		base := e.Name()
		if i := strings.IndexByte(base, '.'); i > 0 {
			base = base[:i]
		}
		if !files[base] {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

func (j *jsonBackend) encodeRun(r *DagRun) ([]byte, error) {
	b, err := splitRun(r, jsonBlobs{j})
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := json.Indent(&out, b, "", "  "); err != nil {
		return b, nil
	}
	return out.Bytes(), nil
}

// resolveXCom reads the large values back in place of their references.
func (j *jsonBackend) resolveXCom(r *DagRun) error {
	dir := j.xcomDir(r.DagID, r.RunID)
	for slot, kv := range r.XCom {
		for k, v := range kv {
			if !IsXComRef(v) {
				continue
			}
			bk := blobKey(slot, k)
			b, err := os.ReadFile(filepath.Join(dir, blobFile(bk)+".json"))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue // removed (cleanup): the reference stays
				}
				return err
			}
			var val any
			if err := json.Unmarshal(b, &val); err != nil {
				return err
			}
			kv[k] = val
			h := sha256.Sum256(b) // the file is what encodeRun marshalled: same checksum
			if r.blobs == nil {
				r.blobs = map[string]string{}
			}
			r.blobs[bk] = hex.EncodeToString(h[:])
			if id := identity(val); id.ok {
				if r.blobSeen == nil {
					r.blobSeen = map[string]valueID{}
				}
				id.size = len(b)
				r.blobSeen[bk] = id
			}
		}
	}
	return nil
}

func (j *jsonBackend) loadRunLite(dagID, runID string) (*DagRun, error) {
	var r DagRun
	if err := readJSON(j.runPath(dagID, runID), &r); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("dag run %s/%s not found", dagID, runID)
		}
		return nil, err
	}
	return &r, nil
}

func (j *jsonBackend) xcomPreviews(r *DagRun) (map[string]map[string]any, error) {
	out := map[string]map[string]any{}
	dir := j.xcomDir(r.DagID, r.RunID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".meta") {
			continue
		}
		var m jsonBlobMeta
		if err := readJSON(filepath.Join(dir, e.Name()), &m); err != nil {
			continue
		}
		var p map[string]any
		_ = json.Unmarshal(m.Preview, &p)
		out[blobKey(m.Slot, m.Key)] = p
	}
	return out, nil
}

func (j *jsonBackend) xcomValue(dagID, runID, slot, key string) (any, error) {
	b, err := os.ReadFile(filepath.Join(j.xcomDir(dagID, runID), blobFile(blobKey(slot, key))+".json"))
	if err != nil {
		return nil, err
	}
	var v any
	return v, json.Unmarshal(b, &v)
}

// compactXCom rewrites finished runs whose file still holds large values.
func (j *jsonBackend) compactXCom(logf func(string, ...any)) (int, error) {
	ids, err := j.dagIDs()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		entries, _ := os.ReadDir(j.runsDir(id))
		for _, e := range entries {
			fi, err := e.Info()
			if err != nil || filepath.Ext(e.Name()) != ".json" || fi.Size() <= int64(xcomInlineMax()) {
				continue
			}
			var r DagRun
			if err := readJSON(filepath.Join(j.runsDir(id), e.Name()), &r); err != nil || r.State == RunRunning || r.State == RunQueued {
				continue
			}
			if err := j.resolveXCom(&r); err != nil {
				return n, err
			}
			if err := j.saveRun(&r, ""); err != nil {
				return n, err
			}
			n++
			if logf != nil {
				logf("%s/%s: %d MB moved out", id, r.RunID, fi.Size()>>20)
			}
		}
	}
	return n, nil
}
