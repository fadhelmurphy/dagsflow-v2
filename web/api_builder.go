package web

import (
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"dagsflow-go/dag"
	"dagsflow-go/engine"
)

// Visual builder API. Workflows are plain YAML DAG files in <dags>/builder/.

func (s *Server) builderPath(id string) string { return filepath.Join(s.BuilderDir, id+".yaml") }

func (s *Server) builderNodes(w http.ResponseWriter, r *http.Request) {
	s.reloadNodes()
	writeJSON(w, 200, map[string]any{"nodes": dag.AllNodes(), "common": dag.CommonFields, "node_errors": dag.CustomNodeErrors()})
}

type wfRow struct {
	DagID    string   `json:"dag_id"`
	File     string   `json:"file"`
	Editable bool     `json:"editable"`
	Tasks    int      `json:"tasks"`
	Schedule string   `json:"schedule"`
	Tags     []string `json:"tags"`
	Desc     string   `json:"description"`
	Error    string   `json:"error,omitempty"`
}

func isYAMLFile(p string) bool {
	e := strings.ToLower(filepath.Ext(p))
	return e == ".yaml" || e == ".yml"
}

func (s *Server) inBuilderDir(p string) bool {
	if s.BuilderDir == "" || p == "" {
		return false
	}
	abs, err1 := filepath.Abs(p)
	dir, err2 := filepath.Abs(s.BuilderDir)
	return err1 == nil && err2 == nil && filepath.Dir(abs) == dir
}

func (s *Server) builderList(w http.ResponseWriter, r *http.Request) {
	a := accessFrom(r)
	rows := []wfRow{}
	seen := map[string]bool{}
	for _, d := range dag.List() {
		if d.IsFileSource() {
			seen[d.File] = true
		}
		if !d.IsFileSource() || !isYAMLFile(d.File) || !a.DAG(engine.ActDagRead, d.ID) {
			continue
		}
		rows = append(rows, wfRow{DagID: d.ID, File: d.File, Editable: s.inBuilderDir(d.File) && a.DAG(engine.ActDagEdit, d.ID),
			Tasks: len(d.Tasks()), Schedule: d.Schedule, Tags: d.Tags, Desc: d.Description})
	}
	// files in the builder folder that failed to load
	if entries, err := os.ReadDir(s.BuilderDir); err == nil {
		errs := dag.ImportErrors()
		for _, e := range entries {
			p := filepath.Join(s.BuilderDir, e.Name())
			id := strings.TrimSuffix(e.Name(), filepath.Ext(e.Name()))
			if e.IsDir() || !isYAMLFile(p) || seen[p] || !a.DAG(engine.ActDagRead, id) {
				continue
			}
			msg := "not loaded yet"
			if err, ok := errs[p]; ok {
				msg = err.Error()
			}
			rows = append(rows, wfRow{DagID: id, File: p, Editable: true, Error: msg})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].DagID < rows[j].DagID })
	writeJSON(w, 200, map[string]any{"workflows": rows, "builder_dir": s.BuilderDir})
}

type graphBody struct {
	dag.Graph
	Etag string `json:"etag"`
}

func issueMsg(iss []dag.GraphIssue) string {
	for _, i := range iss {
		if !i.Warning {
			if i.Node != "" {
				return i.Node + ": " + i.Message
			}
			return i.Message
		}
	}
	return ""
}

func (s *Server) builderValidate(w http.ResponseWriter, r *http.Request) {
	s.reloadNodes()
	var g dag.Graph
	if err := readBody(r, &g); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	y, iss := g.Validate()
	if iss == nil {
		iss = []dag.GraphIssue{}
	}
	writeJSON(w, 200, map[string]any{"ok": y != nil, "issues": iss, "yaml": string(y)})
}

// existingFile finds the file that defines dag id (registered or not).
func (s *Server) existingFile(id string) (path string, registered bool) {
	if d, ok := dag.Get(id); ok {
		return d.File, true
	}
	p := s.builderPath(id)
	if _, err := os.Stat(p); err == nil {
		return p, false
	}
	return "", false
}

func (s *Server) builderGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("dag")
	a := accessFrom(r)
	file, registered := s.existingFile(id)
	if file == "" {
		fail(w, 404, "no workflow %q", id)
		return
	}
	if !a.DAG(engine.ActDagRead, id) {
		fail(w, 403, "you cannot read DAG %s", id)
		return
	}
	if d, ok := dag.Get(id); ok && !d.IsFileSource() {
		fail(w, 422, "%s is written in Go and compiled into the binary; the builder only opens YAML DAGs", id)
		return
	}
	if !isYAMLFile(file) {
		fail(w, 422, "%s is defined in %s; the builder only opens YAML DAGs", id, filepath.Base(file))
		return
	}
	data, err := os.ReadFile(file)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	g, err := dag.ParseGraph(data)
	if err != nil {
		fail(w, 422, "cannot open in the builder: %v", err)
		return
	}
	editable := s.inBuilderDir(file) && a.DAG(engine.ActDagEdit, id)
	writeJSON(w, 200, map[string]any{"graph": g, "etag": dag.Etag(data), "editable": editable, "file": file,
		"loaded": registered, "paused": s.Store.GetDagMeta(id).IsPaused})
}

func (s *Server) builderPut(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("dag")
	s.reloadNodes()
	var b graphBody
	if err := readBody(r, &b); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	b.Graph.DagID = id
	a := accessFrom(r)
	if !a.DAG(engine.ActDagEdit, id) {
		fail(w, 403, "you cannot edit DAG %s", id)
		return
	}
	if !dag.ValidDagID(id) {
		fail(w, 400, "invalid DAG id")
		return
	}
	y, iss := b.Graph.Validate()
	if y == nil {
		writeJSON(w, 422, map[string]any{"error": issueMsg(iss), "issues": iss})
		return
	}
	s.builderMu.Lock()
	defer s.builderMu.Unlock()
	path := s.builderPath(id)
	if d, ok := dag.Get(id); ok && filepath.Clean(d.File) != filepath.Clean(path) {
		fail(w, 409, "DAG %s is already defined in %s; choose another id", id, d.File)
		return
	}
	cur, err := os.ReadFile(path)
	creating := errors.Is(err, fs.ErrNotExist)
	switch {
	case err == nil:
		if b.Etag != dag.Etag(cur) {
			fail(w, 409, "this workflow was changed by someone else (or the id is already taken). Reload it, or save under another id")
			return
		}
	case errors.Is(err, fs.ErrNotExist):
		if b.Etag != "" {
			fail(w, 409, "this workflow no longer exists")
			return
		}
	default:
		fail(w, 500, "%v", err)
		return
	}
	if err := os.MkdirAll(s.BuilderDir, 0o755); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	tmp, err := os.CreateTemp(s.BuilderDir, ".tmp-*")
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	_, werr := tmp.Write(y)
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(tmp.Name())
		fail(w, 500, "write failed: %v %v", werr, cerr)
		return
	}
	_ = os.Chmod(tmp.Name(), 0o644)
	if err := os.Rename(tmp.Name(), path); err != nil {
		_ = os.Remove(tmp.Name())
		fail(w, 500, "%v", err)
		return
	}
	if s.Reload != nil {
		if err := s.Reload(path); err != nil {
			fail(w, 500, "saved, but reload failed: %v", err)
			return
		}
	}
	// a new scheduled workflow starts paused: nothing runs until it is switched on
	meta := s.Store.GetDagMeta(id)
	if creating && hasSchedule(b.Graph.Settings) && !meta.IsPaused {
		meta.IsPaused = true
		_ = s.Store.SaveDagMeta(meta)
	}
	s.audit(r, "builder_save", id, "", "", filepath.Base(path))
	_, loaded := dag.Get(id)
	writeJSON(w, 200, map[string]any{"dag_id": id, "etag": dag.Etag(y), "file": path, "loaded": loaded,
		"issues": iss, "paused": meta.IsPaused, "created": creating})
}

func (s *Server) builderDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("dag")
	if !dag.ValidDagID(id) {
		fail(w, 400, "invalid DAG id")
		return
	}
	if !accessFrom(r).DAG(engine.ActDagDelete, id) {
		fail(w, 403, "you cannot delete DAG %s", id)
		return
	}
	s.builderMu.Lock()
	defer s.builderMu.Unlock()
	path := s.builderPath(id)
	if d, ok := dag.Get(id); ok && filepath.Clean(d.File) != filepath.Clean(path) {
		fail(w, 409, "DAG %s is not a builder workflow (it lives in %s)", id, d.File)
		return
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			fail(w, 404, "no workflow %q", id)
			return
		}
		fail(w, 500, "%v", err)
		return
	}
	if s.Reload != nil {
		_ = s.Reload(path)
	}
	s.audit(r, "builder_delete", id, "", "", filepath.Base(path))
	writeJSON(w, 200, map[string]any{"deleted": id})
}

func hasSchedule(settings map[string]any) bool {
	if sc, _ := settings["schedule"].(string); strings.TrimSpace(sc) != "" && sc != "@once" && sc != "None" {
		return true
	}
	ds, _ := settings["schedule_datasets"].([]any)
	return len(ds) > 0
}
