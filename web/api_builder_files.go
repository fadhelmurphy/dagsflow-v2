package web

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"dagsflow-go/dag"
)

// Files for the "CSV / Excel file" node: uploaded from the builder into
// <dags>/builder/files/. Only builder.edit users can list, upload or delete.

type fileRow struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Modified string `json:"modified"`
}

const maxUpload = 25 << 20

func (s *Server) uploadsDir() (string, error) {
	d := dag.UploadsPath()
	if d == "" {
		return "", errors.New("uploads are not configured")
	}
	return d, nil
}

func (s *Server) builderFiles(w http.ResponseWriter, r *http.Request) {
	dir, err := s.uploadsDir()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	rows := []fileRow{}
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		if _, err := dag.SafeUploadName(e.Name()); err != nil {
			continue
		}
		if info, err := e.Info(); err == nil {
			rows = append(rows, fileRow{e.Name(), info.Size(), info.ModTime().Format(time.RFC3339)})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Modified > rows[j].Modified })
	writeJSON(w, 200, map[string]any{"files": rows, "max_bytes": maxUpload})
}

func (s *Server) builderFileUpload(w http.ResponseWriter, r *http.Request) {
	dir, err := s.uploadsDir()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxUpload+(1<<20))
	if err := r.ParseMultipartForm(8 << 20); err != nil {
		fail(w, http.StatusRequestEntityTooLarge, "upload failed or larger than %d MB", maxUpload>>20)
		return
	}
	defer r.MultipartForm.RemoveAll()
	src, hdr, err := r.FormFile("file")
	if err != nil {
		fail(w, 400, "send the file in a multipart field named \"file\"")
		return
	}
	defer src.Close()
	name, err := dag.SafeUploadName(hdr.Filename)
	if err != nil {
		fail(w, 400, "%v", err)
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	tmp, err := os.CreateTemp(dir, ".upload-*")
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	defer os.Remove(tmp.Name())
	n, err := io.Copy(tmp, io.LimitReader(src, maxUpload+1))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	if n > maxUpload {
		fail(w, http.StatusRequestEntityTooLarge, "file is larger than %d MB", maxUpload>>20)
		return
	}
	final := filepath.Join(dir, name)
	if err := os.Rename(tmp.Name(), final); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	if _, info, err := dag.ReadGrid(final, "", "", ""); err != nil {
		_ = os.Remove(final)
		fail(w, 422, "%v", err)
		return
	} else {
		s.audit(r, "builder_file_upload", "", "", "", name)
		writeJSON(w, 201, map[string]any{"name": name, "size": n, "format": info.Format, "sheets": info.Sheets})
	}
}

func (s *Server) uploadPath(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	dir, err := s.uploadsDir()
	if err != nil {
		fail(w, 500, "%v", err)
		return "", "", false
	}
	name := r.PathValue("name")
	if clean, err := dag.SafeUploadName(name); err != nil || clean != name {
		fail(w, 400, "bad file name")
		return "", "", false
	}
	p := filepath.Join(dir, name)
	if _, err := os.Stat(p); err != nil {
		fail(w, 404, "file %q not found", name)
		return "", "", false
	}
	return p, name, true
}

func (s *Server) builderFileDelete(w http.ResponseWriter, r *http.Request) {
	p, name, ok := s.uploadPath(w, r)
	if !ok {
		return
	}
	if err := os.Remove(p); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	s.audit(r, "builder_file_delete", "", "", "", name)
	w.WriteHeader(http.StatusNoContent)
}

// builderFilePreview shows the raw top-left of the sheet (with A1 positions) and
// what the node would read with the given range.
func (s *Server) builderFilePreview(w http.ResponseWriter, r *http.Request) {
	p, name, ok := s.uploadPath(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	grid, info, err := dag.ReadGrid(p, "", q.Get("sheet"), q.Get("delimiter"))
	if err != nil {
		writeJSON(w, 200, map[string]any{"name": name, "sheets": info.Sheets, "error": err.Error()})
		return
	}
	const maxR, maxC = 15, 20
	raw := [][]any{}
	for i := 0; i < len(grid) && i < maxR; i++ {
		row := grid[i]
		if row == nil {
			row = []any{}
		}
		if len(row) > maxC {
			row = row[:maxC]
		}
		raw = append(raw, row)
	}
	res := map[string]any{"name": name, "format": info.Format, "sheets": info.Sheets, "total_rows": len(grid), "raw": raw}
	rng := q.Get("range")
	if rng != "" || q.Get("no_header") != "" || q.Get("keep_headers") != "" {
		rows, err := dag.PreviewRows(grid, rng, q.Get("no_header") == "1", q.Get("keep_headers") == "1", 5)
		if err != nil {
			res["range_error"] = err.Error()
		} else {
			res["columns"], res["sample"] = rows.Columns, rows.Sample
			res["row_count"] = rows.Count
		}
	}
	writeJSON(w, 200, res)
}
