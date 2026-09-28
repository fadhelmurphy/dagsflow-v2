package engine

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// AuditEvent ~ Airflow's Event Log (log table): who did what, when.
type AuditEvent struct {
	Time     time.Time `json:"when"`
	User     string    `json:"owner"`
	Event    string    `json:"event"`
	DagID    string    `json:"dag_id,omitempty"`
	RunID    string    `json:"run_id,omitempty"`
	TaskID   string    `json:"task_id,omitempty"`
	MapIndex *int      `json:"map_index,omitempty"`
	Extra    string    `json:"extra,omitempty"`
}

var auditMu sync.Mutex

func (s *Store) auditPath() string { return filepath.Join(s.Home, "state", "audit.log") }

// Audit appends one event (JSON line). O_APPEND keeps concurrent writers from
// different processes from interleaving small records.
func (s *Store) Audit(ev AuditEvent) {
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	if ev.User == "" {
		ev.User = "system"
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	auditMu.Lock()
	defer auditMu.Unlock()
	_ = os.MkdirAll(filepath.Dir(s.auditPath()), 0o755)
	f, err := os.OpenFile(s.auditPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(b, '\n'))
}

// AuditEvents returns the newest events first, filtered by the non-empty fields.
func (s *Store) AuditEvents(dagID, runID, taskID string, limit int) ([]AuditEvent, error) {
	f, err := os.Open(s.auditPath())
	if os.IsNotExist(err) {
		return []AuditEvent{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	const window = 8 << 20 // only scan the last 8MB
	if st, err := f.Stat(); err == nil && st.Size() > window {
		_, _ = f.Seek(st.Size()-window, io.SeekStart)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var all []AuditEvent
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		var ev AuditEvent
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue
		}
		if (dagID == "" || ev.DagID == dagID) && (runID == "" || ev.RunID == runID) && (taskID == "" || ev.TaskID == taskID) {
			all = append(all, ev)
		}
	}
	out := []AuditEvent{}
	for i := len(all) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, all[i])
	}
	return out, nil
}

// ---- reparse requests ----------------------------------------------------------

// RequestReparse asks every process (scheduler, webservers) to reload a DAG
// source even if its files did not change.
func (s *Store) RequestReparse(source string) error {
	m := map[string]time.Time{}
	return s.updateTable("reparse_requests", &m, func() error {
		m[source] = time.Now()
		return nil
	})
}

func (s *Store) ReparseRequests() map[string]time.Time {
	m := map[string]time.Time{}
	_ = s.readTable("reparse_requests", &m)
	return m
}

// ReparseRequestsChanged reports the modification time of the request table.
func (s *Store) ReparseRequestsStamp() time.Time {
	if fi, err := os.Stat(s.tablePath("reparse_requests")); err == nil {
		return fi.ModTime()
	}
	return time.Time{}
}
