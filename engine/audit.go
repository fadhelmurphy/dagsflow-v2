package engine

import "time"

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

// Audit records one event.
func (s *Store) Audit(ev AuditEvent) {
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	if ev.User == "" {
		ev.User = "system"
	}
	s.be().audit(ev)
}

// AuditEvents returns the newest events first, filtered by the non-empty fields.
func (s *Store) AuditEvents(dagID, runID, taskID string, limit int) ([]AuditEvent, error) {
	return s.be().auditEvents(dagID, runID, taskID, limit)
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
	return s.be().tableStamp("reparse_requests")
}
