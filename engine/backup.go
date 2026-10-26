package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Backup is a portable dump of the metadata (any backend → any backend):
// `db export` / `db import`, also used to move from JSON files to Postgres.
// Variables and connections stay encrypted when DAGSFLOW_FERNET_KEY is set.
// Task logs are not included.
type Backup struct {
	Format     int                        `json:"format"`
	ExportedAt time.Time                  `json:"exported_at"`
	Backend    string                     `json:"backend"`
	Tables     map[string]json.RawMessage `json:"tables"`
	DagMeta    []*DagMeta                 `json:"dag_meta"`
	Runs       []*DagRun                  `json:"dag_runs"`
	Audit      []AuditEvent               `json:"event_logs"`
	Catalog    *CatalogBackup             `json:"catalog,omitempty"`
}

// CatalogBackup: the data catalog (datasets, columns, schema history, lineage).
type CatalogBackup struct {
	Tables []*CatalogTable `json:"tables"`
	Edges  []LineageEdge   `json:"edges"`
}

var backupTables = []string{"variables", "connections", "pools", "users", "datasets", "reparse_requests", "catalog_sync"}

// Export dumps the metadata. auditLimit caps the event log (newest first).
func (s *Store) Export(auditLimit int) (*Backup, error) {
	b := &Backup{Format: 1, ExportedAt: time.Now().UTC(), Backend: s.Backend(), Tables: map[string]json.RawMessage{}}
	for _, name := range backupTables {
		var raw json.RawMessage
		if err := s.readTable(name, &raw); err != nil {
			return nil, fmt.Errorf("table %s: %w", name, err)
		}
		if len(raw) > 0 {
			b.Tables[name] = raw
		}
	}
	ids, err := s.DagIDs()
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		b.DagMeta = append(b.DagMeta, s.GetDagMeta(id))
		runs, err := s.ListRuns(id)
		if err != nil {
			return nil, fmt.Errorf("runs of %s: %w", id, err)
		}
		for _, r := range runs { // a backup holds the values, not references to them
			if err := s.ResolveXCom(r); err != nil {
				return nil, fmt.Errorf("xcom of %s/%s: %w", id, r.RunID, err)
			}
		}
		b.Runs = append(b.Runs, runs...)
	}
	tables, edges, err := s.catalog().dump()
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	if len(tables)+len(edges) > 0 {
		b.Catalog = &CatalogBackup{Edges: edges, Tables: make([]*CatalogTable, 0, len(tables))}
		for _, t := range tables {
			b.Catalog.Tables = append(b.Catalog.Tables, t)
		}
		sort.Slice(b.Catalog.Tables, func(i, j int) bool { return b.Catalog.Tables[i].ID < b.Catalog.Tables[j].ID })
	}
	ev, err := s.AuditEvents("", "", "", auditLimit)
	if err != nil {
		return nil, err
	}
	for i := len(ev) - 1; i >= 0; i-- { // oldest first, so import keeps the order
		b.Audit = append(b.Audit, ev[i])
	}
	return b, nil
}

// ImportStats: what Import wrote.
type ImportStats struct {
	Tables, DagMeta, Runs, SkippedRuns, Audit, CatalogTables, CatalogEdges int
}

// Import restores a backup. Tables and DAG metadata are replaced; runs that
// already exist are kept (skipped). Running runs are imported as queued so a
// scheduler resumes them.
func (s *Store) Import(b *Backup) (ImportStats, error) {
	var st ImportStats
	if b.Format != 1 {
		return st, fmt.Errorf("unsupported backup format %d", b.Format)
	}
	for name, raw := range b.Tables {
		var cur json.RawMessage
		if err := s.updateTable(name, &cur, func() error { cur = raw; return nil }); err != nil {
			return st, fmt.Errorf("table %s: %w", name, err)
		}
		st.Tables++
	}
	for _, m := range b.DagMeta {
		if err := s.SaveDagMeta(m); err != nil {
			return st, err
		}
		st.DagMeta++
	}
	for _, r := range b.Runs {
		r.version, r.owned = 0, false
		if r.State == RunRunning {
			r.State = RunQueued
		}
		ok, err := s.CreateRun(r)
		if err != nil {
			return st, fmt.Errorf("run %s/%s: %w", r.DagID, r.RunID, err)
		}
		if ok {
			st.Runs++
		} else {
			st.SkippedRuns++
		}
	}
	for _, ev := range b.Audit {
		s.Audit(ev)
		st.Audit++
	}
	if c := b.Catalog; c != nil { // as it was: no merge, no new schema changes
		cs := s.catalog()
		defer s.dropCatalogCache()
		for start := 0; start < len(c.Tables); start += 2000 {
			batch := c.Tables[start:min(start+2000, len(c.Tables))]
			ids := make([]string, len(batch))
			for i, t := range batch {
				ids[i] = t.ID
			}
			if err := cs.updateTables(ids, func(m map[string]*CatalogTable) error {
				for _, t := range batch {
					m[t.ID] = t
				}
				return nil
			}); err != nil {
				return st, fmt.Errorf("catalog: %w", err)
			}
			st.CatalogTables += len(batch)
		}
		for start := 0; start < len(c.Edges); start += 5000 {
			batch := c.Edges[start:min(start+5000, len(c.Edges))]
			if err := cs.putEdges(batch); err != nil {
				return st, fmt.Errorf("catalog lineage: %w", err)
			}
			st.CatalogEdges += len(batch)
		}
	}
	return st, nil
}
