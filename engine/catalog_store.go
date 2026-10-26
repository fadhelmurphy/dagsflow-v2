package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Where the catalog lives. Postgres: one row per dataset and per edge
// (indexed by id, lower-case id, scope, from and to), so a task that reports
// lineage touches a few rows and a lineage walk reads the edges it follows.
// JSON store: documents split in shards (a single machine, smaller catalogs).
type catalogStore interface {
	// updateTables lets fn change the datasets ids (absent from m when new)
	// and writes them back, atomically per call on Postgres.
	updateTables(ids []string, fn func(m map[string]*CatalogTable) error) error
	// markDropped marks the datasets of scope that are not in keep.
	markDropped(scope string, keep map[string]bool, at time.Time) error
	putEdges(es []LineageEdge) error
	summaries(q string) ([]CatalogSummary, error)
	tables(ids []string) (map[string]*CatalogTable, error)
	tableFold(lowerID string) (*CatalogTable, error)
	// edges into the dataset (upstream) or out of it
	edges(ds string, upstream bool) ([]LineageEdge, error)
}

func (s *Store) catalog() catalogStore {
	if pg, ok := s.be().(*pgBackend); ok {
		return pgCatalog{pg}
	}
	return kvCatalog{s}
}

type colsEntry struct {
	names []string
	at    time.Time
}

type catalogCache struct {
	mu   sync.Mutex
	cols map[string]colsEntry
	// JSON store: every shard, read at most every few seconds
	tables map[string]*CatalogTable
	edges  []LineageEdge
	at     time.Time
}

var catCaches sync.Map // *Store -> *catalogCache

func (s *Store) catCache() *catalogCache {
	c, _ := catCaches.LoadOrStore(s, &catalogCache{})
	return c.(*catalogCache)
}

func (s *Store) dropCatalogCache() {
	c := s.catCache()
	c.mu.Lock()
	c.cols, c.tables, c.edges = nil, nil, nil
	c.mu.Unlock()
}

// ---- Postgres ----------------------------------------------------------------------------

type pgCatalog struct{ p *pgBackend }

func catalogCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Minute) // a first sync writes thousands of rows
}

func uniqSorted(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	n := 0
	for i, id := range out {
		if i == 0 || id != out[n-1] {
			out[n] = id
			n++
		}
	}
	return out[:n]
}

func (c pgCatalog) updateTables(ids []string, fn func(m map[string]*CatalogTable) error) error {
	ids = uniqSorted(ids)
	ctx, cancel := catalogCtx()
	defer cancel()
	tx, err := c.p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	// lock the rows, creating the missing ones (sorted: no deadlocks between writers)
	if _, err := tx.Exec(ctx, `INSERT INTO catalog_table (id, lid, doc, summary)
		SELECT x, lower(x), 'null', 'null' FROM unnest($1::text[]) AS x ORDER BY x ON CONFLICT (id) DO NOTHING`, ids); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT id, doc FROM catalog_table WHERE id = ANY($1) ORDER BY id FOR UPDATE`, ids)
	if err != nil {
		return err
	}
	m := map[string]*CatalogTable{}
	before := map[string]string{}
	for rows.Next() {
		var id string
		var doc []byte
		if err := rows.Scan(&id, &doc); err != nil {
			rows.Close()
			return err
		}
		before[id] = string(doc)
		var t *CatalogTable
		if err := json.Unmarshal(doc, &t); err != nil {
			rows.Close()
			return err
		}
		if t != nil {
			m[id] = t
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if err := fn(m); err != nil {
		return err
	}
	b := &pgx.Batch{}
	for _, id := range ids {
		t := m[id]
		if t == nil {
			b.Queue(`DELETE FROM catalog_table WHERE id = $1 AND doc = 'null'::jsonb`, id)
			continue
		}
		doc, _ := json.Marshal(t)
		if string(doc) == before[id] {
			continue
		}
		sum, _ := json.Marshal(summarize(t))
		b.Queue(`UPDATE catalog_table SET doc = $2, summary = $3, colnames = $4, scope = $5, updated_at = now() WHERE id = $1`,
			id, doc, sum, liveColumnNames(t), t.Scope)
	}
	if b.Len() > 0 {
		if err := tx.SendBatch(ctx, b).Close(); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (c pgCatalog) markDropped(scope string, keep map[string]bool, at time.Time) error {
	ctx, cancel := catalogCtx()
	defer cancel()
	rows, err := c.p.pool.Query(ctx, `SELECT id FROM catalog_table WHERE scope = $1 AND doc <> 'null'::jsonb AND NOT doc ? 'dropped'`, scope)
	if err != nil {
		return err
	}
	var gone []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if !keep[id] {
			gone = append(gone, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(gone) == 0 {
		return err
	}
	return c.updateTables(gone, func(m map[string]*CatalogTable) error {
		for _, t := range m {
			if t.Scope == scope && t.Dropped == nil {
				d := at
				t.Dropped = &d
			}
		}
		return nil
	})
}

func (c pgCatalog) putEdges(es []LineageEdge) error {
	sort.Slice(es, func(i, j int) bool { return es[i].key() < es[j].key() })
	ctx, cancel := catalogCtx()
	defer cancel()
	b := &pgx.Batch{}
	last := ""
	for _, e := range es {
		k := e.key()
		if k == last { // the same edge twice in one statement would fail
			continue
		}
		last = k
		doc, _ := json.Marshal(e)
		b.Queue(`INSERT INTO catalog_edge (k, from_id, to_id, doc) VALUES ($1, $2, $3, $4)
			ON CONFLICT (k) DO UPDATE SET doc = EXCLUDED.doc`, k, e.From, e.To, doc)
	}
	return c.p.pool.SendBatch(ctx, b).Close()
}

func (c pgCatalog) summaries(q string) ([]CatalogSummary, error) {
	ctx, cancel := catalogCtx()
	defer cancel()
	like := "%" + strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`).Replace(q) + "%"
	rows, err := c.p.pool.Query(ctx, `SELECT summary FROM catalog_table
		WHERE doc <> 'null'::jsonb AND ($1 = '' OR lid LIKE $2 OR colnames LIKE $2) ORDER BY id`, q, like)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []CatalogSummary{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		var it CatalogSummary
		if err := json.Unmarshal(b, &it); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func (c pgCatalog) scanTables(q string, args ...any) (map[string]*CatalogTable, error) {
	ctx, cancel := catalogCtx()
	defer cancel()
	rows, err := c.p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*CatalogTable{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		var t *CatalogTable
		if err := json.Unmarshal(b, &t); err != nil {
			return nil, err
		}
		if t != nil {
			out[t.ID] = t
		}
	}
	return out, rows.Err()
}

func (c pgCatalog) tables(ids []string) (map[string]*CatalogTable, error) {
	return c.scanTables(`SELECT doc FROM catalog_table WHERE id = ANY($1)`, ids)
}

func (c pgCatalog) tableFold(lowerID string) (*CatalogTable, error) {
	m, err := c.scanTables(`SELECT doc FROM catalog_table WHERE lid = $1 ORDER BY id LIMIT 1`, lowerID)
	for _, t := range m {
		return t, err
	}
	return nil, err
}

func (c pgCatalog) edges(ds string, upstream bool) ([]LineageEdge, error) {
	ctx, cancel := catalogCtx()
	defer cancel()
	q := `SELECT doc FROM catalog_edge WHERE from_id = $1`
	if upstream {
		q = `SELECT doc FROM catalog_edge WHERE to_id = $1`
	}
	rows, err := c.p.pool.Query(ctx, q, ds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LineageEdge
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		var e LineageEdge
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- JSON store (kv documents in shards) ------------------------------------------------

type kvCatalog struct{ s *Store }

const (
	kvTableShards = 128
	kvEdgeShards  = 32
)

func shardOf(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(key)))
	return int(h.Sum32() % uint32(n))
}

func kvTableKey(i int) string { return fmt.Sprintf("catalog_t%03d", i) }
func kvEdgeKey(i int) string  { return fmt.Sprintf("catalog_e%02d", i) }

func (c kvCatalog) updateTables(ids []string, fn func(m map[string]*CatalogTable) error) error {
	byShard := map[int][]string{}
	for _, id := range uniqSorted(ids) {
		byShard[shardOf(id, kvTableShards)] = append(byShard[shardOf(id, kvTableShards)], id)
	}
	// fn sees every id at once (it merges a batch); each shard is then written
	// back in its own update
	m := map[string]*CatalogTable{}
	for i, list := range byShard {
		var doc map[string]*CatalogTable
		if err := c.s.readTable(kvTableKey(i), &doc); err != nil {
			return err
		}
		for _, id := range list {
			if t := doc[id]; t != nil {
				m[id] = t
			}
		}
	}
	before := map[string]string{}
	for id, t := range m {
		b, _ := json.Marshal(t)
		before[id] = string(b)
	}
	if err := fn(m); err != nil {
		return err
	}
	for i, list := range byShard {
		changed := false
		for _, id := range list {
			if t := m[id]; t != nil {
				b, _ := json.Marshal(t)
				changed = changed || string(b) != before[id]
			}
		}
		if !changed { // e.g. tables only named by an edge, already known
			continue
		}
		var doc map[string]*CatalogTable
		err := c.s.updateTable(kvTableKey(i), &doc, func() error {
			if doc == nil {
				doc = map[string]*CatalogTable{}
			}
			for _, id := range list {
				if t := m[id]; t != nil {
					doc[id] = t
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c kvCatalog) markDropped(scope string, keep map[string]bool, at time.Time) error {
	for i := 0; i < kvTableShards; i++ {
		var doc map[string]*CatalogTable
		err := c.s.updateTable(kvTableKey(i), &doc, func() error {
			for id, t := range doc {
				if t.Scope == scope && !keep[id] && t.Dropped == nil {
					d := at
					t.Dropped = &d
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c kvCatalog) putEdges(es []LineageEdge) error {
	byShard := map[int][]LineageEdge{}
	for _, e := range es {
		byShard[shardOf(e.To, kvEdgeShards)] = append(byShard[shardOf(e.To, kvEdgeShards)], e)
	}
	for i, list := range byShard {
		var doc map[string]*LineageEdge
		err := c.s.updateTable(kvEdgeKey(i), &doc, func() error {
			if doc == nil {
				doc = map[string]*LineageEdge{}
			}
			for _, e := range list {
				ec := e
				doc[e.key()] = &ec
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// all reads every shard (cached a few seconds; dropped on every write).
func (c kvCatalog) all() (map[string]*CatalogTable, []LineageEdge, error) {
	cc := c.s.catCache()
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.tables != nil && time.Since(cc.at) < 5*time.Second {
		return cc.tables, cc.edges, nil
	}
	tables := map[string]*CatalogTable{}
	for i := 0; i < kvTableShards; i++ {
		var doc map[string]*CatalogTable
		if err := c.s.readTable(kvTableKey(i), &doc); err != nil {
			return nil, nil, err
		}
		for k, v := range doc {
			tables[k] = v
		}
	}
	var edges []LineageEdge
	for i := 0; i < kvEdgeShards; i++ {
		var doc map[string]*LineageEdge
		if err := c.s.readTable(kvEdgeKey(i), &doc); err != nil {
			return nil, nil, err
		}
		for _, e := range doc {
			edges = append(edges, *e)
		}
	}
	cc.tables, cc.edges, cc.at = tables, edges, time.Now()
	return tables, edges, nil
}

func (c kvCatalog) summaries(q string) ([]CatalogSummary, error) {
	tables, _, err := c.all()
	if err != nil {
		return nil, err
	}
	out := []CatalogSummary{}
	for _, t := range tables {
		if q == "" || strings.Contains(strings.ToLower(t.ID), q) || strings.Contains(liveColumnNames(t), q) {
			out = append(out, summarize(t))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (c kvCatalog) tables(ids []string) (map[string]*CatalogTable, error) {
	out := map[string]*CatalogTable{}
	for _, id := range ids {
		var doc map[string]*CatalogTable
		if err := c.s.readTable(kvTableKey(shardOf(id, kvTableShards)), &doc); err != nil {
			return nil, err
		}
		if t := doc[id]; t != nil {
			out[id] = t
		}
	}
	return out, nil
}

func (c kvCatalog) tableFold(lowerID string) (*CatalogTable, error) {
	var doc map[string]*CatalogTable
	if err := c.s.readTable(kvTableKey(shardOf(lowerID, kvTableShards)), &doc); err != nil {
		return nil, err
	}
	for id, t := range doc {
		if strings.ToLower(id) == lowerID {
			return t, nil
		}
	}
	return nil, nil
}

func (c kvCatalog) edges(ds string, upstream bool) ([]LineageEdge, error) {
	if upstream { // edges are sharded by where they lead
		var doc map[string]*LineageEdge
		if err := c.s.readTable(kvEdgeKey(shardOf(ds, kvEdgeShards)), &doc); err != nil {
			return nil, err
		}
		var out []LineageEdge
		for _, e := range doc {
			if e.To == ds {
				out = append(out, *e)
			}
		}
		return out, nil
	}
	_, all, err := c.all()
	if err != nil {
		return nil, err
	}
	var out []LineageEdge
	for _, e := range all {
		if e.From == ds {
			out = append(out, e)
		}
	}
	return out, nil
}
