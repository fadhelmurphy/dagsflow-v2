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
	// dropEdgesVia removes the edges made by via that are not in keep (keys)
	dropEdgesVia(via string, keep map[string]bool) error
	// dropEdgesInto removes the edges into to of that kind last seen before t
	dropEdgesInto(to, kind string, before time.Time) error
	// expireEdges removes the edges last seen before t; returns how many
	expireEdges(before time.Time) (int, error)
	// purgeDropped removes datasets dropped before t (and their edges); returns how many
	purgeDropped(before time.Time) (int, error)
	// dump returns everything (backup)
	dump() (map[string]*CatalogTable, []LineageEdge, error)
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
		b.Queue(`INSERT INTO catalog_edge (k, from_id, to_id, doc, via, kind, last_seen) VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (k) DO UPDATE SET doc = EXCLUDED.doc, via = EXCLUDED.via, kind = EXCLUDED.kind, last_seen = EXCLUDED.last_seen`,
			k, e.From, e.To, doc, e.Via, e.Kind, e.LastSeen)
	}
	return c.p.pool.SendBatch(ctx, b).Close()
}

func (c pgCatalog) dropEdgesVia(via string, keep map[string]bool) error {
	ctx, cancel := catalogCtx()
	defer cancel()
	ks := make([]string, 0, len(keep))
	for k := range keep {
		ks = append(ks, k)
	}
	_, err := c.p.pool.Exec(ctx, `DELETE FROM catalog_edge WHERE via = $1 AND NOT (k = ANY($2))`, via, ks)
	return err
}

func (c pgCatalog) dropEdgesInto(to, kind string, before time.Time) error {
	ctx, cancel := catalogCtx()
	defer cancel()
	_, err := c.p.pool.Exec(ctx, `DELETE FROM catalog_edge WHERE to_id = $1 AND kind = $2 AND last_seen < $3`, to, kind, before)
	return err
}

func (c pgCatalog) expireEdges(before time.Time) (int, error) {
	ctx, cancel := catalogCtx()
	defer cancel()
	tag, err := c.p.pool.Exec(ctx, `DELETE FROM catalog_edge WHERE last_seen < $1`, before)
	return int(tag.RowsAffected()), err
}

func (c pgCatalog) purgeDropped(before time.Time) (int, error) {
	ctx, cancel := catalogCtx()
	defer cancel()
	tx, err := c.p.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `DELETE FROM catalog_table WHERE doc ? 'dropped' AND (doc->>'dropped')::timestamptz < $1 RETURNING id`, before)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM catalog_edge WHERE from_id = ANY($1) OR to_id = ANY($1)`, ids); err != nil {
			return 0, err
		}
	}
	return len(ids), tx.Commit(ctx)
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

func (c pgCatalog) dump() (map[string]*CatalogTable, []LineageEdge, error) {
	tables, err := c.scanTables(`SELECT doc FROM catalog_table WHERE doc <> 'null'::jsonb`)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := catalogCtx()
	defer cancel()
	rows, err := c.p.pool.Query(ctx, `SELECT doc FROM catalog_edge ORDER BY k`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var edges []LineageEdge
	for rows.Next() {
		var b []byte
		var e LineageEdge
		if err := rows.Scan(&b); err != nil {
			return nil, nil, err
		}
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, nil, err
		}
		edges = append(edges, e)
	}
	return tables, edges, rows.Err()
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
	newIdx := map[string]map[int]bool{}
	for i, list := range byShard {
		var doc map[string]*LineageEdge
		err := c.s.updateTable(kvEdgeKey(i), &doc, func() error {
			if doc == nil {
				doc = map[string]*LineageEdge{}
			}
			for _, e := range list {
				ec := e
				doc[e.key()] = &ec
				if e.Kind == "dagsflow" && e.Via != "" {
					if newIdx[e.Via] == nil {
						newIdx[e.Via] = map[int]bool{}
					}
					newIdx[e.Via][i] = true
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if len(newIdx) == 0 {
		return nil
	}
	var idx map[string][]int
	return c.s.updateTable("catalog_eidx", &idx, func() error {
		if idx == nil {
			idx = map[string][]int{}
		}
		for via, shards := range newIdx {
			have := map[int]bool{}
			for _, i := range idx[via] {
				have[i] = true
			}
			for i := range shards {
				if !have[i] {
					idx[via] = append(idx[via], i)
				}
			}
			sort.Ints(idx[via])
		}
		return nil
	})
}

// editEdges rewrites every edge shard with keep deciding which edges stay.
func (c kvCatalog) editEdges(keep func(e *LineageEdge) bool) (int, error) {
	n := 0
	for i := 0; i < kvEdgeShards; i++ {
		var doc map[string]*LineageEdge
		err := c.s.updateTable(kvEdgeKey(i), &doc, func() error {
			for k, e := range doc {
				if !keep(e) {
					delete(doc, k)
					n++
				}
			}
			return nil
		})
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (c kvCatalog) dropEdgesVia(via string, keep map[string]bool) error {
	var idx map[string][]int // dagsflow task -> the shards holding its edges
	if err := c.s.readTable("catalog_eidx", &idx); err != nil {
		return err
	}
	for _, i := range idx[via] {
		var doc map[string]*LineageEdge
		err := c.s.updateTable(kvEdgeKey(i), &doc, func() error {
			for k, e := range doc {
				if e.Via == via && !keep[k] {
					delete(doc, k)
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

func (c kvCatalog) dropEdgesInto(to, kind string, before time.Time) error {
	var doc map[string]*LineageEdge
	return c.s.updateTable(kvEdgeKey(shardOf(to, kvEdgeShards)), &doc, func() error {
		for k, e := range doc {
			if e.To == to && e.Kind == kind && e.LastSeen.Before(before) {
				delete(doc, k)
			}
		}
		return nil
	})
}

func (c kvCatalog) expireEdges(before time.Time) (int, error) {
	return c.editEdges(func(e *LineageEdge) bool { return !e.LastSeen.Before(before) })
}

func (c kvCatalog) purgeDropped(before time.Time) (int, error) {
	gone := map[string]bool{}
	for i := 0; i < kvTableShards; i++ {
		var doc map[string]*CatalogTable
		err := c.s.updateTable(kvTableKey(i), &doc, func() error {
			for id, t := range doc {
				if t.Dropped != nil && t.Dropped.Before(before) {
					gone[id] = true
					delete(doc, id)
				}
			}
			return nil
		})
		if err != nil {
			return len(gone), err
		}
	}
	if len(gone) > 0 {
		if _, err := c.editEdges(func(e *LineageEdge) bool { return !gone[e.From] && !gone[e.To] }); err != nil {
			return len(gone), err
		}
	}
	return len(gone), nil
}

// allEdges reads the edge shards only (much smaller than the tables).
func (c kvCatalog) allEdges() ([]LineageEdge, error) {
	cc := c.s.catCache()
	cc.mu.Lock()
	if cc.tables != nil && time.Since(cc.at) < 5*time.Second {
		defer cc.mu.Unlock()
		return cc.edges, nil
	}
	cc.mu.Unlock()
	var edges []LineageEdge
	for i := 0; i < kvEdgeShards; i++ {
		var doc map[string]*LineageEdge
		if err := c.s.readTable(kvEdgeKey(i), &doc); err != nil {
			return nil, err
		}
		for _, e := range doc {
			edges = append(edges, *e)
		}
	}
	return edges, nil
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

func (c kvCatalog) dump() (map[string]*CatalogTable, []LineageEdge, error) {
	c.s.dropCatalogCache()
	return c.all()
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
	all, err := c.allEdges()
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
