package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgBackend stores the metadata in Postgres. Several schedulers, webservers
// and workers on different machines can share it:
//
//   - a running DagRun is owned by one process (owner + lease_until, renewed
//     every few seconds); runs whose owner stopped renewing are re-queued by
//     the leader scheduler and resumed by any scheduler,
//   - writes by non-owners use optimistic concurrency (version column),
//   - pools are enforced globally (pool_slot rows of live processes),
//   - task logs and the task queue (remote executors) live in the database,
//     so no shared filesystem is needed.
type pgBackend struct {
	pool *pgxpool.Pool
}

const opTimeout = 30 * time.Second

func pgctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), opTimeout)
}

func openPG(dsn string) (*pgBackend, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("DAGSFLOW_DATABASE_URL: %w", err)
	}
	if cfg.MaxConns < 8 {
		cfg.MaxConns = 8
	}
	ctx, cancel := pgctx()
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to metadata database: %w", err)
	}
	b := &pgBackend{pool: pool}
	if err := b.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate metadata database: %w", err)
	}
	return b, nil
}

func (p *pgBackend) kind() string { return "postgres" }
func (p *pgBackend) close()       { p.pool.Close() }

// Ping checks the connection (readiness probes).
func (p *pgBackend) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return p.pool.Ping(ctx)
}

// ---- schema -----------------------------------------------------------------------

// migrations are applied in order, each once, under an advisory lock (so
// several processes starting together don't race). Never edit a released
// migration: append a new one.
var migrations = []string{
	// 1: initial schema
	`CREATE TABLE dag_run (
		dag_id text NOT NULL, run_id text NOT NULL,
		state text NOT NULL, run_type text NOT NULL,
		logical_date timestamptz NOT NULL, queued_at timestamptz NOT NULL, end_date timestamptz,
		owner text, lease_until timestamptz,
		version bigint NOT NULL DEFAULT 1,
		doc jsonb NOT NULL,
		updated_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (dag_id, run_id));
	CREATE INDEX dag_run_state_idx ON dag_run (state, dag_id);
	CREATE INDEX dag_run_end_idx ON dag_run (end_date);
	CREATE TABLE dag_meta (dag_id text PRIMARY KEY, doc jsonb NOT NULL);
	CREATE TABLE kv_table (name text PRIMARY KEY, doc jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
	CREATE TABLE run_command (id bigserial PRIMARY KEY, dag_id text NOT NULL, run_id text NOT NULL, doc jsonb NOT NULL);
	CREATE INDEX run_command_run_idx ON run_command (dag_id, run_id);
	CREATE TABLE audit_log (id bigserial PRIMARY KEY, at timestamptz NOT NULL, dag_id text, run_id text, task_id text, doc jsonb NOT NULL);
	CREATE INDEX audit_log_dag_idx ON audit_log (dag_id, id);
	CREATE INDEX audit_log_at_idx ON audit_log (at);
	CREATE TABLE task_log (id bigserial PRIMARY KEY, dag_id text NOT NULL, run_id text NOT NULL, task_id text NOT NULL,
		map_index int NOT NULL, try int NOT NULL, data bytea NOT NULL);
	CREATE INDEX task_log_key_idx ON task_log (dag_id, run_id, task_id, map_index, try, id);
	CREATE TABLE component (id text PRIMARY KEY, kind text NOT NULL, host text, pid int,
		started_at timestamptz NOT NULL, heartbeat_at timestamptz NOT NULL, info jsonb);
	CREATE TABLE lease (name text PRIMARY KEY, owner text NOT NULL, until timestamptz NOT NULL);
	CREATE TABLE pool_slot (holder text PRIMARY KEY, pool text NOT NULL, slots int NOT NULL, owner text NOT NULL,
		acquired_at timestamptz NOT NULL DEFAULT now());
	CREATE INDEX pool_slot_pool_idx ON pool_slot (pool);
	CREATE TABLE task_job (id bigserial PRIMARY KEY, queue text NOT NULL, state text NOT NULL, worker text,
		cancel boolean NOT NULL DEFAULT false,
		dag_id text NOT NULL, run_id text NOT NULL, task_id text NOT NULL, map_index int NOT NULL, try int NOT NULL,
		payload jsonb NOT NULL, result jsonb,
		created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, ended_at timestamptz, heartbeat_at timestamptz);
	CREATE INDEX task_job_queue_idx ON task_job (state, queue, id);
	CREATE INDEX task_job_run_idx ON task_job (dag_id, run_id);`,
	// 2: the DAG list reads the latest runs of each DAG
	`CREATE INDEX IF NOT EXISTS dag_run_latest_idx ON dag_run (dag_id, logical_date DESC, queued_at DESC);`,
	// 3: large XCom values live outside the run row (lists of runs stay small)
	`CREATE TABLE IF NOT EXISTS xcom_blob (dag_id text NOT NULL, run_id text NOT NULL, slot text NOT NULL, key text NOT NULL,
		data jsonb NOT NULL, PRIMARY KEY (dag_id, run_id, slot, key));`,
}

// SchemaVersion is the schema this binary expects.
func SchemaVersion() int { return len(migrations) }

func (p *pgBackend) migrate(ctx context.Context) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(727274)`); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(727274)`)
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS df_migration (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	var cur int
	if err := conn.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM df_migration`).Scan(&cur); err != nil {
		return err
	}
	if cur > len(migrations) {
		return fmt.Errorf("database schema version %d is newer than this dagsflow-go (%d): upgrade the binary", cur, len(migrations))
	}
	for v := cur + 1; v <= len(migrations); v++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, migrations[v-1]); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %d: %w", v, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO df_migration (version) VALUES ($1)`, v); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *pgBackend) schemaVersion() (int, error) {
	ctx, cancel := pgctx()
	defer cancel()
	var v int
	err := p.pool.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM df_migration`).Scan(&v)
	return v, err
}

// ---- dag runs ---------------------------------------------------------------------------

func leaseInterval() string { return fmt.Sprintf("%d seconds", int(runLease.Seconds())) }

func (p *pgBackend) saveRun(r *DagRun, me string) error {
	ctx, cancel := pgctx()
	defer cancel()
	doc, err := p.encodeRun(ctx, r)
	if err != nil {
		return err
	}
	var ver int64
	switch {
	case r.owned:
		var owner any
		if r.State == RunRunning {
			owner = me
		}
		err = p.pool.QueryRow(ctx, `UPDATE dag_run SET state=$3, run_type=$4, logical_date=$5, queued_at=$6, end_date=$7, doc=$8,
			owner=$9::text, lease_until=CASE WHEN $9::text IS NULL THEN NULL ELSE now() + $10::interval END,
			version=version+1, updated_at=now()
			WHERE dag_id=$1 AND run_id=$2 AND owner=$11 RETURNING version`,
			r.DagID, r.RunID, r.State, r.RunType, r.LogicalDate, r.QueuedAt, r.EndDate, doc, owner, leaseInterval(), me).Scan(&ver)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: lost ownership of %s/%s", ErrConflict, r.DagID, r.RunID)
		}
		if err == nil && owner == nil {
			r.owned = false
		}
	case r.version == 0:
		err = p.pool.QueryRow(ctx, `INSERT INTO dag_run (dag_id, run_id, state, run_type, logical_date, queued_at, end_date, doc)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (dag_id, run_id) DO NOTHING RETURNING version`,
			r.DagID, r.RunID, r.State, r.RunType, r.LogicalDate, r.QueuedAt, r.EndDate, doc).Scan(&ver)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: run %s/%s already exists", ErrConflict, r.DagID, r.RunID)
		}
	default:
		err = p.pool.QueryRow(ctx, `UPDATE dag_run SET state=$3, run_type=$4, logical_date=$5, queued_at=$6, end_date=$7, doc=$8,
			owner=NULL, lease_until=NULL, version=version+1, updated_at=now()
			WHERE dag_id=$1 AND run_id=$2 AND version=$9 AND (owner IS NULL OR lease_until < now()) RETURNING version`,
			r.DagID, r.RunID, r.State, r.RunType, r.LogicalDate, r.QueuedAt, r.EndDate, doc, r.version).Scan(&ver)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
	}
	if err != nil {
		return err
	}
	r.version = ver
	return nil
}

func decodeRun(doc []byte, ver int64) (*DagRun, error) {
	var r DagRun
	if err := json.Unmarshal(doc, &r); err != nil {
		return nil, err
	}
	r.version = ver
	return &r, nil
}

func (p *pgBackend) loadRun(dagID, runID string) (*DagRun, error) {
	ctx, cancel := pgctx()
	defer cancel()
	var doc []byte
	var ver int64
	err := p.pool.QueryRow(ctx, `SELECT doc, version FROM dag_run WHERE dag_id=$1 AND run_id=$2`, dagID, runID).Scan(&doc, &ver)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("dag run %s/%s not found", dagID, runID)
	}
	if err != nil {
		return nil, err
	}
	r, err := decodeRun(doc, ver)
	if err != nil {
		return nil, err
	}
	return r, p.resolveXCom(ctx, r)
}

func (p *pgBackend) runExists(dagID, runID string) bool {
	ctx, cancel := pgctx()
	defer cancel()
	var one int
	return p.pool.QueryRow(ctx, `SELECT 1 FROM dag_run WHERE dag_id=$1 AND run_id=$2`, dagID, runID).Scan(&one) == nil
}

func (p *pgBackend) deleteRun(dagID, runID string) error {
	ctx, cancel := pgctx()
	defer cancel()
	tag, err := p.pool.Exec(ctx, `DELETE FROM dag_run WHERE dag_id=$1 AND run_id=$2 AND (owner IS NULL OR lease_until < now())`, dagID, runID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if p.runExists(dagID, runID) {
			return ErrConflict
		}
		return fmt.Errorf("dag run %s/%s not found", dagID, runID)
	}
	_, _ = p.pool.Exec(ctx, `DELETE FROM run_command WHERE dag_id=$1 AND run_id=$2`, dagID, runID)
	_, _ = p.pool.Exec(ctx, `DELETE FROM task_job WHERE dag_id=$1 AND run_id=$2 AND state NOT IN ('queued','running')`, dagID, runID)
	_, _ = p.pool.Exec(ctx, `DELETE FROM xcom_blob WHERE dag_id=$1 AND run_id=$2`, dagID, runID)
	return p.deleteLogs(dagID, runID)
}

func (p *pgBackend) listRuns(dagID string, states []string) ([]*DagRun, error) {
	ctx, cancel := pgctx()
	defer cancel()
	if states == nil {
		states = []string{}
	}
	rows, err := p.pool.Query(ctx, `SELECT doc, version FROM dag_run WHERE dag_id=$1 AND (cardinality($2::text[]) = 0 OR state = ANY($2))`, dagID, states)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DagRun
	for rows.Next() {
		var doc []byte
		var ver int64
		if err := rows.Scan(&doc, &ver); err != nil {
			return nil, err
		}
		if r, err := decodeRun(doc, ver); err == nil {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

func (p *pgBackend) runCounts(dagID string) (map[string]int, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT state, count(*) FROM dag_run WHERE dag_id=$1 GROUP BY state`, dagID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

func (p *pgBackend) latestRuns(dagID string, n int) ([]*DagRun, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT doc - 'xcom' - 'rendered_fields', version FROM dag_run WHERE dag_id=$1 ORDER BY logical_date DESC, queued_at DESC LIMIT $2`, dagID, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DagRun
	for rows.Next() {
		var doc []byte
		var ver int64
		if err := rows.Scan(&doc, &ver); err != nil {
			return nil, err
		}
		if r, err := decodeRun(doc, ver); err == nil {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

// listRunsLite: the runs without their XComs and rendered fields (what the
// grid and run lists show), so old runs with big inline values stay cheap.
func (p *pgBackend) listRunsLite(dagID string) ([]*DagRun, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT doc - 'xcom' - 'rendered_fields', version FROM dag_run WHERE dag_id=$1`, dagID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DagRun
	for rows.Next() {
		var doc []byte
		var ver int64
		if err := rows.Scan(&doc, &ver); err != nil {
			return nil, err
		}
		if r, err := decodeRun(doc, ver); err == nil {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

func (p *pgBackend) claimRun(dagID, runID, me string, maxActive int) (*DagRun, error) {
	ctx, cancel := pgctx()
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('dag_run:' || $1))`, dagID); err != nil {
		return nil, err
	}
	if maxActive > 0 {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM dag_run WHERE dag_id=$1 AND state='running' AND owner IS NOT NULL AND lease_until > now()`, dagID).Scan(&n); err != nil {
			return nil, err
		}
		if n >= maxActive {
			return nil, nil
		}
	}
	var doc []byte
	var ver int64
	err = tx.QueryRow(ctx, `UPDATE dag_run SET state='running', owner=$3, lease_until=now() + $4::interval,
		doc=jsonb_set(doc, '{state}', '"running"'), version=version+1, updated_at=now()
		WHERE dag_id=$1 AND run_id=$2 AND state='queued' AND (owner IS NULL OR lease_until < now())
		RETURNING doc, version`, dagID, runID, me, leaseInterval()).Scan(&doc, &ver)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	r, err := decodeRun(doc, ver)
	if err != nil {
		return nil, err
	}
	r.owned = true
	return r, p.resolveXCom(ctx, r)
}

func (p *pgBackend) takeOwnership(r *DagRun, me string) error {
	prev := r.State
	r.State = RunRunning
	ctx, cancel := pgctx()
	defer cancel()
	doc, err := p.encodeRun(ctx, r)
	if err != nil {
		return err
	}
	var ver int64
	if r.version == 0 {
		err = p.pool.QueryRow(ctx, `INSERT INTO dag_run (dag_id, run_id, state, run_type, logical_date, queued_at, end_date, doc, owner, lease_until)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now() + $10::interval) ON CONFLICT (dag_id, run_id) DO NOTHING RETURNING version`,
			r.DagID, r.RunID, r.State, r.RunType, r.LogicalDate, r.QueuedAt, r.EndDate, doc, me, leaseInterval()).Scan(&ver)
	} else {
		err = p.pool.QueryRow(ctx, `UPDATE dag_run SET state=$3, run_type=$4, logical_date=$5, queued_at=$6, end_date=$7, doc=$8,
			owner=$9, lease_until=now() + $10::interval, version=version+1, updated_at=now()
			WHERE dag_id=$1 AND run_id=$2 AND version=$11 AND (owner IS NULL OR owner=$9 OR lease_until < now()) RETURNING version`,
			r.DagID, r.RunID, r.State, r.RunType, r.LogicalDate, r.QueuedAt, r.EndDate, doc, me, leaseInterval(), r.version).Scan(&ver)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		r.State = prev
		return fmt.Errorf("%w: %s/%s is owned by another process or changed", ErrConflict, r.DagID, r.RunID)
	}
	if err != nil {
		r.State = prev
		return err
	}
	r.version, r.owned = ver, true
	return nil
}

func (p *pgBackend) requeueOrphans() ([]string, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `UPDATE dag_run SET state='queued', owner=NULL, lease_until=NULL,
		doc=jsonb_set(doc, '{state}', '"queued"'), version=version+1, updated_at=now()
		WHERE state='running' AND (owner IS NULL OR lease_until < now()) RETURNING dag_id, run_id`)
	if err != nil {
		return nil, err
	}
	var out [][2]string
	for rows.Next() {
		var d, r string
		if err := rows.Scan(&d, &r); err == nil {
			out = append(out, [2]string{d, r})
		}
	}
	rows.Close()
	// jobs of the dead owner keep running on their workers: the adopting
	// scheduler re-attaches to them (see Engine.Execute)
	var ids []string
	for _, x := range out {
		ids = append(ids, x[0]+"/"+x[1])
	}
	return ids, nil
}

func (p *pgBackend) renewLeases(me string, keys []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(keys) == 0 {
		return out, nil
	}
	ctx, cancel := pgctx()
	defer cancel()
	// only runs this process is still executing: a run left behind by an
	// early return expires and is adopted
	rows, err := p.pool.Query(ctx, `UPDATE dag_run SET lease_until = now() + $2::interval
		WHERE owner=$1 AND state='running' AND (dag_id || chr(31) || run_id) = ANY($3)
		RETURNING dag_id || chr(31) || run_id`, me, leaseInterval(), keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out[k] = true
	}
	return out, rows.Err()
}

func (p *pgBackend) deleteDagData(dagID string) error {
	ctx, cancel := pgctx()
	defer cancel()
	for _, q := range []string{
		`DELETE FROM dag_run WHERE dag_id=$1`, `DELETE FROM dag_meta WHERE dag_id=$1`,
		`DELETE FROM task_log WHERE dag_id=$1`, `DELETE FROM run_command WHERE dag_id=$1`,
		`DELETE FROM task_job WHERE dag_id=$1 AND state NOT IN ('queued','running')`,
		`DELETE FROM xcom_blob WHERE dag_id=$1`,
	} {
		if _, err := p.pool.Exec(ctx, q, dagID); err != nil {
			return err
		}
	}
	return nil
}

func (p *pgBackend) dagIDs() ([]string, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT DISTINCT dag_id FROM dag_run UNION SELECT dag_id FROM dag_meta ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (p *pgBackend) getDagMeta(dagID string) *DagMeta {
	m := &DagMeta{DagID: dagID}
	ctx, cancel := pgctx()
	defer cancel()
	var doc []byte
	if p.pool.QueryRow(ctx, `SELECT doc FROM dag_meta WHERE dag_id=$1`, dagID).Scan(&doc) == nil {
		_ = json.Unmarshal(doc, m)
	}
	return m
}

func (p *pgBackend) saveDagMeta(m *DagMeta) error {
	doc, err := json.Marshal(m)
	if err != nil {
		return err
	}
	ctx, cancel := pgctx()
	defer cancel()
	_, err = p.pool.Exec(ctx, `INSERT INTO dag_meta (dag_id, doc) VALUES ($1, $2) ON CONFLICT (dag_id) DO UPDATE SET doc=EXCLUDED.doc`, m.DagID, doc)
	return err
}

// ---- small tables (variables, connections, pools, users, datasets, ...) -----------------

func unmarshalDoc(doc []byte, v any) error {
	if len(doc) == 0 || string(doc) == "null" {
		return nil
	}
	return json.Unmarshal(doc, v)
}

func (p *pgBackend) readTable(name string, v any) error {
	ctx, cancel := pgctx()
	defer cancel()
	var doc []byte
	err := p.pool.QueryRow(ctx, `SELECT doc FROM kv_table WHERE name=$1`, name).Scan(&doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return unmarshalDoc(doc, v)
}

// updateTable is a real transaction (row lock), unlike the JSON files.
func (p *pgBackend) updateTable(name string, v any, fn func() error) error {
	ctx, cancel := pgctx()
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO kv_table (name, doc) VALUES ($1, 'null') ON CONFLICT (name) DO NOTHING`, name); err != nil {
		return err
	}
	var doc []byte
	if err := tx.QueryRow(ctx, `SELECT doc FROM kv_table WHERE name=$1 FOR UPDATE`, name).Scan(&doc); err != nil {
		return err
	}
	if err := unmarshalDoc(doc, v); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	out, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE kv_table SET doc=$2, updated_at=now() WHERE name=$1`, name, out); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *pgBackend) tableStamp(name string) time.Time {
	ctx, cancel := pgctx()
	defer cancel()
	var t time.Time
	_ = p.pool.QueryRow(ctx, `SELECT updated_at FROM kv_table WHERE name=$1`, name).Scan(&t)
	return t
}

// ---- command queue ----------------------------------------------------------------

func (p *pgBackend) enqueueCommand(dagID, runID string, m MarkRequest) error {
	doc, err := json.Marshal(m)
	if err != nil {
		return err
	}
	ctx, cancel := pgctx()
	defer cancel()
	_, err = p.pool.Exec(ctx, `INSERT INTO run_command (dag_id, run_id, doc) VALUES ($1,$2,$3)`, dagID, runID, doc)
	return err
}

func (p *pgBackend) takeCommands(dagID, runID string) []MarkRequest {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `DELETE FROM run_command WHERE dag_id=$1 AND run_id=$2 RETURNING id, doc`, dagID, runID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	type item struct {
		id int64
		m  MarkRequest
	}
	var items []item
	for rows.Next() {
		var it item
		var doc []byte
		if rows.Scan(&it.id, &doc) == nil && json.Unmarshal(doc, &it.m) == nil {
			items = append(items, it)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].id < items[j].id })
	out := make([]MarkRequest, len(items))
	for i, it := range items {
		out[i] = it.m
	}
	return out
}

func (p *pgBackend) dropCommands(dagID, runID string) {
	ctx, cancel := pgctx()
	defer cancel()
	_, _ = p.pool.Exec(ctx, `DELETE FROM run_command WHERE dag_id=$1 AND run_id=$2`, dagID, runID)
}

// ---- audit --------------------------------------------------------------------------

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (p *pgBackend) audit(ev AuditEvent) {
	doc, err := json.Marshal(ev)
	if err != nil {
		return
	}
	ctx, cancel := pgctx()
	defer cancel()
	_, _ = p.pool.Exec(ctx, `INSERT INTO audit_log (at, dag_id, run_id, task_id, doc) VALUES ($1,$2,$3,$4,$5)`,
		ev.Time, nullable(ev.DagID), nullable(ev.RunID), nullable(ev.TaskID), doc)
}

func (p *pgBackend) auditEvents(dagID, runID, taskID string, limit int) ([]AuditEvent, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT doc FROM audit_log
		WHERE ($1 = '' OR dag_id = $1) AND ($2 = '' OR run_id = $2) AND ($3 = '' OR task_id = $3)
		ORDER BY id DESC LIMIT $4`, dagID, runID, taskID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AuditEvent{}
	for rows.Next() {
		var doc []byte
		var ev AuditEvent
		if rows.Scan(&doc) == nil && json.Unmarshal(doc, &ev) == nil {
			out = append(out, ev)
		}
	}
	return out, rows.Err()
}

// ---- task logs (in the database: no shared volume needed) ---------------------------

type pgLogWriter struct {
	p    *pgBackend
	k    LogKey
	mu   sync.Mutex
	buf  bytes.Buffer
	done chan struct{}
	wg   sync.WaitGroup
}

func (p *pgBackend) openLog(k LogKey) (io.WriteCloser, string, error) {
	w := &pgLogWriter{p: p, k: k, done: make(chan struct{})}
	w.wg.Add(1)
	go func() { // flush at least every second so the UI can follow a running task
		defer w.wg.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-w.done:
				return
			case <-t.C:
				w.flush()
			}
		}
	}()
	return w, fmt.Sprintf("db://task_log/%s/%s/%s/%d/%d", k.DagID, k.RunID, k.TaskID, k.MapIndex, k.Try), nil
}

func (w *pgLogWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	n, _ := w.buf.Write(b)
	big := w.buf.Len() >= 256<<10
	w.mu.Unlock()
	if big {
		w.flush()
	}
	return n, nil
}

func (w *pgLogWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() == 0 {
		return
	}
	data := append([]byte(nil), w.buf.Bytes()...)
	ctx, cancel := pgctx()
	defer cancel()
	if _, err := w.p.pool.Exec(ctx, `INSERT INTO task_log (dag_id, run_id, task_id, map_index, try, data) VALUES ($1,$2,$3,$4,$5,$6)`,
		w.k.DagID, w.k.RunID, w.k.TaskID, w.k.MapIndex, w.k.Try, data); err != nil {
		metricErrors.inc("task_log_write")
		return // keep the buffer: retried on the next flush
	}
	w.buf.Reset()
}

func (w *pgLogWriter) Close() error {
	select {
	case <-w.done:
		return nil
	default:
	}
	close(w.done)
	w.wg.Wait()
	w.flush()
	return nil
}

func (p *pgBackend) readLog(k LogKey) ([]byte, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT data FROM task_log WHERE dag_id=$1 AND run_id=$2 AND task_id=$3 AND map_index=$4 AND try=$5 ORDER BY id`,
		k.DagID, k.RunID, k.TaskID, k.MapIndex, k.Try)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out bytes.Buffer
	found := false
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		out.Write(b)
		found = true
	}
	if !found {
		return nil, fs.ErrNotExist
	}
	return out.Bytes(), rows.Err()
}

func (p *pgBackend) deleteLogs(dagID, runID string) error {
	ctx, cancel := pgctx()
	defer cancel()
	_, err := p.pool.Exec(ctx, `DELETE FROM task_log WHERE dag_id=$1 AND ($2 = '' OR run_id=$2)`, dagID, runID)
	return err
}

// ---- components, leases, pools -----------------------------------------------------------

func (p *pgBackend) heartbeat(c Component) error {
	info, _ := json.Marshal(c.Info)
	ctx, cancel := pgctx()
	defer cancel()
	_, err := p.pool.Exec(ctx, `INSERT INTO component (id, kind, host, pid, started_at, heartbeat_at, info) VALUES ($1,$2,$3,$4,$5, now(), $6)
		ON CONFLICT (id) DO UPDATE SET heartbeat_at=now(), info=EXCLUDED.info`, c.ID, c.Kind, c.Host, c.PID, c.Started, info)
	if err == nil {
		// forget processes dead for a day, and pool slots of dead processes
		_, _ = p.pool.Exec(ctx, `DELETE FROM component WHERE heartbeat_at < now() - interval '1 day'`)
		_, _ = p.pool.Exec(ctx, `DELETE FROM pool_slot s WHERE NOT EXISTS (SELECT 1 FROM component c WHERE c.id = s.owner AND c.heartbeat_at > now() - $1::interval)`,
			fmt.Sprintf("%d seconds", int(componentTimeout.Seconds())))
	}
	return err
}

func (p *pgBackend) components() ([]Component, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT id, kind, coalesce(host,''), coalesce(pid,0), started_at,
		extract(epoch from now() - heartbeat_at)::float8, info FROM component ORDER BY kind, started_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Component
	for rows.Next() {
		var c Component
		var age float64
		var info []byte
		if err := rows.Scan(&c.ID, &c.Kind, &c.Host, &c.PID, &c.Started, &age, &info); err != nil {
			return nil, err
		}
		// ages come from the database clock, so machines' clock skew doesn't matter
		c.Heartbeat = time.Now().Add(-time.Duration(age * float64(time.Second)))
		_ = unmarshalDoc(info, &c.Info)
		out = append(out, c)
	}
	return out, rows.Err()
}

func (p *pgBackend) removeComponent(id string) {
	ctx, cancel := pgctx()
	defer cancel()
	_, _ = p.pool.Exec(ctx, `DELETE FROM pool_slot WHERE owner=$1`, id)
	_, _ = p.pool.Exec(ctx, `DELETE FROM lease WHERE owner=$1`, id)
	_, _ = p.pool.Exec(ctx, `DELETE FROM component WHERE id=$1`, id)
}

func (p *pgBackend) tryLease(name, me string, ttl time.Duration) (bool, error) {
	ctx, cancel := pgctx()
	defer cancel()
	var owner string
	err := p.pool.QueryRow(ctx, `INSERT INTO lease (name, owner, until) VALUES ($1, $2, now() + $3::interval)
		ON CONFLICT (name) DO UPDATE SET owner=EXCLUDED.owner, until=EXCLUDED.until
		WHERE lease.owner = EXCLUDED.owner OR lease.until < now() RETURNING owner`,
		name, me, fmt.Sprintf("%d milliseconds", ttl.Milliseconds())).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (p *pgBackend) acquirePoolSlot(pool string, slots, limit int, holder, me string) (bool, error) {
	ctx, cancel := pgctx()
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('pool:' || $1))`, pool); err != nil {
		return false, err
	}
	var used int
	if err := tx.QueryRow(ctx, `SELECT coalesce(sum(s.slots), 0) FROM pool_slot s JOIN component c ON c.id = s.owner
		WHERE s.pool=$1 AND s.holder <> $2 AND c.heartbeat_at > now() - $3::interval`,
		pool, holder, fmt.Sprintf("%d seconds", int(componentTimeout.Seconds()))).Scan(&used); err != nil {
		return false, err
	}
	if used+slots > limit {
		return false, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO pool_slot (holder, pool, slots, owner) VALUES ($1,$2,$3,$4)
		ON CONFLICT (holder) DO UPDATE SET pool=EXCLUDED.pool, slots=EXCLUDED.slots, owner=EXCLUDED.owner, acquired_at=now()`,
		holder, pool, slots, me); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (p *pgBackend) releasePoolSlot(holder string) {
	ctx, cancel := pgctx()
	defer cancel()
	_, _ = p.pool.Exec(ctx, `DELETE FROM pool_slot WHERE holder=$1`, holder)
}

func (p *pgBackend) poolUsage() (map[string]int, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT s.pool, sum(s.slots)::int FROM pool_slot s JOIN component c ON c.id = s.owner
		WHERE c.heartbeat_at > now() - $1::interval GROUP BY s.pool`, fmt.Sprintf("%d seconds", int(componentTimeout.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var name string
		var n int
		if err := rows.Scan(&name, &n); err != nil {
			return nil, err
		}
		out[name] = n
	}
	return out, rows.Err()
}

func (p *pgBackend) secretKey() ([]byte, error) {
	ctx, cancel := pgctx()
	defer cancel()
	raw := make([]byte, 32)
	if _, err := randRead(raw); err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%x", raw)
	if _, err := p.pool.Exec(ctx, `INSERT INTO kv_table (name, doc) VALUES ('webserver_secret_key', to_jsonb($1::text)) ON CONFLICT (name) DO NOTHING`, key); err != nil {
		return nil, err
	}
	var k string
	if err := p.pool.QueryRow(ctx, `SELECT doc #>> '{}' FROM kv_table WHERE name='webserver_secret_key'`).Scan(&k); err != nil {
		return nil, err
	}
	return []byte(k), nil
}

// ---- task queue (worker / kubernetes executors) ---------------------------------------------

const jobCols = `id, queue, state, coalesce(worker,''), cancel, created_at, started_at, ended_at,
	extract(epoch from now() - heartbeat_at)::float8, payload, result`

func scanJob(row pgx.Row) (*TaskJob, error) {
	var j TaskJob
	var hbAge *float64
	var payload, result []byte
	if err := row.Scan(&j.ID, &j.Queue, &j.State, &j.Worker, &j.Cancel, &j.CreatedAt, &j.StartedAt, &j.EndedAt, &hbAge, &payload, &result); err != nil {
		return nil, err
	}
	if hbAge != nil {
		t := time.Now().Add(-time.Duration(*hbAge * float64(time.Second)))
		j.Heartbeat = &t
	}
	if err := json.Unmarshal(payload, &j.Payload); err != nil {
		return nil, err
	}
	if len(result) > 0 && string(result) != "null" {
		j.Result = &JobResult{}
		if err := json.Unmarshal(result, j.Result); err != nil {
			return nil, err
		}
	}
	return &j, nil
}

func (p *pgBackend) enqueueJob(j *TaskJob) (int64, error) {
	payload, err := json.Marshal(j.Payload)
	if err != nil {
		return 0, fmt.Errorf("task payload is not JSON serialisable: %w", err)
	}
	ctx, cancel := pgctx()
	defer cancel()
	var id int64
	err = p.pool.QueryRow(ctx, `INSERT INTO task_job (queue, state, dag_id, run_id, task_id, map_index, try, payload)
		VALUES ($1, 'queued', $2, $3, $4, $5, $6, $7) RETURNING id`,
		j.Queue, j.Payload.DagID, j.Payload.RunID, j.Payload.TaskID, j.Payload.MapIndex, j.Payload.Try, payload).Scan(&id)
	if err == nil {
		_, _ = p.pool.Exec(ctx, `SELECT pg_notify('dagsflow_jobs', $1)`, j.Queue)
	}
	return id, err
}

func (p *pgBackend) claimJob(queues []string, worker string, id int64) (*TaskJob, error) {
	ctx, cancel := pgctx()
	defer cancel()
	var row pgx.Row
	if id > 0 {
		row = p.pool.QueryRow(ctx, `UPDATE task_job SET state='running', worker=$2, started_at=now(), heartbeat_at=now()
			WHERE id=$1 AND state='queued' AND NOT cancel RETURNING `+jobCols, id, worker)
	} else {
		row = p.pool.QueryRow(ctx, `UPDATE task_job SET state='running', worker=$2, started_at=now(), heartbeat_at=now()
			WHERE id = (SELECT id FROM task_job WHERE state='queued' AND queue = ANY($1) AND NOT cancel
				ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
			RETURNING `+jobCols, queues, worker)
	}
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

func (p *pgBackend) getJob(id int64) (*TaskJob, error) {
	ctx, cancel := pgctx()
	defer cancel()
	return scanJob(p.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM task_job WHERE id=$1`, id))
}

func (p *pgBackend) jobHeartbeat(id int64, worker string) (bool, error) {
	ctx, cancel := pgctx()
	defer cancel()
	var c bool
	err := p.pool.QueryRow(ctx, `UPDATE task_job SET heartbeat_at=now() WHERE id=$1 AND worker=$2 AND state='running' RETURNING cancel`, id, worker).Scan(&c)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil // not ours any more: stop
	}
	return c, err
}

func (p *pgBackend) finishJob(id int64, worker string, res JobResult) error {
	b, err := json.Marshal(res)
	if err != nil {
		res = JobResult{Error: fmt.Sprintf("task result is not JSON serialisable: %v", err)}
		b, _ = json.Marshal(res)
	}
	state := JobSuccess
	if res.Error != "" {
		state = JobFailed
	}
	ctx, cancel := pgctx()
	defer cancel()
	_, err = p.pool.Exec(ctx, `UPDATE task_job SET state=$3, result=$4, ended_at=now(), heartbeat_at=now()
		WHERE id=$1 AND worker=$2 AND state='running'`, id, worker, state, b)
	return err
}

func (p *pgBackend) cancelJob(id int64) error {
	ctx, cancel := pgctx()
	defer cancel()
	_, err := p.pool.Exec(ctx, `UPDATE task_job SET cancel=true,
		state=CASE WHEN state='queued' THEN 'cancelled' ELSE state END,
		ended_at=CASE WHEN state='queued' THEN now() ELSE ended_at END WHERE id=$1`, id)
	return err
}

func (p *pgBackend) findActiveJob(dagID, runID, taskID string, mapIndex, try int) (*TaskJob, error) {
	ctx, cancel := pgctx()
	defer cancel()
	j, err := scanJob(p.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM task_job
		WHERE dag_id=$1 AND run_id=$2 AND task_id=$3 AND map_index=$4 AND try=$5 AND state IN ('queued','running') AND NOT cancel
		ORDER BY id DESC LIMIT 1`, dagID, runID, taskID, mapIndex, try))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

func (p *pgBackend) jobStats() (map[string]int, error) {
	ctx, cancel := pgctx()
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT queue, state, count(*)::int FROM task_job WHERE state IN ('queued','running') GROUP BY queue, state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var q, s string
		var n int
		if err := rows.Scan(&q, &s, &n); err != nil {
			return nil, err
		}
		out[q+"\x00"+s] = n
	}
	return out, rows.Err()
}

// ---- retention ----------------------------------------------------------------------------

func (p *pgBackend) cleanup(before time.Time) (CleanupStats, error) {
	var st CleanupStats
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	tag, err := p.pool.Exec(ctx, `DELETE FROM dag_run WHERE state IN ('success','failed') AND end_date < $1 AND owner IS NULL`, before)
	if err != nil {
		return st, err
	}
	st.Runs = int(tag.RowsAffected())
	if tag, err = p.pool.Exec(ctx, `DELETE FROM task_log t WHERE NOT EXISTS (SELECT 1 FROM dag_run r WHERE r.dag_id = t.dag_id AND r.run_id = t.run_id)`); err != nil {
		return st, err
	}
	st.Logs = int(tag.RowsAffected())
	if tag, err = p.pool.Exec(ctx, `DELETE FROM audit_log WHERE at < $1`, before); err != nil {
		return st, err
	}
	st.Audit = int(tag.RowsAffected())
	if tag, err = p.pool.Exec(ctx, `DELETE FROM task_job WHERE state IN ('success','failed','cancelled') AND coalesce(ended_at, created_at) < $1`, before); err != nil {
		return st, err
	}
	st.Jobs = int(tag.RowsAffected())
	_, _ = p.pool.Exec(ctx, `DELETE FROM run_command c WHERE NOT EXISTS (SELECT 1 FROM dag_run r WHERE r.dag_id = c.dag_id AND r.run_id = c.run_id)`)
	_, _ = p.pool.Exec(ctx, `DELETE FROM xcom_blob b WHERE NOT EXISTS (SELECT 1 FROM dag_run r WHERE r.dag_id = b.dag_id AND r.run_id = b.run_id)`)
	return st, nil
}
