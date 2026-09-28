package engine

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// jsonBackend keeps the metadata as JSON files under $DAGSFLOW_HOME/state.
// Good for a single machine (one scheduler); use Postgres for anything more.
type jsonBackend struct{ home string }

// A process-wide mutex serialises read-modify-write of the small tables
// within one process; cross-process writes are last-writer-wins.
var (
	tableMu sync.Mutex
	auditMu sync.Mutex
)

func (j *jsonBackend) kind() string { return "json" }
func (j *jsonBackend) close()       {}

func (j *jsonBackend) state(p ...string) string {
	return filepath.Join(append([]string{j.home, "state"}, p...)...)
}
func (j *jsonBackend) runsDir(dagID string) string { return j.state("runs", dagID) }
func (j *jsonBackend) runPath(dagID, runID string) string {
	return filepath.Join(j.runsDir(dagID), safeName(runID)+".json")
}
func (j *jsonBackend) metaPath(dagID string) string { return j.state("dags", dagID+".json") }
func (j *jsonBackend) logsDir() string              { return filepath.Join(j.home, "logs") }

func writeJSONAtomic(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp%d", path, time.Now().UnixNano())
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return renameReplace(tmp, path)
}

// renameReplace atomically replaces path. On Windows the replace fails while
// another process (e.g. the webserver) has the file open, so it is retried
// for up to ~2s instead of losing the write.
func renameReplace(tmp, path string) error {
	var err error
	for i := 0; i < 30; i++ {
		if err = os.Rename(tmp, path); err == nil {
			return nil
		}
		if runtime.GOOS != "windows" {
			break
		}
		time.Sleep(time.Duration(10+i*5) * time.Millisecond)
	}
	_ = os.Remove(tmp)
	return err
}

func readJSON(path string, v any) error {
	var b []byte
	var err error
	for i := 0; i < 10; i++ { // Windows: the file may be mid-replace (sharing violation)
		if b, err = os.ReadFile(path); err == nil || os.IsNotExist(err) || runtime.GOOS != "windows" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// ---- runs ---------------------------------------------------------------------

func (j *jsonBackend) saveRun(r *DagRun, _ string) error {
	return writeJSONAtomic(j.runPath(r.DagID, r.RunID), r)
}

func (j *jsonBackend) loadRun(dagID, runID string) (*DagRun, error) {
	var r DagRun
	if err := readJSON(j.runPath(dagID, runID), &r); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("dag run %s/%s not found", dagID, runID)
		}
		return nil, err
	}
	return &r, nil
}

func (j *jsonBackend) runExists(dagID, runID string) bool {
	_, err := os.Stat(j.runPath(dagID, runID))
	return err == nil
}

func (j *jsonBackend) deleteRun(dagID, runID string) error {
	if err := os.Remove(j.runPath(dagID, runID)); err != nil {
		return err
	}
	return j.deleteLogs(dagID, runID)
}

func (j *jsonBackend) listRuns(dagID string, states []string) ([]*DagRun, error) {
	entries, err := os.ReadDir(j.runsDir(dagID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []*DagRun
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		var r DagRun
		if err := readJSON(filepath.Join(j.runsDir(dagID), e.Name()), &r); err == nil && stateIn(r.State, states) {
			out = append(out, &r)
		}
	}
	return out, nil
}

func stateIn(s string, states []string) bool {
	if len(states) == 0 {
		return true
	}
	for _, x := range states {
		if x == s {
			return true
		}
	}
	return false
}

// claimRun: a single scheduler per DAGSFLOW_HOME, so claiming is just the
// queued → running transition.
func (j *jsonBackend) claimRun(dagID, runID, me string, _ int) (*DagRun, error) {
	r, err := j.loadRun(dagID, runID)
	if err != nil || r.State != RunQueued {
		return nil, err
	}
	r.State = RunRunning
	r.owned = true
	return r, j.saveRun(r, me)
}

func (j *jsonBackend) takeOwnership(r *DagRun, me string) error {
	r.State, r.owned = RunRunning, true
	return j.saveRun(r, me)
}

func (j *jsonBackend) requeueOrphans() ([]string, error)                     { return nil, nil }
func (j *jsonBackend) renewLeases(string, []string) (map[string]bool, error) { return nil, nil }

func (j *jsonBackend) deleteDagData(dagID string) error {
	for _, p := range []string{j.runsDir(dagID), j.metaPath(dagID), filepath.Join(j.logsDir(), "dag_id="+dagID)} {
		if err := os.RemoveAll(p); err != nil {
			return err
		}
	}
	return nil
}

func (j *jsonBackend) dagIDs() ([]string, error) {
	seen := map[string]bool{}
	for _, dir := range []string{j.state("runs"), j.state("dags")} {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			seen[strings.TrimSuffix(e.Name(), ".json")] = true
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func (j *jsonBackend) getDagMeta(dagID string) *DagMeta {
	m := &DagMeta{DagID: dagID}
	_ = readJSON(j.metaPath(dagID), m)
	return m
}

func (j *jsonBackend) saveDagMeta(m *DagMeta) error { return writeJSONAtomic(j.metaPath(m.DagID), m) }

// ---- tables ---------------------------------------------------------------------

func (j *jsonBackend) tablePath(name string) string { return j.state(name + ".json") }

func (j *jsonBackend) readTable(name string, v any) error {
	err := readJSON(j.tablePath(name), v)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (j *jsonBackend) updateTable(name string, v any, fn func() error) error {
	tableMu.Lock()
	defer tableMu.Unlock()
	if err := j.readTable(name, v); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return writeJSONAtomic(j.tablePath(name), v)
}

func (j *jsonBackend) tableStamp(name string) time.Time {
	if fi, err := os.Stat(j.tablePath(name)); err == nil {
		return fi.ModTime()
	}
	return time.Time{}
}

// ---- command queue ------------------------------------------------------------------

func (j *jsonBackend) commandDir(dagID, runID string) string {
	return j.state("commands", dagID, safeName(runID))
}

func (j *jsonBackend) enqueueCommand(dagID, runID string, m MarkRequest) error {
	dir := j.commandDir(dagID, runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return writeJSONAtomic(filepath.Join(dir, fmt.Sprintf("%020d.json", m.At.UnixNano())), m)
}

func (j *jsonBackend) takeCommands(dagID, runID string) []MarkRequest {
	dir := j.commandDir(dagID, runID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []MarkRequest
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		p := filepath.Join(dir, e.Name())
		b, err := os.ReadFile(p)
		_ = os.Remove(p)
		var m MarkRequest
		if err == nil && json.Unmarshal(b, &m) == nil {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].At.Before(out[k].At) })
	return out
}

func (j *jsonBackend) dropCommands(dagID, runID string) { _ = os.RemoveAll(j.commandDir(dagID, runID)) }

// ---- audit ------------------------------------------------------------------------

func (j *jsonBackend) auditPath() string { return j.state("audit.log") }

// audit appends one event (JSON line). O_APPEND keeps concurrent writers from
// different processes from interleaving small records.
func (j *jsonBackend) audit(ev AuditEvent) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	auditMu.Lock()
	defer auditMu.Unlock()
	_ = os.MkdirAll(filepath.Dir(j.auditPath()), 0o755)
	f, err := os.OpenFile(j.auditPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(b, '\n'))
}

func (j *jsonBackend) auditEvents(dagID, runID, taskID string, limit int) ([]AuditEvent, error) {
	f, err := os.Open(j.auditPath())
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

// ---- task logs ----------------------------------------------------------------------

func (j *jsonBackend) logPath(k LogKey) string {
	parts := []string{j.logsDir(), "dag_id=" + k.DagID, "run_id=" + safeName(k.RunID), "task_id=" + k.TaskID}
	if k.MapIndex >= 0 {
		parts = append(parts, fmt.Sprintf("map_index=%d", k.MapIndex))
	}
	parts = append(parts, fmt.Sprintf("attempt=%d.log", k.Try))
	return filepath.Join(parts...)
}

func (j *jsonBackend) openLog(k LogKey) (io.WriteCloser, string, error) {
	p := j.logPath(k)
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, p, err
	}
	return &lockedWriter{w: f, c: f}, p, nil
}

func (j *jsonBackend) readLog(k LogKey) ([]byte, error) { return os.ReadFile(j.logPath(k)) }

func (j *jsonBackend) deleteLogs(dagID, runID string) error {
	p := filepath.Join(j.logsDir(), "dag_id="+dagID)
	if runID != "" {
		p = filepath.Join(p, "run_id="+safeName(runID))
	}
	return os.RemoveAll(p)
}

// lockedWriter makes a log safe for concurrent writers (task + subprocess pipes).
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
	c  io.Closer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func (l *lockedWriter) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.c == nil {
		return nil
	}
	err := l.c.Close()
	l.c = nil
	return err
}

// ---- components (single scheduler pid file) ----------------------------------------

func (j *jsonBackend) pidPath() string { return j.state("scheduler.pid") }

func (j *jsonBackend) heartbeat(c Component) error {
	if c.Kind != "scheduler" {
		return nil
	}
	p := j.pidPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(fmt.Sprintf("%d %d", c.PID, time.Now().Unix())), 0o644)
}

func (j *jsonBackend) components() ([]Component, error) {
	b, err := os.ReadFile(j.pidPath())
	if err != nil {
		return nil, nil
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return nil, nil
	}
	pid, _ := strconv.Atoi(f[0])
	ts, _ := strconv.ParseInt(f[1], 10, 64)
	hb := time.Unix(ts, 0)
	// the pid file is refreshed every tick; 10s without refresh = dead
	if time.Since(hb) >= 10*time.Second {
		hb = time.Now().Add(-componentTimeout)
	}
	return []Component{{ID: "scheduler", Kind: "scheduler", PID: pid, Heartbeat: hb}}, nil
}

func (j *jsonBackend) removeComponent(id string) {
	if id == "scheduler" {
		_ = os.Remove(j.pidPath())
	}
}

func (j *jsonBackend) tryLease(string, string, time.Duration) (bool, error) { return true, nil }

// Pools are enforced in-process by the (single) scheduler.
func (j *jsonBackend) acquirePoolSlot(string, int, int, string, string) (bool, error) {
	return true, nil
}
func (j *jsonBackend) releasePoolSlot(string)             {}
func (j *jsonBackend) poolUsage() (map[string]int, error) { return nil, nil }

func (j *jsonBackend) secretKey() ([]byte, error) {
	p := j.state("webserver_secret_key")
	if b, err := os.ReadFile(p); err == nil && len(b) >= 32 {
		return b, nil
	}
	raw := make([]byte, 32)
	_, _ = rand.Read(raw)
	key := []byte(hex.EncodeToString(raw))
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	// O_EXCL: if another replica created it first, use theirs
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		_, err = f.Write(key)
		f.Close()
		if err != nil {
			return nil, err
		}
		return key, nil
	}
	if b, err := os.ReadFile(p); err == nil && len(b) >= 32 {
		return b, nil
	}
	return nil, errors.New("cannot create webserver secret key")
}

// ---- task queue: Postgres only ----------------------------------------------------------

func (j *jsonBackend) enqueueJob(*TaskJob) (int64, error) { return 0, ErrNeedsDatabase }
func (j *jsonBackend) claimJob([]string, string, int64) (*TaskJob, error) {
	return nil, ErrNeedsDatabase
}
func (j *jsonBackend) getJob(int64) (*TaskJob, error)           { return nil, ErrNeedsDatabase }
func (j *jsonBackend) jobHeartbeat(int64, string) (bool, error) { return false, ErrNeedsDatabase }
func (j *jsonBackend) finishJob(int64, string, JobResult) error { return ErrNeedsDatabase }
func (j *jsonBackend) cancelJob(int64) error                    { return ErrNeedsDatabase }
func (j *jsonBackend) findActiveJob(string, string, string, int, int) (*TaskJob, error) {
	return nil, nil
}
func (j *jsonBackend) jobStats() (map[string]int, error) { return nil, nil }

// ---- retention ------------------------------------------------------------------------

func (j *jsonBackend) cleanup(before time.Time) (CleanupStats, error) {
	var st CleanupStats
	ids, _ := j.dagIDs()
	for _, id := range ids {
		runs, _ := j.listRuns(id, []string{RunSuccess, RunFailed})
		for _, r := range runs {
			if r.EndDate != nil && r.EndDate.Before(before) {
				if err := j.deleteRun(id, r.RunID); err == nil {
					st.Runs++
				}
			}
		}
	}
	// audit log: keep events newer than the cutoff
	auditMu.Lock()
	defer auditMu.Unlock()
	b, err := os.ReadFile(j.auditPath())
	if err != nil {
		return st, nil
	}
	var keep bytes.Buffer
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		var ev AuditEvent
		if json.Unmarshal(sc.Bytes(), &ev) == nil && ev.Time.Before(before) {
			st.Audit++
			continue
		}
		keep.Write(sc.Bytes())
		keep.WriteByte('\n')
	}
	if st.Audit > 0 {
		tmp := j.auditPath() + ".tmp"
		if err := os.WriteFile(tmp, keep.Bytes(), 0o644); err != nil {
			return st, err
		}
		return st, renameReplace(tmp, j.auditPath())
	}
	return st, nil
}
