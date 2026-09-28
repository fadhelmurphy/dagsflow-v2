package web

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"dagsflow-go/dag"
	"dagsflow-go/engine"
)

//go:embed static
var staticFS embed.FS

// Server ~ `airflow webserver` + stable REST API (/api/v1). It only reads/writes
// the metadata store; the scheduler is what actually executes runs.
//
// Auth: HTTP Basic (users) or "Authorization: Bearer <token>" (users token).
// With no users defined, auth is off (the CLI only allows that on localhost).
type Server struct {
	Store *engine.Store

	authMu    sync.Mutex
	authCache map[[32]byte]authEntry
	key       []byte // session signing key
}

type authEntry struct {
	user *engine.User
	exp  time.Time
}

type ctxKey struct{}

var anonymousAdmin = &engine.User{Username: "anonymous", Role: engine.RoleAdmin}

func userFrom(r *http.Request) *engine.User {
	u, _ := r.Context().Value(ctxKey{}).(*engine.User)
	return u
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	type route struct {
		pattern string
		role    engine.Role
		h       http.HandlerFunc
	}
	V, U, O, A := engine.RoleViewer, engine.RoleUser, engine.RoleOp, engine.RoleAdmin
	routes := []route{
		{"GET /api/v1/health", "", s.health},
		{"POST /api/v1/login", "", s.login},
		{"POST /api/v1/logout", "", s.logout},
		{"DELETE /api/v1/dags/{dag}", O, s.deleteDag},
		{"GET /api/v1/me", V, s.me},
		{"GET /api/v1/dags", V, s.listDags},
		{"GET /api/v1/dags/{dag}", V, s.getDag},
		{"PATCH /api/v1/dags/{dag}", U, s.patchDag},
		{"GET /api/v1/dags/{dag}/dagRuns", V, s.listRuns},
		{"POST /api/v1/dags/{dag}/dagRuns", U, s.trigger},
		{"GET /api/v1/dags/{dag}/dagRuns/{run}", V, s.getRun},
		{"DELETE /api/v1/dags/{dag}/dagRuns/{run}", O, s.deleteRun},
		{"POST /api/v1/dags/{dag}/dagRuns/{run}/clear", U, s.clear},
		{"GET /api/v1/dags/{dag}/dagRuns/{run}/taskInstances", V, s.listTIs},
		{"GET /api/v1/dags/{dag}/dagRuns/{run}/taskInstances/{task}/logs/{try}", V, s.log},
		{"GET /api/v1/importErrors", V, s.importErrors},
		{"GET /api/v1/datasets", V, s.datasets},
		{"GET /api/v1/pools", V, s.listPools},
		{"PUT /api/v1/pools/{name}", O, s.putPool},
		{"POST /api/v1/pools", O, s.putPool},
		{"PUT /api/v1/pools", O, s.putPool},
		{"DELETE /api/v1/pools/{name}", O, s.deletePool},
		{"GET /api/v1/variables", O, s.listVariables},
		{"GET /api/v1/variables/{key}", O, s.getVariable},
		{"PUT /api/v1/variables/{key}", O, s.putVariable},
		{"POST /api/v1/variables", O, s.putVariable},
		{"PUT /api/v1/variables", O, s.putVariable},
		{"DELETE /api/v1/variables/{key}", O, s.deleteVariable},
		{"GET /api/v1/connections", O, s.listConnections},
		{"PUT /api/v1/connections/{id}", O, s.putConnection},
		{"POST /api/v1/connections", O, s.putConnection},
		{"PUT /api/v1/connections", O, s.putConnection},
		{"DELETE /api/v1/connections/{id}", O, s.deleteConnection},
		{"GET /api/v1/users", A, s.listUsers},
		{"POST /api/v1/users", A, s.createUser},
		{"DELETE /api/v1/users/{user}", A, s.deleteUser},
		{"POST /api/v1/users/{user}/tokens", A, s.issueToken},
	}
	for _, rt := range routes {
		mux.Handle(rt.pattern, s.require(rt.role, rt.h))
	}
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		fail(w, 404, "no such endpoint: %s %s", r.Method, r.URL.Path)
	})

	static, _ := fs.Sub(staticFS, "static")
	files := http.FileServer(http.FS(static))
	// Static UI files hold no data, so they are public; the SPA sends the
	// browser to /login when the API answers 401.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			fail(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		switch {
		case r.URL.Path == "/login":
			r.URL.Path = "/login.html"
		case r.URL.Path != "/":
			if _, err := fs.Stat(static, r.URL.Path[1:]); err != nil {
				r.URL.Path = "/"
			}
		}
		w.Header().Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
	return secureHeaders(mux)
}

func secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "same-origin")
		next.ServeHTTP(w, r)
	})
}

// require authenticates the request and enforces a minimum role.
func (s *Server) require(min engine.Role, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if min == "" {
			h(w, r)
			return
		}
		u, bearer, ok := s.authenticate(r)
		if !ok {
			if r.Header.Get("X-Dagsflow") == "" { // API clients get a Basic challenge; the UI shows its login page
				w.Header().Set("WWW-Authenticate", `Basic realm="dagsflow"`)
			}
			fail(w, http.StatusUnauthorized, "authentication required")
			return
		}
		// CSRF guard for cookie/basic-auth browsers: custom header needs CORS preflight.
		if r.Method != http.MethodGet && !bearer && r.Header.Get("X-Dagsflow") != "1" {
			fail(w, http.StatusForbidden, "missing X-Dagsflow header")
			return
		}
		if u.Role.Level() < min.Level() {
			fail(w, http.StatusForbidden, "role %s cannot do this (needs %s)", u.Role, min)
			return
		}
		h(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, u)))
	})
}

func (s *Server) authenticate(r *http.Request) (*engine.User, bool, bool) {
	users, err := s.Store.Users()
	if err == nil && len(users) == 0 {
		return anonymousAdmin, false, true
	}
	if u, ok := s.sessionUser(r); ok {
		return u, false, true
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		u, ok := s.cached("t:"+h, func() (*engine.User, bool) { return s.Store.Authenticate("", "", strings.TrimPrefix(h, "Bearer ")) })
		return u, true, ok
	}
	name, pw, ok := r.BasicAuth()
	if !ok {
		return nil, false, false
	}
	u, ok := s.cached("b:"+name+"\x00"+pw, func() (*engine.User, bool) { return s.Store.Authenticate(name, pw, "") })
	return u, false, ok
}

// cached avoids re-running PBKDF2 on every request (the UI polls). Entries
// expire after a minute so role changes and deletions apply quickly.
func (s *Server) cached(k string, fn func() (*engine.User, bool)) (*engine.User, bool) {
	sum := sha256.Sum256([]byte(k))
	s.authMu.Lock()
	if s.authCache == nil {
		s.authCache = map[[32]byte]authEntry{}
	}
	if e, ok := s.authCache[sum]; ok && time.Now().Before(e.exp) {
		s.authMu.Unlock()
		return e.user, true
	}
	s.authMu.Unlock()
	u, ok := fn()
	if ok {
		s.authMu.Lock()
		s.authCache[sum] = authEntry{u, time.Now().Add(time.Minute)}
		s.authMu.Unlock()
	}
	return u, ok
}

// Serve runs the HTTP server until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sh)
	}()
	fmt.Printf("[webserver] listening on http://%s (API: /api/v1)\n", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// ---- helpers -----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, format string, a ...any) {
	writeJSON(w, code, map[string]any{"status": code, "error": fmt.Sprintf(format, a...)})
}

func (s *Server) dagOr404(w http.ResponseWriter, r *http.Request) (*dag.DAG, bool) {
	d, ok := dag.Get(r.PathValue("dag"))
	if !ok {
		fail(w, http.StatusNotFound, "dag %q not found", r.PathValue("dag"))
	}
	return d, ok
}

func (s *Server) runOr404(w http.ResponseWriter, r *http.Request) (*dag.DAG, *engine.DagRun, bool) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return nil, nil, false
	}
	run, err := s.Store.LoadRun(d.ID, r.PathValue("run"))
	if err != nil {
		fail(w, 404, "%v", err)
		return nil, nil, false
	}
	return d, run, true
}

func readBody(r *http.Request, v any) error {
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil || len(b) == 0 {
		return err
	}
	return json.Unmarshal(b, v)
}

// ---- DAGs & runs ---------------------------------------------------------------

type runSummary struct {
	RunID       string               `json:"dag_run_id"`
	RunType     string               `json:"run_type"`
	State       string               `json:"state"`
	LogicalDate time.Time            `json:"logical_date"`
	StartDate   *time.Time           `json:"start_date,omitempty"`
	EndDate     *time.Time           `json:"end_date,omitempty"`
	Tasks       map[string]dag.State `json:"task_states,omitempty"`
	SLAMisses   int                  `json:"sla_misses,omitempty"`
}

func summarize(r *engine.DagRun, withTasks bool) runSummary {
	s := runSummary{RunID: r.RunID, RunType: r.RunType, State: r.State, LogicalDate: r.LogicalDate,
		StartDate: r.StartDate, EndDate: r.EndDate, SLAMisses: len(r.SLAMisses)}
	if withTasks {
		s.Tasks = map[string]dag.State{}
		for id, ti := range r.Tasks {
			s.Tasks[id] = ti.State
		}
	}
	return s
}

func lastN(runs []*engine.DagRun, n int) []*engine.DagRun {
	if n > 0 && len(runs) > n {
		return runs[len(runs)-n:]
	}
	return runs
}

func scheduleLabel(d *dag.DAG) string {
	if d.IsDatasetScheduled() {
		return "datasets: " + strings.Join(d.Datasets, ", ")
	}
	if d.Schedule == "" {
		return "None"
	}
	return d.Schedule
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	pid, alive := engine.SchedulerInfo(s.Store)
	writeJSON(w, 200, map[string]any{"scheduler_alive": alive, "scheduler_pid": pid})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	writeJSON(w, 200, map[string]any{"username": u.Username, "role": u.Role, "auth_enabled": u != anonymousAdmin})
}

func (s *Server) listDags(w http.ResponseWriter, r *http.Request) {
	type item struct {
		DagID       string         `json:"dag_id"`
		Owner       string         `json:"owner"`
		Schedule    string         `json:"schedule"`
		Description string         `json:"description"`
		Tags        []string       `json:"tags"`
		Source      string         `json:"source"`
		IsPaused    bool           `json:"is_paused"`
		Next        *time.Time     `json:"next_data_interval_start,omitempty"`
		Recent      []runSummary   `json:"recent_runs"`
		Active      int            `json:"active_runs"`
		RunCounts   map[string]int `json:"run_counts"`
		LastRun     *runSummary    `json:"last_run,omitempty"`
		TaskCounts  map[string]int `json:"recent_task_counts"`
	}
	out := []item{}
	for _, d := range dag.List() {
		m := s.Store.GetDagMeta(d.ID)
		runs, _ := s.Store.ListRuns(d.ID)
		it := item{DagID: d.ID, Owner: d.Owner(), Schedule: scheduleLabel(d), Description: d.Description, Tags: d.Tags,
			Source: d.Source, IsPaused: m.IsPaused, Next: m.NextDataIntervalStart, Recent: []runSummary{},
			RunCounts: map[string]int{}, TaskCounts: map[string]int{}}
		if it.Tags == nil {
			it.Tags = []string{}
		}
		for _, run := range runs {
			it.RunCounts[run.State]++
			if run.State == engine.RunQueued || run.State == engine.RunRunning {
				it.Active++
			}
		}
		for _, run := range lastN(runs, 14) {
			it.Recent = append(it.Recent, summarize(run, false))
		}
		if len(runs) > 0 {
			last := runs[len(runs)-1]
			ls := summarize(last, false)
			it.LastRun = &ls
			for id, ti := range last.Tasks {
				if kids := last.Mapped[id]; len(kids) > 0 {
					for _, k := range kids {
						it.TaskCounts[stateName(k.State)]++
					}
					continue
				}
				it.TaskCounts[stateName(ti.State)]++
			}
		}
		out = append(out, it)
	}
	writeJSON(w, 200, map[string]any{"dags": out, "total_entries": len(out), "import_errors": importErrMap()})
}

func stateName(s dag.State) string {
	if s == "" {
		return "none"
	}
	return string(s)
}

func (s *Server) deleteDag(w http.ResponseWriter, r *http.Request) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return
	}
	runs, _ := s.Store.ListRuns(d.ID)
	for _, run := range runs {
		if run.State == engine.RunRunning {
			fail(w, 409, "dag has a running run; wait for it or pause the DAG first")
			return
		}
	}
	if err := s.Store.DeleteDagData(d.ID); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func importErrMap() map[string]string {
	errs := map[string]string{}
	for id, err := range dag.ImportErrors() {
		errs[id] = err.Error()
	}
	return errs
}

func (s *Server) importErrors(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"import_errors": importErrMap()})
}

type taskInfo struct {
	TaskID      string   `json:"task_id"`
	Upstream    []string `json:"upstream"`
	TriggerRule string   `json:"trigger_rule"`
	Retries     int      `json:"retries"`
	Timeout     string   `json:"timeout"`
	Pool        string   `json:"pool"`
	Priority    int      `json:"priority_weight"`
	SLA         string   `json:"sla,omitempty"`
	IsBranch    bool     `json:"is_branch"`
	IsSensor    bool     `json:"is_sensor"`
	IsMapped    bool     `json:"is_mapped"`
	Outlets     []string `json:"outlets,omitempty"`
}

func describeTasks(d *dag.DAG) []taskInfo {
	out := []taskInfo{}
	for _, t := range d.Tasks() {
		up := []string{}
		for _, u := range t.Upstream() {
			up = append(up, u.ID)
		}
		ti := taskInfo{TaskID: t.ID, Upstream: up, TriggerRule: string(t.TriggerRule()), Retries: t.Retries(),
			Timeout: "-", Pool: t.Pool(), Priority: t.PriorityWeight(), IsBranch: t.IsBranch(),
			IsSensor: t.Sensor() != nil, IsMapped: t.MapSource() != nil, Outlets: t.Outlets()}
		if t.Timeout() > 0 {
			ti.Timeout = t.Timeout().String()
		}
		if t.SLA() > 0 {
			ti.SLA = t.SLA().String()
		}
		out = append(out, ti)
	}
	return out
}

func (s *Server) getDag(w http.ResponseWriter, r *http.Request) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return
	}
	n, _ := strconv.Atoi(r.URL.Query().Get("runs"))
	if n <= 0 || n > 200 {
		n = 25
	}
	runs, _ := s.Store.ListRuns(d.ID)
	rs := []runSummary{}
	for _, run := range lastN(runs, n) {
		rs = append(rs, summarize(run, true))
	}
	m := s.Store.GetDagMeta(d.ID)
	writeJSON(w, 200, map[string]any{
		"dag_id": d.ID, "schedule": scheduleLabel(d), "description": d.Description, "tags": d.Tags,
		"source": d.Source, "catchup": d.Catchup, "max_active_runs": d.MaxActiveRuns, "params": d.Params,
		"datasets": d.Datasets, "is_paused": m.IsPaused, "next_data_interval_start": m.NextDataIntervalStart,
		"tasks": describeTasks(d), "runs": rs, "total_runs": len(runs),
	})
}

func (s *Server) patchDag(w http.ResponseWriter, r *http.Request) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return
	}
	var body struct {
		IsPaused *bool `json:"is_paused"`
	}
	if err := readBody(r, &body); err != nil || body.IsPaused == nil {
		fail(w, 400, "body must be {\"is_paused\": true|false}")
		return
	}
	m := s.Store.GetDagMeta(d.ID)
	m.IsPaused = *body.IsPaused
	if err := s.Store.SaveDagMeta(m); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"dag_id": d.ID, "is_paused": m.IsPaused})
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return
	}
	runs, _ := s.Store.ListRuns(d.ID)
	state := r.URL.Query().Get("state")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	out := []runSummary{}
	for i := len(runs) - 1; i >= 0 && len(out) < limit; i-- {
		if state == "" || runs[i].State == state {
			out = append(out, summarize(runs[i], false))
		}
	}
	writeJSON(w, 200, map[string]any{"dag_runs": out, "total_entries": len(runs)})
}

func (s *Server) trigger(w http.ResponseWriter, r *http.Request) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return
	}
	var body struct {
		Conf  map[string]any `json:"conf"`
		RunID string         `json:"dag_run_id"`
	}
	if err := readBody(r, &body); err != nil {
		fail(w, 400, "conf must be a JSON object: %v", err)
		return
	}
	now := time.Now().UTC()
	run := engine.NewDagRun(d, engine.RunTypeManual, body.RunID, now, now, now, body.Conf)
	if s.Store.RunExists(d.ID, run.RunID) {
		fail(w, 409, "run %s already exists", run.RunID)
		return
	}
	if err := s.Store.SaveRun(run); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	writeJSON(w, 201, summarize(run, false))
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	if _, run, ok := s.runOr404(w, r); ok {
		writeJSON(w, 200, run)
	}
}

func (s *Server) deleteRun(w http.ResponseWriter, r *http.Request) {
	d, run, ok := s.runOr404(w, r)
	if !ok {
		return
	}
	if run.State == engine.RunRunning {
		fail(w, 409, "run is still running")
		return
	}
	if err := s.Store.DeleteRun(d.ID, run.RunID); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) clear(w http.ResponseWriter, r *http.Request) {
	d, run, ok := s.runOr404(w, r)
	if !ok {
		return
	}
	var body struct {
		TaskIDs    []string `json:"task_ids"`
		Upstream   bool     `json:"include_upstream"`
		Downstream bool     `json:"include_downstream"`
		OnlyFailed bool     `json:"only_failed"`
	}
	if err := readBody(r, &body); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	cleared, err := engine.ClearTasks(d, run, body.TaskIDs, body.Upstream, body.Downstream, body.OnlyFailed)
	if err != nil {
		fail(w, 409, "%v", err)
		return
	}
	if len(cleared) > 0 {
		if err := s.Store.SaveRun(run); err != nil {
			fail(w, 500, "%v", err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"cleared": cleared})
}

func (s *Server) listTIs(w http.ResponseWriter, r *http.Request) {
	d, run, ok := s.runOr404(w, r)
	if !ok {
		return
	}
	out := []*engine.TaskInstance{}
	for _, t := range d.Tasks() {
		if ti := run.Tasks[t.ID]; ti != nil {
			out = append(out, ti)
		}
		out = append(out, run.Mapped[t.ID]...)
	}
	writeJSON(w, 200, map[string]any{"task_instances": out, "total_entries": len(out)})
}

func (s *Server) log(w http.ResponseWriter, r *http.Request) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return
	}
	if _, ok := d.Task(r.PathValue("task")); !ok {
		fail(w, 404, "task %q not found", r.PathValue("task"))
		return
	}
	try, _ := strconv.Atoi(r.PathValue("try"))
	if try < 1 {
		fail(w, 400, "try must be >= 1")
		return
	}
	idx := -1
	if v := r.URL.Query().Get("map_index"); v != "" {
		idx, _ = strconv.Atoi(v)
	}
	b, err := os.ReadFile(engine.TaskLogPath(s.Store, d.ID, r.PathValue("run"), r.PathValue("task"), idx, try))
	if err != nil {
		fail(w, 404, "no log for try %d yet", try)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(b)
}

// ---- datasets & pools ------------------------------------------------------------

func (s *Server) datasets(w http.ResponseWriter, r *http.Request) {
	ds, err := s.Store.Datasets()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	type item struct {
		URI         string                `json:"uri"`
		LastUpdated *time.Time            `json:"last_updated,omitempty"`
		Producers   []string              `json:"producing_tasks"`
		Consumers   []string              `json:"consuming_dags"`
		Events      []engine.DatasetEvent `json:"recent_events"`
	}
	all := map[string]*item{}
	get := func(u string) *item {
		if all[u] == nil {
			all[u] = &item{URI: u, Producers: []string{}, Consumers: []string{}, Events: []engine.DatasetEvent{}}
		}
		return all[u]
	}
	for _, d := range dag.List() {
		for _, u := range d.Datasets {
			get(u).Consumers = append(get(u).Consumers, d.ID)
		}
		for _, t := range d.Tasks() {
			for _, u := range t.Outlets() {
				get(u).Producers = append(get(u).Producers, d.ID+"."+t.ID)
			}
		}
	}
	for u, d := range ds {
		it := get(u)
		lu := d.LastUpdated
		it.LastUpdated = &lu
		it.Events = lastEvents(d.Events, 10)
	}
	out := make([]*item, 0, len(all))
	for _, it := range all {
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URI < out[j].URI })
	writeJSON(w, 200, map[string]any{"datasets": out})
}

func lastEvents(ev []engine.DatasetEvent, n int) []engine.DatasetEvent {
	if len(ev) > n {
		ev = ev[len(ev)-n:]
	}
	out := make([]engine.DatasetEvent, len(ev))
	for i := range ev {
		out[i] = ev[len(ev)-1-i]
	}
	return out
}

func (s *Server) listPools(w http.ResponseWriter, r *http.Request) {
	pools, err := s.Store.Pools()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	// occupancy from task instances of active runs
	running, queued := map[string]int{}, map[string]int{}
	for _, d := range dag.List() {
		runs, _ := s.Store.ListRuns(d.ID)
		for _, run := range runs {
			if run.State != engine.RunRunning {
				continue
			}
			tis := []*engine.TaskInstance{}
			for id, ti := range run.Tasks {
				if len(run.Mapped[id]) == 0 {
					tis = append(tis, ti)
				}
				tis = append(tis, run.Mapped[id]...)
			}
			for _, ti := range tis {
				pool := ti.Pool
				if t, ok := d.Task(ti.TaskID); ok {
					pool = t.Pool()
				}
				switch ti.State {
				case dag.StateRunning:
					running[pool]++
				case dag.StateQueued:
					queued[pool]++
				}
			}
		}
	}
	type item struct {
		Name        string `json:"name"`
		Slots       int    `json:"slots"`
		Running     int    `json:"running_slots"`
		Queued      int    `json:"queued_slots"`
		Description string `json:"description"`
	}
	out := []item{}
	for _, p := range pools {
		out = append(out, item{p.Name, p.Slots, running[p.Name], queued[p.Name], p.Description})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, 200, map[string]any{"pools": out})
}

func (s *Server) putPool(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name        string `json:"name"`
		Slots       *int   `json:"slots"`
		Description string `json:"description"`
	}
	if err := readBody(r, &body); err != nil || body.Slots == nil {
		fail(w, 400, "body must be {\"slots\": n} (plus \"name\" when not in the URL)")
		return
	}
	name := firstOf(r.PathValue("name"), body.Name)
	if name == "" {
		fail(w, 400, "pool name is required: PUT /api/v1/pools/{name} or \"name\" in the body")
		return
	}
	if err := s.Store.SetPool(name, *body.Slots, body.Description); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"name": name, "slots": *body.Slots})
}

func (s *Server) deletePool(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DeletePool(r.PathValue("name")); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- variables & connections ---------------------------------------------------------

func (s *Server) listVariables(w http.ResponseWriter, r *http.Request) {
	vars, err := s.Store.Variables()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	out := []map[string]any{}
	for _, v := range vars {
		val, err := v.Value()
		if err != nil {
			val = "<" + err.Error() + ">"
		}
		out = append(out, map[string]any{"key": v.Key, "value": val, "description": v.Description,
			"encrypted": v.Secret.Encrypted, "updated_at": v.UpdatedAt})
	}
	writeJSON(w, 200, map[string]any{"variables": out, "encryption_enabled": engine.EncryptionEnabled()})
}

func (s *Server) getVariable(w http.ResponseWriter, r *http.Request) {
	v, err := dag.GetVariable(r.PathValue("key"))
	if err != nil {
		fail(w, 404, "%v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"key": r.PathValue("key"), "value": v})
}

func (s *Server) putVariable(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key         string  `json:"key"`
		Value       *string `json:"value"`
		Description string  `json:"description"`
	}
	if err := readBody(r, &body); err != nil || body.Value == nil {
		fail(w, 400, "body must be {\"value\": \"...\"} (plus \"key\" when not in the URL)")
		return
	}
	key := firstOf(r.PathValue("key"), body.Key)
	if key == "" {
		fail(w, 400, "variable key is required: PUT /api/v1/variables/{key} or \"key\" in the body")
		return
	}
	if err := s.Store.SetVariable(key, *body.Value, body.Description); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"key": key})
}

func (s *Server) deleteVariable(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DeleteVariable(r.PathValue("key")); err != nil {
		fail(w, 404, "%v", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var sensitive = []string{"password", "passwd", "secret", "token", "key", "webhook", "credential", "private"}

func mask(cfg map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range cfg {
		lk := strings.ToLower(k)
		out[k] = v
		for _, s := range sensitive {
			if strings.Contains(lk, s) {
				out[k] = "***"
			}
		}
	}
	return out
}

func (s *Server) listConnections(w http.ResponseWriter, r *http.Request) {
	conns, err := s.Store.Connections()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	out := []map[string]any{}
	for _, c := range conns {
		item := map[string]any{"conn_id": c.ID, "conn_type": c.Type, "description": c.Description,
			"encrypted": c.Secret.Encrypted, "updated_at": c.UpdatedAt}
		if dec, err := c.Decode(); err == nil {
			item["config"] = mask(dec.Config)
		} else {
			item["error"] = err.Error()
		}
		out = append(out, item)
	}
	writeJSON(w, 200, map[string]any{"connections": out, "encryption_enabled": engine.EncryptionEnabled()})
}

func (s *Server) putConnection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ConnID       string         `json:"conn_id"`
		ConnectionID string         `json:"connection_id"` // Airflow's field name
		Type         string         `json:"conn_type"`
		Config       map[string]any `json:"config"`
		Description  string         `json:"description"`
	}
	if err := readBody(r, &body); err != nil || body.Type == "" {
		fail(w, 400, "body must be {\"conn_type\": \"...\", \"config\": {...}} (plus \"conn_id\" when not in the URL)")
		return
	}
	id := firstOf(r.PathValue("id"), body.ConnID, body.ConnectionID)
	if id == "" {
		fail(w, 400, "conn_id is required: PUT /api/v1/connections/{conn_id} or \"conn_id\" in the body")
		return
	}
	if err := s.Store.SetConnection(id, dag.Connection{Type: body.Type, Config: body.Config}, body.Description); err != nil {
		fail(w, 500, "%v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"conn_id": id})
}

func (s *Server) deleteConnection(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DeleteConnection(r.PathValue("id")); err != nil {
		fail(w, 404, "%v", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- users ------------------------------------------------------------------------------

func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := s.Store.Users()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	out := []map[string]any{}
	for _, u := range users {
		out = append(out, map[string]any{"username": u.Username, "role": u.Role, "api_tokens": len(u.Tokens), "created_at": u.CreatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["username"].(string) < out[j]["username"].(string) })
	writeJSON(w, 200, map[string]any{"users": out})
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	var body struct{ Username, Password, Role string }
	if err := readBody(r, &body); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	role, err := engine.ParseRole(body.Role)
	if err != nil {
		fail(w, 400, "%v", err)
		return
	}
	if err := s.Store.CreateUser(body.Username, body.Password, role); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	writeJSON(w, 201, map[string]any{"username": body.Username, "role": role})
}

func (s *Server) deleteUser(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DeleteUser(r.PathValue("user")); err != nil {
		fail(w, 404, "%v", err)
		return
	}
	s.authMu.Lock()
	s.authCache = nil
	s.authMu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) issueToken(w http.ResponseWriter, r *http.Request) {
	tok, err := s.Store.IssueToken(r.PathValue("user"))
	if err != nil {
		fail(w, 404, "%v", err)
		return
	}
	writeJSON(w, 201, map[string]any{"token": tok, "note": "shown once; send as Authorization: Bearer <token>"})
}

func firstOf(vals ...string) string {
	for _, v := range vals {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}
