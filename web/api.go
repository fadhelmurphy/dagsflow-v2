package web

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
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

	// Reparse reloads a DAG's source file in this process and asks the other
	// processes (scheduler) to do the same. Set by the CLI.
	Reparse func(d *dag.DAG) error

	// Visual builder: where workflows are stored and how to reload the DAG
	// folder (and tell the other processes). Set by the CLI.
	BuilderDir string
	Reload     func(path string) error

	builderMu sync.Mutex

	// TLSCert/TLSKey: serve HTTPS.
	TLSCert, TLSKey string

	limiter loginLimiter

	rolesCache map[string]*engine.RoleDef
	rolesAt    time.Time
}

type authEntry struct {
	user *engine.User
	exp  time.Time
}

type ctxKey struct{}

var anonymousAdmin = &engine.User{Username: "anonymous", Role: engine.RoleAdmin, Roles: []string{string(engine.RoleAdmin)}}

func userFrom(r *http.Request) *engine.User {
	if p, _ := r.Context().Value(ctxKey{}).(*principal); p != nil {
		return p.u
	}
	return nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	type route struct {
		pattern string
		perm    string // "" public, "auth" any signed-in user, else an engine.Act* action
		h       http.HandlerFunc
	}
	const auth = "auth"
	routes := []route{
		{"GET /api/v1/health", "", s.health},
		{"GET /api/v1/components", engine.ActSystemRead, s.components},
		{"POST /api/v1/login", "", s.login},
		{"POST /api/v1/logout", "", s.logout},
		{"GET /api/v1/me", auth, s.me},
		{"GET /api/v1/dags", auth, s.listDags}, // filtered by dag.read
		{"GET /api/v1/dags/{dag}", engine.ActDagRead, s.getDag},
		{"PATCH /api/v1/dags/{dag}", engine.ActDagEdit, s.patchDag},
		{"DELETE /api/v1/dags/{dag}", engine.ActDagDelete, s.deleteDag},
		{"GET /api/v1/dags/{dag}/dagRuns", engine.ActDagRead, s.listRuns},
		{"POST /api/v1/dags/{dag}/dagRuns", engine.ActDagTrigger, s.trigger},
		{"GET /api/v1/dags/{dag}/dagRuns/{run}", engine.ActDagRead, s.getRun},
		{"GET /api/v1/dags/{dag}/dagRuns/{run}/xcom", engine.ActDagRead, s.getXComValue},
		{"DELETE /api/v1/dags/{dag}/dagRuns/{run}", engine.ActDagDelete, s.deleteRun},
		{"POST /api/v1/dags/{dag}/dagRuns/{run}/clear", engine.ActDagEdit, s.clear},
		{"GET /api/v1/dags/{dag}/dagRuns/{run}/taskInstances", engine.ActDagRead, s.listTIs},
		{"GET /api/v1/dags/{dag}/dagRuns/{run}/taskInstances/{task}/logs/{try}", engine.ActDagRead, s.log},
		{"GET /api/v1/dags/{dag}/source", engine.ActDagRead, s.dagSource},
		{"POST /api/v1/dags/{dag}/reparse", engine.ActDagEdit, s.reparse},
		{"POST /api/v1/dags/{dag}/dagRuns/{run}/mark", engine.ActDagEdit, s.markRun},
		{"POST /api/v1/dags/{dag}/dagRuns/{run}/taskInstances/{task}/mark", engine.ActDagEdit, s.markTask},
		{"GET /api/v1/dags/{dag}/dagRuns/{run}/taskInstances/{task}/rendered", engine.ActDagRead, s.rendered},
		{"PUT /api/v1/dags/{dag}/dagRuns/{run}/note", engine.ActDagEdit, s.runNote},
		{"PUT /api/v1/dags/{dag}/dagRuns/{run}/taskInstances/{task}/note", engine.ActDagEdit, s.tiNote},
		{"GET /api/v1/taskInstances", auth, s.taskInstances}, // filtered by dag.read
		{"GET /api/v1/eventLogs", engine.ActEventsRead, s.eventLogs},
		{"GET /api/v1/importErrors", engine.ActSystemRead, s.importErrors},
		{"GET /api/v1/datasets", engine.ActDatasets, s.datasets},
		{"GET /api/v1/pools", engine.ActPoolsRead, s.listPools},
		{"PUT /api/v1/pools/{name}", engine.ActPoolsEdit, s.putPool},
		{"POST /api/v1/pools", engine.ActPoolsEdit, s.putPool},
		{"PUT /api/v1/pools", engine.ActPoolsEdit, s.putPool},
		{"DELETE /api/v1/pools/{name}", engine.ActPoolsEdit, s.deletePool},
		{"GET /api/v1/variables", engine.ActVarsRead, s.listVariables},
		{"GET /api/v1/variables/{key}", engine.ActVarsRead, s.getVariable},
		{"PUT /api/v1/variables/{key}", engine.ActVarsEdit, s.putVariable},
		{"POST /api/v1/variables", engine.ActVarsEdit, s.putVariable},
		{"PUT /api/v1/variables", engine.ActVarsEdit, s.putVariable},
		{"DELETE /api/v1/variables/{key}", engine.ActVarsEdit, s.deleteVariable},
		{"GET /api/v1/connections", engine.ActConnsRead, s.listConnections},
		{"PUT /api/v1/connections/{id}", engine.ActConnsEdit, s.putConnection},
		{"POST /api/v1/connections", engine.ActConnsEdit, s.putConnection},
		{"PUT /api/v1/connections", engine.ActConnsEdit, s.putConnection},
		{"DELETE /api/v1/connections/{id}", engine.ActConnsEdit, s.deleteConnection},
		{"GET /api/v1/roles", auth, s.roles},
		{"GET /api/v1/permissions", auth, s.permissionsList},
		{"POST /api/v1/roles", engine.ActRolesManage, s.createRole},
		{"PATCH /api/v1/roles/{role}", engine.ActRolesManage, s.patchRole},
		{"DELETE /api/v1/roles/{role}", engine.ActRolesManage, s.deleteRole},
		{"GET /api/v1/users", engine.ActUsersManage, s.listUsers},
		{"POST /api/v1/users", engine.ActUsersManage, s.createUser},
		{"GET /api/v1/users/{user}", engine.ActUsersManage, s.getUser},
		{"PATCH /api/v1/users/{user}", engine.ActUsersManage, s.patchUser},
		{"DELETE /api/v1/users/{user}", engine.ActUsersManage, s.deleteUser},
		{"POST /api/v1/users/{user}/tokens", engine.ActUsersManage, s.issueToken},
		{"GET /api/v1/builder/nodes", engine.ActBuilderEdit, s.builderNodes},
		{"GET /api/v1/builder/custom-nodes/{type}", engine.ActBuilderEdit, s.customNodeGet},
		{"PUT /api/v1/builder/custom-nodes/{type}", engine.ActBuilderEdit, s.customNodePut},
		{"GET /api/v1/builder/custom-nodes/{type}/usage", engine.ActBuilderEdit, s.customNodeUsageGet},
		{"DELETE /api/v1/builder/custom-nodes/{type}", engine.ActBuilderEdit, s.customNodeDelete},
		{"GET /api/v1/builder/workflows", engine.ActBuilderEdit, s.builderList},
		{"POST /api/v1/builder/validate", engine.ActBuilderEdit, s.builderValidate},
		{"POST /api/v1/builder/run-node", engine.ActBuilderEdit, s.builderRunNode},
		{"GET /api/v1/builder/workflows/{dag}", engine.ActBuilderEdit, s.builderGet},
		{"PUT /api/v1/builder/workflows/{dag}", engine.ActBuilderEdit, s.builderPut},
		{"DELETE /api/v1/builder/workflows/{dag}", engine.ActBuilderEdit, s.builderDelete},
		{"GET /api/v1/builder/files", engine.ActBuilderEdit, s.builderFiles},
		{"POST /api/v1/builder/files", engine.ActBuilderEdit, s.builderFileUpload},
		{"DELETE /api/v1/builder/files/{name}", engine.ActBuilderEdit, s.builderFileDelete},
		{"GET /api/v1/builder/files/{name}/preview", engine.ActBuilderEdit, s.builderFilePreview},
	}
	for _, rt := range routes {
		mux.Handle(rt.pattern, s.require(rt.perm, rt.h))
	}
	// probes and Prometheus: no data, no auth (metrics can be turned off or
	// protected with DAGSFLOW_METRICS_TOKEN)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Store.Ping(); err != nil {
			http.Error(w, "metadata database unreachable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("GET /metrics", s.metrics)
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
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		if secureRequest(r) {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000")
		}
		next.ServeHTTP(w, r)
	})
}

// require authenticates the request and checks the permission: a global
// action, or a DAG action on the DAG in the path.
func (s *Server) require(perm string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if perm == "" {
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
		a := s.access(u)
		if perm != "auth" {
			allowed := false
			if dagID := r.PathValue("dag"); dagID != "" && strings.HasPrefix(perm, "dag.") {
				allowed = a.DAG(perm, dagID)
			} else {
				allowed = a.Can(perm)
			}
			if !allowed {
				what := perm
				if dagID := r.PathValue("dag"); dagID != "" {
					what += " on DAG " + dagID
				}
				fail(w, http.StatusForbidden, "your roles (%s) do not allow %s", strings.Join(a.Roles, ", "), what)
				return
			}
		}
		h(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, &principal{u, a})))
	})
}

type principal struct {
	u *engine.User
	a *engine.Access
}

// access resolves a user's permissions (roles cached for a few seconds).
func (s *Server) access(u *engine.User) *engine.Access {
	if u == anonymousAdmin {
		return engine.FullAccess()
	}
	s.authMu.Lock()
	roles, fresh := s.rolesCache, time.Since(s.rolesAt) < 5*time.Second
	s.authMu.Unlock()
	if !fresh || roles == nil {
		if rs, err := s.Store.Roles(); err == nil {
			roles = rs
			s.authMu.Lock()
			s.rolesCache, s.rolesAt = rs, time.Now()
			s.authMu.Unlock()
		}
	}
	return engine.AccessFor(u, roles)
}

func accessFrom(r *http.Request) *engine.Access {
	if p, _ := r.Context().Value(ctxKey{}).(*principal); p != nil {
		return p.a
	}
	return &engine.Access{}
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
	keys := []string{"u:" + strings.ToLower(name), "ip:" + clientIP(r)}
	u, ok := s.cached("b:"+name+"\x00"+pw, func() (*engine.User, bool) {
		if s.loginBlocked(keys...) > 0 { // same lockout as the login form
			return nil, false
		}
		u, ok := s.Store.Authenticate(name, pw, "")
		if !ok {
			s.loginFailed(keys...)
		}
		return u, ok
	})
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

// Serve runs the HTTP server until ctx is cancelled (then drains requests
// for up to 15s).
func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler(),
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 60 * time.Second,
		WriteTimeout: 120 * time.Second, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 64 << 10,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(sh)
	}()
	var err error
	if s.TLSCert != "" || s.TLSKey != "" {
		if s.TLSCert == "" || s.TLSKey == "" {
			return errors.New("both --tls-cert and --tls-key are needed for HTTPS")
		}
		fmt.Printf("[webserver] listening on https://%s (API: /api/v1)\n", addr)
		err = srv.ListenAndServeTLS(s.TLSCert, s.TLSKey)
	} else {
		fmt.Printf("[webserver] listening on http://%s (API: /api/v1)\n", addr)
		err = srv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// metrics serves Prometheus metrics. DAGSFLOW_METRICS=off disables it;
// DAGSFLOW_METRICS_TOKEN requires "Authorization: Bearer <token>".
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if strings.EqualFold(os.Getenv("DAGSFLOW_METRICS"), "off") {
		fail(w, 404, "metrics are disabled")
		return
	}
	if tok := os.Getenv("DAGSFLOW_METRICS_TOKEN"); tok != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(tok)) != 1 {
			fail(w, http.StatusUnauthorized, "metrics token required")
			return
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	engine.WriteMetrics(w, s.Store)
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

// failStore: a concurrent change (another webserver, or a scheduler that just
// started the run) is a 409 the client can retry, anything else a 500.
func failStore(w http.ResponseWriter, err error) {
	if errors.Is(err, engine.ErrConflict) {
		fail(w, http.StatusConflict, "%v", err)
		return
	}
	fail(w, 500, "%v", err)
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
	TIs         map[string]tiLite    `json:"task_instances,omitempty"`
	SLAMisses   int                  `json:"sla_misses,omitempty"`
}

// tiLite: what the grid, Gantt and Task Duration views need per task instance.
type tiLite struct {
	State     dag.State  `json:"state"`
	StartDate *time.Time `json:"start_date,omitempty"`
	EndDate   *time.Time `json:"end_date,omitempty"`
	TryNumber int        `json:"try_number"`
	Mapped    int        `json:"mapped,omitempty"`
}

func summarize(r *engine.DagRun, withTasks bool) runSummary {
	s := runSummary{RunID: r.RunID, RunType: r.RunType, State: r.State, LogicalDate: r.LogicalDate,
		StartDate: r.StartDate, EndDate: r.EndDate, SLAMisses: len(r.SLAMisses)}
	if withTasks {
		s.Tasks, s.TIs = map[string]dag.State{}, map[string]tiLite{}
		for id, ti := range r.Tasks {
			s.Tasks[id] = ti.State
			s.TIs[id] = tiLite{ti.State, ti.StartDate, ti.EndDate, ti.TryNumber, len(r.Mapped[id])}
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

// health ~ Airflow's /health: metadatabase + scheduler status (public, no data).
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	pid, alive := engine.SchedulerInfo(s.Store)
	db := map[string]any{"status": "healthy", "backend": s.Store.Backend()}
	if err := s.Store.Ping(); err != nil {
		db = map[string]any{"status": "unhealthy", "backend": s.Store.Backend()}
	}
	sched := map[string]any{"status": map[bool]string{true: "healthy", false: "unhealthy"}[alive]}
	out := map[string]any{"metadatabase": db, "scheduler": sched,
		// kept for older clients
		"scheduler_alive": alive, "scheduler_pid": pid}
	if cs, err := s.Store.Components(); err == nil && s.Store.Distributed() {
		counts := map[string]int{}
		for _, c := range cs {
			if c.Alive() {
				counts[c.Kind]++
			}
		}
		out["components"] = counts
	}
	writeJSON(w, 200, out)
}

// components lists schedulers, webservers and workers (Admin → Components).
func (s *Server) components(w http.ResponseWriter, r *http.Request) {
	cs, err := s.Store.Components()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	type item struct {
		engine.Component
		Alive bool    `json:"alive"`
		Age   float64 `json:"heartbeat_age_seconds"`
	}
	out := []item{}
	for _, c := range cs {
		out = append(out, item{c, c.Alive(), time.Since(c.Heartbeat).Seconds()})
	}
	writeJSON(w, 200, map[string]any{"backend": s.Store.Backend(), "components": out})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	u, a := userFrom(r), accessFrom(r)
	writeJSON(w, 200, map[string]any{"username": u.Username, "role": u.Role, "roles": a.Roles,
		"permissions": a.Permissions(), "can": a.Global(), "auth_enabled": u != anonymousAdmin})
}

type dagListEntry struct {
	DagID       string          `json:"dag_id"`
	Owner       string          `json:"owner"`
	Schedule    string          `json:"schedule"`
	Description string          `json:"description"`
	Tags        []string        `json:"tags"`
	Source      string          `json:"source"`
	IsPaused    bool            `json:"is_paused"`
	Next        *time.Time      `json:"next_data_interval_start,omitempty"`
	Recent      []runSummary    `json:"recent_runs"`
	Active      int             `json:"active_runs"`
	RunCounts   map[string]int  `json:"run_counts"`
	LastRun     *runSummary     `json:"last_run,omitempty"`
	TaskCounts  map[string]int  `json:"recent_task_counts"`
	Perms       map[string]bool `json:"permissions"`
}

func (s *Server) listDags(w http.ResponseWriter, r *http.Request) {
	a := accessFrom(r)
	var dags []*dag.DAG
	for _, d := range dag.List() {
		if a.CanDAG(engine.ActDagRead, d.ID, d.Tags) {
			dags = append(dags, d)
		}
	}
	out := make([]dagListEntry, len(dags))
	// a few DAGs at a time: each needs its own metadata queries (round trips to the database)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i, d := range dags {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, d *dag.DAG) {
			defer func() { <-sem; wg.Done() }()
			out[i] = s.dagListItem(a, d)
		}(i, d)
	}
	wg.Wait()
	errs := map[string]string{}
	if a.Can(engine.ActSystemRead) {
		errs = importErrMap()
	}
	writeJSON(w, 200, map[string]any{"dags": out, "total_entries": len(out), "import_errors": errs})
}

func (s *Server) dagListItem(a *engine.Access, d *dag.DAG) dagListEntry {
	type item = dagListEntry
	{
		m := s.Store.GetDagMeta(d.ID)
		counts, runs, _ := s.Store.RunSummary(d.ID, 14) // counts + the latest runs only: the list refreshes every few seconds
		it := item{DagID: d.ID, Owner: d.Owner(), Schedule: scheduleLabel(d), Description: d.Description, Tags: d.Tags,
			Source: d.Source, IsPaused: m.IsPaused, Next: m.NextDataIntervalStart, Recent: []runSummary{},
			RunCounts: map[string]int{}, TaskCounts: map[string]int{}, Perms: a.DagPermissions(d.ID)}
		if it.Tags == nil {
			it.Tags = []string{}
		}
		for st, n := range counts {
			it.RunCounts[st] = n
		}
		it.Active = counts[engine.RunQueued] + counts[engine.RunRunning]
		for _, run := range runs {
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
		return it
	}
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
	runs, _ := s.Store.ListRunsLite(d.ID)
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
	s.audit(r, "delete_dag", d.ID, "", "", "runs, metadata and logs")
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
	TaskID      string          `json:"task_id"`
	Upstream    []string        `json:"upstream"`
	TriggerRule string          `json:"trigger_rule"`
	Retries     int             `json:"retries"`
	Timeout     string          `json:"timeout"`
	Pool        string          `json:"pool"`
	Priority    int             `json:"priority_weight"`
	SLA         string          `json:"sla,omitempty"`
	IsBranch    bool            `json:"is_branch"`
	IsSensor    bool            `json:"is_sensor"`
	IsMapped    bool            `json:"is_mapped"`
	Outlets     []string        `json:"outlets,omitempty"`
	Operator    string          `json:"operator"`
	ExtraLinks  []dag.ExtraLink `json:"extra_links,omitempty"`
	Templates   []string        `json:"template_fields"`
	RetryDelay  string          `json:"retry_delay"`
	PoolSlots   int             `json:"pool_slots"`
	Downstream  []string        `json:"downstream"`
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
		ti.Operator, ti.ExtraLinks, ti.RetryDelay, ti.PoolSlots = t.Operator(), t.ExtraLinks(), t.RetryDelay().String(), t.PoolSlots()
		ti.Templates, ti.Downstream = []string{}, []string{}
		for _, f := range t.TemplateFields() {
			ti.Templates = append(ti.Templates, f.Name)
		}
		for _, dn := range t.Downstream() {
			ti.Downstream = append(ti.Downstream, dn.ID)
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
	q := r.URL.Query()
	n, _ := strconv.Atoi(q.Get("runs"))
	if n <= 0 || n > 365 {
		n = 25
	}
	var base time.Time
	if v := q.Get("base_date"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			base = t
		}
	}
	runs, _ := s.Store.ListRunsLite(d.ID)
	counts := map[string]int{}
	var filtered []*engine.DagRun
	for _, run := range runs {
		counts[run.State]++
		if (base.IsZero() || !run.LogicalDate.After(base)) &&
			(q.Get("run_type") == "" || run.RunType == q.Get("run_type")) &&
			(q.Get("state") == "" || run.State == q.Get("state")) {
			filtered = append(filtered, run)
		}
	}
	rs := []runSummary{}
	for _, run := range lastN(filtered, n) {
		rs = append(rs, summarize(run, true))
	}
	m := s.Store.GetDagMeta(d.ID)
	writeJSON(w, 200, map[string]any{
		"dag_id": d.ID, "owner": d.Owner(), "schedule": scheduleLabel(d), "description": d.Description, "tags": d.Tags,
		"source": d.Source, "file": d.File, "is_file_source": d.IsFileSource(), "parsed_at": d.ParsedAt,
		"catchup": d.Catchup, "max_active_runs": d.MaxActiveRuns, "params": d.Params,
		"datasets": d.Datasets, "is_paused": m.IsPaused, "next_data_interval_start": m.NextDataIntervalStart,
		"tasks": describeTasks(d), "runs": rs, "total_runs": len(runs), "filtered_runs": len(filtered), "run_counts": counts,
		"permissions": accessFrom(r).DagPermissions(d.ID),
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
	ev := "unpause"
	if m.IsPaused {
		ev = "pause"
	}
	s.audit(r, ev, d.ID, "", "", "")
	writeJSON(w, 200, map[string]any{"dag_id": d.ID, "is_paused": m.IsPaused})
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	d, ok := s.dagOr404(w, r)
	if !ok {
		return
	}
	runs, _ := s.Store.ListRunsLite(d.ID)
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
		failStore(w, err)
		return
	}
	extra := ""
	if len(body.Conf) > 0 {
		b, _ := json.Marshal(body.Conf)
		extra = "conf=" + string(b)
	}
	s.audit(r, "trigger", d.ID, run.RunID, "", extra)
	writeJSON(w, 201, summarize(run, false))
}

// getRun returns the run. Large XCom values are replaced by a preview
// ({"$xcom_ref": bytes, "rows": n, "sample": [first rows]}): the page polls this
// every few seconds. ?xcom=full returns them whole; /xcom returns one value.
func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	_, run, ok := s.runOr404(w, r)
	if !ok {
		return
	}
	if r.URL.Query().Get("xcom") != "full" && len(run.XCom) > 0 {
		cp := *run
		cp.XCom = make(map[string]map[string]any, len(run.XCom))
		for slot, kv := range run.XCom {
			m := make(map[string]any, len(kv))
			for k, v := range kv {
				m[k] = xcomPreview(v)
			}
			cp.XCom[slot] = m
		}
		run = &cp
	}
	writeJSON(w, 200, run)
}

// xcomPreviewBytes: XCom values larger than this are previewed in run details.
const xcomPreviewBytes = 64 << 10

func xcomPreview(v any) any {
	if engine.IsXComRef(v) {
		return v
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) <= xcomPreviewBytes {
		return v
	}
	out := map[string]any{"$xcom_ref": len(b)}
	if list, ok := v.([]any); ok {
		out["rows"] = len(list)
		n := min(len(list), 50)
		out["sample"] = list[:n]
	}
	return out
}

// getXComValue returns one XCom value whole (?slot=task[#mapIndex]&key=…).
func (s *Server) getXComValue(w http.ResponseWriter, r *http.Request) {
	_, run, ok := s.runOr404(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	v, found := run.XCom[q.Get("slot")][firstNonEmpty(q.Get("key"), "return_value")]
	if !found {
		fail(w, 404, "no XCom %q for %s", firstNonEmpty(q.Get("key"), "return_value"), q.Get("slot"))
		return
	}
	if q.Get("download") == "1" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fmt.Sprintf("%s_%s_%s.json", run.RunID, q.Get("slot"), firstNonEmpty(q.Get("key"), "return_value"))))
	}
	writeJSON(w, 200, v)
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
		failStore(w, err)
		return
	}
	s.audit(r, "delete_run", d.ID, run.RunID, "", "")
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
		MapIndex   *int     `json:"map_index"`
	}
	if err := readBody(r, &body); err != nil {
		fail(w, 400, "invalid body: %v", err)
		return
	}
	idx := -1
	if body.MapIndex != nil {
		idx = *body.MapIndex
	}
	cleared, err := engine.ClearTaskInstances(d, run, engine.ClearOptions{TaskIDs: body.TaskIDs, Upstream: body.Upstream,
		Downstream: body.Downstream, OnlyFailed: body.OnlyFailed, MapIndex: idx})
	if err != nil {
		fail(w, 409, "%v", err)
		return
	}
	if len(cleared) > 0 {
		if err := s.Store.SaveRun(run); err != nil {
			failStore(w, err)
			return
		}
		task := ""
		if len(body.TaskIDs) == 1 {
			task = body.TaskIDs[0]
		}
		s.audit(r, "clear", d.ID, run.RunID, task, strings.Join(cleared, ", "))
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
	b, err := s.Store.ReadTaskLog(engine.LogKey{DagID: d.ID, RunID: r.PathValue("run"), TaskID: r.PathValue("task"), MapIndex: idx, Try: try})
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
		runs, _ := s.Store.ListRunsLite(d.ID)
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
	s.audit(r, "pool_set", "", "", "", fmt.Sprintf("%s=%d", name, *body.Slots))
	writeJSON(w, 200, map[string]any{"name": name, "slots": *body.Slots})
}

func (s *Server) deletePool(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DeletePool(r.PathValue("name")); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	s.audit(r, "pool_delete", "", "", "", r.PathValue("name"))
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
	s.audit(r, "variable_set", "", "", "", key)
	writeJSON(w, 200, map[string]any{"key": key})
}

func (s *Server) deleteVariable(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DeleteVariable(r.PathValue("key")); err != nil {
		fail(w, 404, "%v", err)
		return
	}
	s.audit(r, "variable_delete", "", "", "", r.PathValue("key"))
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
	s.audit(r, "connection_set", "", "", "", id)
	writeJSON(w, 200, map[string]any{"conn_id": id})
}

func (s *Server) deleteConnection(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DeleteConnection(r.PathValue("id")); err != nil {
		fail(w, 404, "%v", err)
		return
	}
	s.audit(r, "connection_delete", "", "", "", r.PathValue("id"))
	w.WriteHeader(http.StatusNoContent)
}

// ---- users ------------------------------------------------------------------------------

func userJSON(u *engine.User) map[string]any {
	return map[string]any{"username": u.Username, "first_name": u.FirstName, "last_name": u.LastName,
		"email": u.Email, "role": u.Role, "roles": u.RoleNames(), "active": u.Active(), "api_tokens": len(u.Tokens),
		"created_at": u.CreatedAt, "changed_at": u.ChangedAt, "last_login": u.LastLogin, "login_count": u.LoginCount}
}

func (s *Server) dropAuthCache() {
	s.authMu.Lock()
	s.authCache = nil
	s.authMu.Unlock()
}

func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := s.Store.Users()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	out := []map[string]any{}
	for _, u := range users {
		out = append(out, userJSON(u))
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["username"].(string) < out[j]["username"].(string) })
	writeJSON(w, 200, map[string]any{"users": out})
}

func (s *Server) getUser(w http.ResponseWriter, r *http.Request) {
	users, _ := s.Store.Users()
	u := users[r.PathValue("user")]
	if u == nil {
		fail(w, 404, "user %q not found", r.PathValue("user"))
		return
	}
	writeJSON(w, 200, userJSON(u))
}

type userBody struct {
	Username  string    `json:"username"`
	Password  *string   `json:"password"`
	FirstName *string   `json:"first_name"`
	LastName  *string   `json:"last_name"`
	Email     *string   `json:"email"`
	Role      *string   `json:"role"`  // one role (older clients)
	Roles     *[]string `json:"roles"` // one or more roles
	Active    *bool     `json:"active"`
}

func (b userBody) update() (engine.UserUpdate, error) {
	up := engine.UserUpdate{FirstName: b.FirstName, LastName: b.LastName, Email: b.Email, Active: b.Active, Password: b.Password}
	if b.Email != nil && *b.Email != "" && !strings.Contains(*b.Email, "@") {
		return up, fmt.Errorf("email looks invalid")
	}
	switch {
	case b.Roles != nil:
		up.Roles = b.Roles
	case b.Role != nil:
		up.Roles = &[]string{*b.Role}
	}
	return up, nil
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	var body userBody
	if err := readBody(r, &body); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	body.Username = strings.TrimSpace(body.Username)
	if body.Password == nil || (body.Role == nil && body.Roles == nil) {
		fail(w, 400, "username, password and roles are required")
		return
	}
	up, err := body.update()
	if err != nil {
		fail(w, 400, "%v", err)
		return
	}
	if err := s.Store.CreateUser(body.Username, *body.Password, *up.Roles...); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	roles := strings.Join(*up.Roles, ",")
	up.Password, up.Roles = nil, nil
	if err := s.Store.UpdateUser(body.Username, up); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	s.audit(r, "user_create", "", "", "", body.Username+" ("+roles+")")
	users, _ := s.Store.Users()
	writeJSON(w, 201, userJSON(users[body.Username]))
}

func (s *Server) patchUser(w http.ResponseWriter, r *http.Request) {
	var body userBody
	if err := readBody(r, &body); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	up, err := body.update()
	if err != nil {
		fail(w, 400, "%v", err)
		return
	}
	name := r.PathValue("user")
	if err := s.Store.UpdateUser(name, up); err != nil {
		code := 400
		if strings.Contains(err.Error(), "not found") {
			code = 404
		}
		fail(w, code, "%v", err)
		return
	}
	s.dropAuthCache()
	var what []string
	if up.Roles != nil {
		what = append(what, "roles="+strings.Join(*up.Roles, ","))
	}
	if up.Active != nil {
		what = append(what, fmt.Sprintf("active=%v", *up.Active))
	}
	if up.Password != nil {
		what = append(what, "password reset")
	}
	s.audit(r, "user_update", "", "", "", name+" "+strings.Join(what, ", "))
	users, _ := s.Store.Users()
	writeJSON(w, 200, userJSON(users[name]))
}

func (s *Server) deleteUser(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("user")
	if me := userFrom(r); me != nil && me.Username == name {
		fail(w, 400, "you cannot delete your own account")
		return
	}
	if err := s.Store.DeleteUser(name); err != nil {
		code := 400
		if strings.Contains(err.Error(), "not found") {
			code = 404
		}
		fail(w, code, "%v", err)
		return
	}
	s.dropAuthCache()
	s.audit(r, "user_delete", "", "", "", name)
	w.WriteHeader(http.StatusNoContent)
}

// ---- roles ------------------------------------------------------------------------------

func (s *Server) roles(w http.ResponseWriter, r *http.Request) {
	rs, err := s.Store.Roles()
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	type item struct {
		*engine.RoleDef
		Users []string `json:"users,omitempty"`
	}
	holders := map[string][]string{}
	if accessFrom(r).Can(engine.ActUsersManage) {
		users, _ := s.Store.Users()
		for _, u := range users {
			for _, n := range u.RoleNames() {
				holders[n] = append(holders[n], u.Username)
			}
		}
	}
	out := []item{}
	for _, rd := range engine.SortedRoles(rs) {
		sort.Strings(holders[rd.Name])
		out = append(out, item{rd, holders[rd.Name]})
	}
	writeJSON(w, 200, map[string]any{"roles": out})
}

// permissionsList: the actions a role can grant (for the role editor).
func (s *Server) permissionsList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"actions": engine.Actions})
}

type roleBody struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Permissions []string `json:"permissions"` // "dag.read:sales_*", "pools.read", ...
}

func (b roleBody) def() (engine.RoleDef, error) {
	d := engine.RoleDef{Name: b.Name, Description: strings.TrimSpace(b.Description)}
	for _, ps := range b.Permissions {
		p, err := engine.ParsePermission(ps)
		if err != nil {
			return d, err
		}
		d.Permissions = append(d.Permissions, p)
	}
	return d, nil
}

func (s *Server) dropRolesCache() {
	s.authMu.Lock()
	s.rolesCache = nil
	s.authMu.Unlock()
}

func (s *Server) createRole(w http.ResponseWriter, r *http.Request) {
	var body roleBody
	if err := readBody(r, &body); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	d, err := body.def()
	if err == nil {
		err = s.Store.CreateRole(d)
	}
	if err != nil {
		fail(w, 400, "%v", err)
		return
	}
	s.dropRolesCache()
	s.audit(r, "role_create", "", "", "", strings.TrimSpace(body.Name)+": "+strings.Join(body.Permissions, " "))
	writeJSON(w, 201, map[string]any{"name": strings.TrimSpace(body.Name)})
}

func (s *Server) patchRole(w http.ResponseWriter, r *http.Request) {
	var body roleBody
	if err := readBody(r, &body); err != nil {
		fail(w, 400, "%v", err)
		return
	}
	name := r.PathValue("role")
	d, err := body.def()
	if err == nil {
		err = s.Store.UpdateRole(name, d)
	}
	if err != nil {
		code := 400
		if strings.Contains(err.Error(), "not found") {
			code = 404
		}
		fail(w, code, "%v", err)
		return
	}
	s.dropRolesCache()
	s.audit(r, "role_update", "", "", "", name+": "+strings.Join(body.Permissions, " "))
	writeJSON(w, 200, map[string]any{"name": name})
}

func (s *Server) deleteRole(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("role")
	if err := s.Store.DeleteRole(name); err != nil {
		code := 400
		if strings.Contains(err.Error(), "not found") {
			code = 404
		}
		fail(w, code, "%v", err)
		return
	}
	s.dropRolesCache()
	s.audit(r, "role_delete", "", "", "", name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) issueToken(w http.ResponseWriter, r *http.Request) {
	tok, err := s.Store.IssueToken(r.PathValue("user"))
	if err != nil {
		fail(w, 404, "%v", err)
		return
	}
	s.audit(r, "user_token_issued", "", "", "", r.PathValue("user"))
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
