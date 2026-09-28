package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"dagsflow-go/bundle"
	"dagsflow-go/dag"
	"dagsflow-go/engine"
	"dagsflow-go/web"

	"github.com/spf13/cobra"
)

var parallelism int

func home() string {
	if h := os.Getenv("DAGSFLOW_HOME"); h != "" {
		return h
	}
	return "."
}

var (
	storeOnce sync.Once
	theStore  *engine.Store
)

// databaseURL: DAGSFLOW_DATABASE_URL, or the file named by
// DAGSFLOW_DATABASE_URL_FILE (Docker/Kubernetes secrets). Empty = JSON files.
func databaseURL() string {
	if v := os.Getenv("DAGSFLOW_DATABASE_URL"); v != "" {
		return v
	}
	if f := os.Getenv("DAGSFLOW_DATABASE_URL_FILE"); f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			exitf("DAGSFLOW_DATABASE_URL_FILE: %v", err)
		}
		return strings.TrimSpace(string(b))
	}
	return ""
}

// newStore returns the process-wide metadata store (one connection pool).
func newStore() *engine.Store {
	storeOnce.Do(func() {
		st, err := engine.OpenStore(home(), databaseURL())
		if err != nil {
			exitf("metadata database: %v", err)
		}
		theStore = st
	})
	return theStore
}

// loadConnections is kept as a no-op hook: connections/variables are resolved
// lazily through the secrets backends configured in setup().
func loadConnections() {}

func dagsFolder() string {
	if d := os.Getenv("DAGSFLOW_DAGS_FOLDER"); d != "" {
		return d
	}
	return filepath.Join(home(), "dags")
}

var goLoader = &bundle.Loader{Log: func(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "[dags] "+f+"\n", a...)
}}

var (
	reparseMu   sync.Mutex
	reparseSeen map[string]time.Time
)

// applyReparseRequests invalidates sources that someone asked to reparse
// (UI/API/CLI, possibly from another process) since we last looked.
func applyReparseRequests() {
	reqs := newStore().ReparseRequests()
	reparseMu.Lock()
	defer reparseMu.Unlock()
	first := reparseSeen == nil
	if first {
		reparseSeen = map[string]time.Time{}
	}
	for src, t := range reqs {
		if !t.After(reparseSeen[src]) {
			continue
		}
		reparseSeen[src] = t
		if first { // everything gets parsed on startup anyway
			continue
		}
		fmt.Fprintf(os.Stderr, "[dags] reparse requested for %s\n", src)
		if ext := strings.ToLower(filepath.Ext(src)); ext == ".yaml" || ext == ".yml" {
			dag.InvalidateYAML(src)
		} else {
			goLoader.Invalidate(src)
		}
	}
}

// loadDags (re)loads the DAG folder: YAML DAGs and Go DAG files (compiled on
// the fly into bundles). Safe to call repeatedly; unchanged files are skipped.
func loadDags() {
	applyReparseRequests()
	dag.LoadDagFolder(dagsFolder())
	goLoader.Home = home()
	goLoader.LoadFolder(dagsFolder())
}

// reparseDag is used by the webserver: record the request for the other
// processes and reload here right away.
func reparseDag(d *dag.DAG) error {
	if !d.IsFileSource() {
		return fmt.Errorf("%s is compiled into the dagsflow-go binary: rebuild and restart to change it (move it to the DAG folder to reparse without rebuilding)", d.ID)
	}
	if err := newStore().RequestReparse(d.Source); err != nil {
		return err
	}
	loadDags()
	return nil
}

// setup wires the secrets backends (env → metastore → connections/*.json)
// and loads the DAG folder (skipped for commands that don't need DAGs).
func setup(cmd *cobra.Command) {
	st := newStore()
	dag.SetSecretsBackends(dag.EnvBackend{}, engine.MetastoreBackend{Store: st},
		dag.FileBackend{Dir: filepath.Join(home(), "connections")})
	for c := cmd; c != nil; c = c.Parent() {
		switch c.Name() {
		case "variables", "connections", "pools", "users", "roles", "config", "stop", "status", "help", "completion", "db":
			return
		}
	}
	loadDags()
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func mustDAG(id string) *dag.DAG {
	d, ok := dag.Get(id)
	if !ok {
		if err, bad := dag.ImportErrors()[id]; bad {
			exitf("dag %s has an import error: %v", id, err)
		}
		exitf("dag %s not found", id)
	}
	return d
}

func exitf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

func table() *tabwriter.Writer { return tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0) }

func fmtTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func parseConf(s string) map[string]any {
	if s == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		exitf("invalid --conf JSON: %v", err)
	}
	return m
}

var rootCmd = &cobra.Command{
	Use:              "dagsflow-go",
	PersistentPreRun: func(c *cobra.Command, _ []string) { setup(c) },
	Short:            "Airflow-like workflow orchestrator in Go",
	Long:             "State lives in $DAGSFLOW_HOME (default: current dir): state/ = metadata, logs/ = task logs, connections/ = connection files.",
}

// ---- scheduler ---------------------------------------------------------------

var schedulerCmd = &cobra.Command{
	Use:   "scheduler",
	Short: "Run the scheduler for all DAGs (foreground; -D to daemonize)",
	Run: func(cmd *cobra.Command, args []string) {
		st := newStore()
		if _, alive := engine.SchedulerInfo(st); alive && !st.Distributed() {
			exitf("a scheduler is already running (see `scheduler status`); several schedulers need the Postgres metadata database")
		}
		if daemon, _ := cmd.Flags().GetBool("daemon"); daemon {
			_ = os.MkdirAll(st.LogsDir(), 0o755)
			lf, err := os.OpenFile(filepath.Join(st.LogsDir(), "scheduler.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				exitf("%v", err)
			}
			pid, err := spawnDetached(lf, "scheduler", "--parallelism", fmt.Sprint(parallelism), "--dag-dir-list-interval", reloadEvery.String(),
				"--executor", executorName, "--retention-days", fmt.Sprint(retentionDays), "--metrics-addr", metricsAddr)
			if err != nil {
				exitf("start scheduler: %v", err)
			}
			fmt.Printf("scheduler started in background (pid %d), log: %s\n", pid, lf.Name())
			return
		}
		loadConnections()
		ctx, cancel := signalContext()
		defer cancel()
		reloadLoop(ctx)
		s := newScheduler(st)
		serveMetrics(ctx, metricsAddr, st)
		if err := s.Run(ctx); err != nil {
			exitf("%v", err)
		}
	},
}

var schedulerStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the background scheduler (running runs resume on next start)",
	Run: func(cmd *cobra.Command, args []string) {
		st := newStore()
		pid, alive := engine.LocalSchedulerInfo(st)
		if !alive {
			exitf("no scheduler running on this machine")
		}
		if err := terminate(st, pid); err != nil {
			exitf("stop pid %d: %v", pid, err)
		}
		fmt.Printf("scheduler (pid %d) is stopping; running runs resume on the next start\n", pid)
	},
}

var schedulerStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show scheduler status",
	Run: func(cmd *cobra.Command, args []string) {
		st := newStore()
		if !st.Distributed() {
			if pid, alive := engine.SchedulerInfo(st); alive {
				fmt.Printf("scheduler is running (pid %d)\n", pid)
			} else {
				fmt.Println("scheduler is not running")
			}
			return
		}
		printComponents(st)
	},
}

var reloadEvery time.Duration

var (
	executorName  string
	retentionDays int
	metricsAddr   string
	tlsCert       string
	tlsKey        string
)

func newScheduler(st *engine.Store) *engine.Scheduler {
	e := engine.NewEngine(st, parallelism)
	ex, err := engine.NewExecutor(executorName, st)
	if err != nil {
		exitf("%v", err)
	}
	e.Executor = ex
	s := engine.NewScheduler(e)
	s.RetentionDays = retentionDays
	s.Log = newLogger("scheduler")
	if !st.Distributed() && os.Getenv("DAGSFLOW_FERNET_KEY") == "" {
		s.Log.Printf("WARNING: DAGSFLOW_FERNET_KEY is not set: variables and connections are stored unencrypted")
	}
	return s
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscan(v, &n); err == nil {
			return n
		}
	}
	return def
}

// reloadLoop rescans the DAG folder in the background (compiling Go DAGs can
// take a while, so this never blocks the scheduler loop). Reparse requests
// are picked up within ~2s.
func reloadLoop(ctx context.Context) {
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		st := newStore()
		last, stamp := time.Now(), st.ReparseRequestsStamp()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				ns := st.ReparseRequestsStamp()
				if time.Since(last) >= reloadEvery || !ns.Equal(stamp) {
					stamp, last = ns, time.Now()
					loadDags()
				}
			}
		}
	}()
}

// ---- webserver / standalone -----------------------------------------------------

var webAddr string

func newWebServer(st *engine.Store) *web.Server {
	users, _ := st.Users()
	if len(users) == 0 && !isLoopback(webAddr) {
		exitf("refusing to listen on %s without users; create one: dagsflow-go users create --username admin --role Admin", webAddr)
	}
	if len(users) == 0 {
		fmt.Println("[webserver] no users defined: auth disabled (localhost only). Create one with `users create` to enable RBAC.")
	}
	st.Register("webserver", map[string]any{"addr": webAddr})
	return &web.Server{Store: st, Reparse: reparseDag, TLSCert: tlsCert, TLSKey: tlsKey}
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && ip.IsLoopback())
}

var webserverCmd = &cobra.Command{
	Use:   "webserver",
	Short: "Serve the web UI (like `airflow webserver`); needs a running scheduler to execute runs",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := signalContext()
		defer cancel()
		reloadLoop(ctx)
		if err := newWebServer(newStore()).Serve(ctx, webAddr); err != nil {
			exitf("%v", err)
		}
	},
}

var standaloneCmd = &cobra.Command{
	Use:   "standalone",
	Short: "Run scheduler + web UI in one process (like `airflow standalone`)",
	Run: func(cmd *cobra.Command, args []string) {
		st := newStore()
		if _, alive := engine.SchedulerInfo(st); alive && !st.Distributed() {
			exitf("a scheduler is already running; use `webserver` instead")
		}
		loadConnections()
		ctx, cancel := signalContext()
		defer cancel()
		errc := make(chan error, 2)
		reloadLoop(ctx)
		go func() { errc <- newScheduler(st).Run(ctx) }()
		go func() { errc <- newWebServer(st).Serve(ctx, webAddr) }()
		if err := <-errc; err != nil {
			cancel()
			exitf("%v", err)
		}
		<-errc
	},
}

func Execute() {
	webserverCmd.Flags().StringVar(&webAddr, "addr", "127.0.0.1:8080", "listen address")
	standaloneCmd.Flags().StringVar(&webAddr, "addr", "127.0.0.1:8080", "listen address")
	for _, c := range []*cobra.Command{webserverCmd, standaloneCmd} {
		c.Flags().StringVar(&tlsCert, "tls-cert", os.Getenv("DAGSFLOW_TLS_CERT"), "TLS certificate file (serve HTTPS)")
		c.Flags().StringVar(&tlsKey, "tls-key", os.Getenv("DAGSFLOW_TLS_KEY"), "TLS private key file")
	}
	for _, c := range []*cobra.Command{schedulerCmd, standaloneCmd} {
		c.Flags().StringVar(&executorName, "executor", envDefault("DAGSFLOW_EXECUTOR", "local"), "local | worker | kubernetes (worker/kubernetes need Postgres)")
		c.Flags().IntVar(&retentionDays, "retention-days", envInt("DAGSFLOW_RETENTION_DAYS", 0), "delete finished runs, logs and event logs older than N days (0 = keep)")
	}
	schedulerCmd.Flags().StringVar(&metricsAddr, "metrics-addr", os.Getenv("DAGSFLOW_METRICS_ADDR"), "serve /metrics and /health on this address (e.g. :9102)")
	standaloneCmd.Flags().IntVar(&parallelism, "parallelism", 16, "max concurrent task instances")
	webserverCmd.Flags().DurationVar(&reloadEvery, "dag-dir-list-interval", 30*time.Second, "how often to rescan the DAG folder")
	for _, c := range []*cobra.Command{schedulerCmd, standaloneCmd} {
		c.Flags().DurationVar(&reloadEvery, "dag-dir-list-interval", 30*time.Second, "how often to rescan the DAG folder")
	}
	rootCmd.AddCommand(webserverCmd, standaloneCmd)
	rootCmd.AddCommand(adminCmds()...)

	schedulerCmd.Flags().BoolP("daemon", "D", false, "run in background")
	schedulerCmd.PersistentFlags().IntVar(&parallelism, "parallelism", 16, "max concurrent task instances")
	schedulerCmd.AddCommand(schedulerStopCmd, schedulerStatusCmd)

	rootCmd.AddCommand(schedulerCmd, dagsCmd(), tasksCmd(), workerCmd(), dbCmd())
	err := rootCmd.Execute()
	if theStore != nil {
		theStore.Close()
	}
	if err != nil {
		os.Exit(1)
	}
}
