package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"dagsflow-go/dag"
	"dagsflow-go/engine"

	"github.com/spf13/cobra"
)

// ---- worker ----------------------------------------------------------------------

func workerCmd() *cobra.Command {
	var queues []string
	var concurrency int
	var jobID int64
	var grace time.Duration
	c := &cobra.Command{
		Use:   "worker",
		Short: "Run task instances from the task queue (executor=worker), or one job (--job-id, kubernetes pods)",
		Long: `A worker (~ airflow celery worker) executes task instances that a scheduler
started with --executor worker (or kubernetes). It needs the Postgres metadata
database (DAGSFLOW_DATABASE_URL) and the same DAG folder and connections as the
scheduler. Task logs, XComs and results go through the database.`,
		Run: func(cmd *cobra.Command, args []string) {
			st := newStore()
			if !st.Distributed() {
				exitf("worker: %v", engine.ErrNeedsDatabase)
			}
			ctx, cancel := signalContext()
			defer cancel()
			w := &engine.Worker{Store: st, Queues: queues, Concurrency: concurrency, Grace: grace, Log: newLogger("worker")}
			if jobID > 0 {
				if err := w.RunJob(ctx, jobID); err != nil {
					exitf("%v", err)
				}
				return
			}
			reloadLoop(ctx)
			serveMetrics(ctx, metricsAddr, nil)
			if err := w.Run(ctx); err != nil {
				exitf("%v", err)
			}
		},
	}
	c.Flags().StringSliceVarP(&queues, "queues", "q", strings.Split(envDefault("DAGSFLOW_WORKER_QUEUES", dag.DefaultQueue), ","), "queues to consume")
	c.Flags().IntVarP(&concurrency, "concurrency", "c", envInt("DAGSFLOW_WORKER_CONCURRENCY", 8), "max task instances at once")
	c.Flags().Int64Var(&jobID, "job-id", 0, "run exactly this job and exit (used by the kubernetes executor)")
	c.Flags().DurationVar(&grace, "grace", 10*time.Minute, "on shutdown, let running tasks finish for this long before cancelling them")
	c.Flags().StringVar(&metricsAddr, "metrics-addr", os.Getenv("DAGSFLOW_METRICS_ADDR"), "serve /metrics and /health on this address (e.g. :9103)")
	c.Flags().DurationVar(&reloadEvery, "dag-dir-list-interval", 30*time.Second, "how often to rescan the DAG folder")
	return c
}

// ---- db ------------------------------------------------------------------------------

func dbCmd() *cobra.Command {
	c := &cobra.Command{Use: "db", Short: "Metadata database: check, migrate, clean, backup/restore"}

	check := &cobra.Command{
		Use: "check", Short: "Check the connection and schema; list live components",
		Run: func(cmd *cobra.Command, args []string) {
			st := newStore()
			if err := st.Ping(); err != nil {
				exitf("database unreachable: %v", err)
			}
			fmt.Printf("backend: %s\n", st.Backend())
			if applied, want, err := st.SchemaStatus(); err != nil {
				exitf("schema: %v", err)
			} else if st.Distributed() {
				fmt.Printf("schema: version %d (binary expects %d)\n", applied, want)
			}
			printComponents(st)
		},
	}

	migrate := &cobra.Command{
		Use: "migrate", Aliases: []string{"init", "upgrade"}, Short: "Create or upgrade the schema (also done automatically on start)",
		Run: func(cmd *cobra.Command, args []string) {
			st := newStore() // opening applies pending migrations
			if applied, _, err := st.SchemaStatus(); err != nil {
				exitf("%v", err)
			} else if st.Distributed() {
				fmt.Printf("schema is at version %d\n", applied)
			} else {
				fmt.Println("JSON store: nothing to migrate")
			}
		},
	}

	var olderThan string
	var yes bool
	clean := &cobra.Command{
		Use: "clean", Short: "Delete finished runs, task logs, event logs, jobs and catalog tables dropped longer than --older-than ago (~ airflow db clean); catalog lineage not seen within DAGSFLOW_CATALOG_EDGE_TTL_DAYS (default 90)",
		Run: func(cmd *cobra.Command, args []string) {
			d, err := parseAge(olderThan)
			if err != nil {
				exitf("--older-than: %v", err)
			}
			before := time.Now().Add(-d)
			if !yes {
				fmt.Printf("This deletes finished runs and logs before %s. Re-run with --yes to proceed.\n", before.Format(time.DateTime))
				return
			}
			st := newStore()
			res, err := st.Cleanup(before)
			if err != nil {
				exitf("%v", err)
			}
			edges, tables, err := st.CleanCatalog(engine.CatalogEdgeTTL(), d)
			if err != nil {
				exitf("catalog: %v", err)
			}
			out := map[string]any{}
			raw, _ := json.Marshal(res)
			_ = json.Unmarshal(raw, &out) // same fields as before, plus the catalog
			out["catalog_edges"], out["catalog_tables"] = edges, tables
			b, _ := json.Marshal(out)
			fmt.Println(string(b))
		},
	}
	clean.Flags().StringVar(&olderThan, "older-than", "90d", "age, e.g. 30d, 720h")
	clean.Flags().BoolVar(&yes, "yes", false, "really delete")

	var file string
	var auditLimit int
	export := &cobra.Command{
		Use: "export", Short: "Dump metadata to JSON (backup, or moving from JSON files to Postgres)",
		Run: func(cmd *cobra.Command, args []string) {
			b, err := newStore().Export(auditLimit)
			if err != nil {
				exitf("%v", err)
			}
			var out io.Writer = os.Stdout
			if file != "" && file != "-" {
				f, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
				if err != nil {
					exitf("%v", err)
				}
				defer f.Close()
				out = f
			}
			enc := json.NewEncoder(out)
			if err := enc.Encode(b); err != nil {
				exitf("%v", err)
			}
			if file != "" && file != "-" {
				fmt.Fprintf(os.Stderr, "exported %d runs, %d dags, %d tables, %d events to %s\n", len(b.Runs), len(b.DagMeta), len(b.Tables), len(b.Audit), file)
			}
		},
	}
	export.Flags().StringVarP(&file, "file", "f", "-", "output file (- = stdout)")
	export.Flags().IntVar(&auditLimit, "event-logs", 100000, "max event log entries to include")

	imp := &cobra.Command{
		Use: "import", Short: "Restore a `db export` dump into the current store",
		Run: func(cmd *cobra.Command, args []string) {
			var in io.Reader = os.Stdin
			if file != "" && file != "-" {
				f, err := os.Open(file)
				if err != nil {
					exitf("%v", err)
				}
				defer f.Close()
				in = f
			}
			var b engine.Backup
			if err := json.NewDecoder(in).Decode(&b); err != nil {
				exitf("read backup: %v", err)
			}
			res, err := newStore().Import(&b)
			if err != nil {
				exitf("%v", err)
			}
			fmt.Printf("imported into %s: %+v\n", newStore().Backend(), res)
		},
	}
	imp.Flags().StringVarP(&file, "file", "f", "-", "input file (- = stdin)")

	compact := &cobra.Command{
		Use: "compact-xcom", Short: "Move large XCom values out of old run rows into xcom_blob (Postgres; speeds up run lists)",
		Run: func(cmd *cobra.Command, args []string) {
			n, err := newStore().CompactXCom(func(f string, a ...any) { fmt.Printf(f+"\n", a...) })
			if err != nil {
				exitf("%v", err)
			}
			fmt.Printf("%d runs compacted\n", n)
		},
	}
	c.AddCommand(check, migrate, clean, export, imp, compact)
	return c
}

func parseAge(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		var n int
		if _, err := fmt.Sscan(strings.TrimSuffix(s, "d"), &n); err != nil || n < 0 {
			return 0, fmt.Errorf("invalid age %q", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err == nil && d < 0 {
		err = errors.New("negative age")
	}
	return d, err
}

func printComponents(st *engine.Store) {
	cs, err := st.Components()
	if err != nil {
		exitf("%v", err)
	}
	if len(cs) == 0 {
		fmt.Println("no registered components")
		return
	}
	w := table()
	fmt.Fprintln(w, "KIND\tID\tHOST\tPID\tSTARTED\tHEARTBEAT\tSTATUS")
	for _, c := range cs {
		status := "alive"
		if !c.Alive() {
			status = "dead"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s ago\t%s\n", c.Kind, c.ID, c.Host, c.PID, fmtTime(&c.Started),
			time.Since(c.Heartbeat).Round(time.Second), status)
	}
	w.Flush()
}

// ---- metrics / health endpoint for scheduler and worker -------------------------------

// serveMetrics exposes /metrics (Prometheus) and /health on addr ("" = off).
// st != nil adds store-wide gauges (the webserver's /metrics has them too).
func serveMetrics(ctx context.Context, addr string, st *engine.Store) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		engine.WriteMetrics(w, st)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := newStore().Ping(); err != nil {
			http.Error(w, "metadata database: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "metrics server %s: %v\n", addr, err)
		}
	}()
}

// ---- logging ------------------------------------------------------------------------------

// newLogger: plain text by default; DAGSFLOW_LOG_FORMAT=json writes one JSON
// object per line (for log collectors: Cloud Logging, Loki, ELK).
func newLogger(component string) *log.Logger {
	if strings.EqualFold(os.Getenv("DAGSFLOW_LOG_FORMAT"), "json") {
		return log.New(&jsonLogWriter{component: component, out: os.Stdout}, "", 0)
	}
	return log.New(os.Stdout, "["+component+"] ", log.LstdFlags)
}

type jsonLogWriter struct {
	mu        sync.Mutex
	component string
	out       io.Writer
}

func (j *jsonLogWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	level := "info"
	low := strings.ToLower(msg)
	switch {
	case strings.Contains(low, "error") || strings.Contains(low, "failed") || strings.Contains(low, "panic"):
		level = "error"
	case strings.Contains(low, "warn"):
		level = "warning"
	}
	b, _ := json.Marshal(map[string]any{"time": time.Now().UTC().Format(time.RFC3339Nano), "severity": level,
		"component": j.component, "message": msg, "pid": os.Getpid()})
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.out.Write(append(b, '\n')); err != nil {
		return 0, err
	}
	return len(p), nil
}
