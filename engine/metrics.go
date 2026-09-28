package engine

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"dagsflow-go/dag"
)

// A tiny Prometheus registry (text exposition format 0.0.4), so there is no
// extra dependency. Process-local metrics are counted where things happen;
// store-wide gauges are computed at scrape time (see StoreMetrics).

type metric interface{ write(w io.Writer) }

var (
	regMu    sync.Mutex
	registry []metric
)

func register[M metric](m M) M {
	regMu.Lock()
	defer regMu.Unlock()
	registry = append(registry, m)
	return m
}

type vec struct {
	name, help, typ string
	labels          []string
	mu              sync.Mutex
	vals            map[string]float64
}

func newVec(typ, name, help string, labels ...string) *vec {
	return register(&vec{name: name, help: help, typ: typ, labels: labels, vals: map[string]float64{}})
}

func (v *vec) key(lv []string) string {
	if len(lv) != len(v.labels) {
		panic(fmt.Sprintf("metric %s: want %d labels", v.name, len(v.labels)))
	}
	return strings.Join(lv, "\x00")
}

func (v *vec) add(d float64, lv ...string) {
	v.mu.Lock()
	v.vals[v.key(lv)] += d
	v.mu.Unlock()
}
func (v *vec) inc(lv ...string) { v.add(1, lv...) }
func (v *vec) set(x float64, lv ...string) {
	v.mu.Lock()
	v.vals[v.key(lv)] = x
	v.mu.Unlock()
}

func escLabel(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

func labelStr(names []string, vals []string, extra ...string) string {
	var parts []string
	for i, n := range names {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, n, escLabel(vals[i])))
	}
	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, extra[i], escLabel(extra[i+1])))
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func fmtFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "+Inf"
	case f == math.Trunc(f) && math.Abs(f) < 1e15:
		return fmt.Sprintf("%d", int64(f))
	}
	return fmt.Sprintf("%g", f)
}

func (v *vec) write(w io.Writer) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", v.name, v.help, v.name, v.typ)
	keys := make([]string, 0, len(v.vals))
	for k := range v.vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		var lv []string
		if len(v.labels) > 0 {
			lv = strings.Split(k, "\x00")
		}
		fmt.Fprintf(w, "%s%s %s\n", v.name, labelStr(v.labels, lv), fmtFloat(v.vals[k]))
	}
}

type histogram struct {
	name, help string
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*hseries
}

type hseries struct {
	counts []uint64
	sum    float64
	n      uint64
}

func newHistogram(name, help string, buckets []float64, labels ...string) *histogram {
	return register(&histogram{name: name, help: help, labels: labels, buckets: buckets, series: map[string]*hseries{}})
}

func (h *histogram) observe(x float64, lv ...string) {
	k := strings.Join(lv, "\x00")
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.series[k]
	if s == nil {
		s = &hseries{counts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	for i, b := range h.buckets {
		if x <= b {
			s.counts[i]++
		}
	}
	s.sum += x
	s.n++
}

func (h *histogram) write(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	keys := make([]string, 0, len(h.series))
	for k := range h.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := h.series[k]
		var lv []string
		if len(h.labels) > 0 {
			lv = strings.Split(k, "\x00")
		}
		for i, b := range h.buckets {
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelStr(h.labels, lv, "le", fmtFloat(b)), s.counts[i])
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelStr(h.labels, lv, "le", "+Inf"), s.n)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, labelStr(h.labels, lv), fmtFloat(s.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, labelStr(h.labels, lv), s.n)
	}
}

var durationBuckets = []float64{1, 5, 15, 30, 60, 300, 900, 1800, 3600, 7200, 21600}

var (
	processStart = time.Now()

	metricErrors      = newVec("counter", "dagsflow_errors_total", "Internal errors by operation.", "op")
	metricRunsStarted = newVec("counter", "dagsflow_dagrun_started_total", "DagRuns started (or resumed) by this process.", "dag_id")
	metricRunsDone    = newVec("counter", "dagsflow_dagrun_finished_total", "DagRuns finished by this process.", "dag_id", "state")
	metricRunDuration = newHistogram("dagsflow_dagrun_duration_seconds", "DagRun duration (start to end).", durationBuckets, "dag_id", "state")
	metricTIDone      = newVec("counter", "dagsflow_ti_finished_total", "Task instance attempts finished.", "dag_id", "task_id", "state")
	metricTIDuration  = newHistogram("dagsflow_ti_duration_seconds", "Task instance attempt duration.", durationBuckets, "dag_id", "state")
	metricTIRunning   = newVec("gauge", "dagsflow_executor_running_tasks", "Task instances currently executing (or dispatched) from this process.")
	metricTIWaiting   = newVec("gauge", "dagsflow_executor_queued_tasks", "Task instances waiting for an executor or pool slot in this process.")
	metricLoop        = newHistogram("dagsflow_scheduler_loop_duration_seconds", "Duration of one scheduler loop.", []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30})
	metricLeader      = newVec("gauge", "dagsflow_scheduler_leader", "1 when this scheduler is the leader (creates runs, adopts orphans).")
	metricOrphans     = newVec("counter", "dagsflow_dagrun_orphans_adopted_total", "Running DagRuns re-queued after their owner stopped.")
	metricJobs        = newVec("counter", "dagsflow_worker_jobs_total", "Jobs executed by this worker.", "queue", "state")
	metricWorkerBusy  = newVec("gauge", "dagsflow_worker_running_jobs", "Jobs currently running in this worker.")
)

// WriteMetrics writes this process's metrics, plus the store-wide gauges
// when st is not nil, in the Prometheus text format.
func WriteMetrics(w io.Writer, st *Store) {
	fmt.Fprintf(w, "# HELP dagsflow_process_start_time_seconds Start time of the process.\n# TYPE dagsflow_process_start_time_seconds gauge\n")
	fmt.Fprintf(w, "dagsflow_process_start_time_seconds %d\n", processStart.Unix())
	fmt.Fprintf(w, "# HELP dagsflow_up 1 when the process is up.\n# TYPE dagsflow_up gauge\ndagsflow_up{pid=\"%d\"} 1\n", os.Getpid())
	regMu.Lock()
	ms := append([]metric(nil), registry...)
	regMu.Unlock()
	for _, m := range ms {
		m.write(w)
	}
	if st != nil {
		StoreMetrics(w, st)
	}
}

// StoreMetrics: gauges read from the metadata store (active runs, pools,
// components, task queue, import errors). Meant for the webserver's /metrics.
func StoreMetrics(w io.Writer, st *Store) {
	g := func(name, help string) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
	}
	g("dagsflow_dags", "DAGs loaded, by paused state.")
	paused := 0
	dags := dag.List()
	for _, d := range dags {
		if st.GetDagMeta(d.ID).IsPaused {
			paused++
		}
	}
	fmt.Fprintf(w, "dagsflow_dags{paused=\"false\"} %d\ndagsflow_dags{paused=\"true\"} %d\n", len(dags)-paused, paused)
	g("dagsflow_dag_import_errors", "DAG files that failed to load.")
	fmt.Fprintf(w, "dagsflow_dag_import_errors %d\n", len(dag.ImportErrors()))

	g("dagsflow_dagruns_active", "Queued/running DagRuns per DAG.")
	for _, d := range dags {
		runs, err := st.ListRunsByState(d.ID, RunQueued, RunRunning)
		if err != nil {
			continue
		}
		var q, r int
		for _, x := range runs {
			if x.State == RunQueued {
				q++
			} else {
				r++
			}
		}
		fmt.Fprintf(w, "dagsflow_dagruns_active{dag_id=\"%s\",state=\"queued\"} %d\n", escLabel(d.ID), q)
		fmt.Fprintf(w, "dagsflow_dagruns_active{dag_id=\"%s\",state=\"running\"} %d\n", escLabel(d.ID), r)
	}

	if pools, err := st.Pools(); err == nil {
		used, _ := st.be().poolUsage()
		g("dagsflow_pool_slots", "Pool size.")
		names := make([]string, 0, len(pools))
		for n := range pools {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Fprintf(w, "dagsflow_pool_slots{pool=\"%s\"} %d\n", escLabel(n), pools[n].Slots)
		}
		if used != nil {
			g("dagsflow_pool_used_slots", "Pool slots in use across all schedulers.")
			for _, n := range names {
				fmt.Fprintf(w, "dagsflow_pool_used_slots{pool=\"%s\"} %d\n", escLabel(n), used[n])
			}
		}
	}

	if cs, err := st.Components(); err == nil {
		g("dagsflow_components_alive", "Live processes by kind.")
		alive := map[string]int{"scheduler": 0, "webserver": 0, "worker": 0}
		for _, c := range cs {
			if c.Alive() {
				alive[c.Kind]++
			}
		}
		kinds := make([]string, 0, len(alive))
		for k := range alive {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		for _, k := range kinds {
			fmt.Fprintf(w, "dagsflow_components_alive{kind=\"%s\"} %d\n", escLabel(k), alive[k])
		}
	}

	if js, err := st.be().jobStats(); err == nil && js != nil {
		g("dagsflow_task_queue_jobs", "Jobs in the task queue by queue and state.")
		keys := make([]string, 0, len(js))
		for k := range js {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			qs := strings.SplitN(k, "\x00", 2)
			fmt.Fprintf(w, "dagsflow_task_queue_jobs{queue=\"%s\",state=\"%s\"} %d\n", escLabel(qs[0]), escLabel(qs[1]), js[k])
		}
	}
}
