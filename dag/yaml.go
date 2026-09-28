package dag

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// YAML DAGs can be added/changed/removed in the DAG folder without rebuilding
// the binary; the scheduler and webserver re-scan the folder periodically.
// They are built from the built-in operators (bash, http, bigquery, ...).

type yamlCommon struct {
	Owner          string   `yaml:"owner"`
	Retries        *int     `yaml:"retries"`
	RetryDelay     string   `yaml:"retry_delay"`
	Timeout        string   `yaml:"timeout"`
	Pool           string   `yaml:"pool"`
	PoolSlots      int      `yaml:"pool_slots"`
	PriorityWeight int      `yaml:"priority_weight"`
	SLA            string   `yaml:"sla"`
	OnFailure      []string `yaml:"on_failure"`
	OnSuccess      []string `yaml:"on_success"`
	OnRetry        []string `yaml:"on_retry"`
}

type yamlExpand struct {
	Items []any `yaml:"items"`
	XCom  *struct {
		Task string `yaml:"task"`
		Key  string `yaml:"key"`
	} `yaml:"xcom"`
}

type yamlTask struct {
	yamlCommon  `yaml:",inline"`
	ID          string      `yaml:"id"`
	Operator    string      `yaml:"operator"`
	Upstream    []string    `yaml:"upstream"`
	TriggerRule string      `yaml:"trigger_rule"`
	Outlets     []string    `yaml:"outlets"`
	Expand      *yamlExpand `yaml:"expand"`

	Command string            `yaml:"command"` // bash
	Env     map[string]string `yaml:"env"`

	SQL     string         `yaml:"sql"` // bigquery
	SQLFile string         `yaml:"sql_file"`
	Params  map[string]any `yaml:"params"`

	Method       string            `yaml:"method"` // http / http_sensor
	URL          string            `yaml:"url"`
	Endpoint     string            `yaml:"endpoint"`
	ConnID       string            `yaml:"conn_id"`
	Headers      map[string]string `yaml:"headers"`
	Body         string            `yaml:"body"`
	ExpectStatus []int             `yaml:"expect_status"`

	TriggerDagID string         `yaml:"trigger_dag_id"` // trigger_dagrun
	Conf         map[string]any `yaml:"conf"`
	Wait         bool           `yaml:"wait_for_completion"`

	Path           string `yaml:"path"` // sensors
	Delta          string `yaml:"delta"`
	ExternalDagID  string `yaml:"external_dag_id"`
	ExternalTaskID string `yaml:"external_task_id"`
	PokeInterval   string `yaml:"poke_interval"`
	SensorTimeout  string `yaml:"sensor_timeout"`
	Mode           string `yaml:"mode"`
	SoftFail       bool   `yaml:"soft_fail"`
}

type yamlDAG struct {
	DagID         string         `yaml:"dag_id"`
	Schedule      string         `yaml:"schedule"`
	Datasets      []string       `yaml:"schedule_datasets"`
	StartDate     string         `yaml:"start_date"`
	Catchup       bool           `yaml:"catchup"`
	MaxActiveRuns int            `yaml:"max_active_runs"`
	Description   string         `yaml:"description"`
	Tags          []string       `yaml:"tags"`
	Params        map[string]any `yaml:"params"`
	DefaultArgs   yamlCommon     `yaml:"default_args"`
	OnSuccess     []string       `yaml:"on_success"`
	OnFailure     []string       `yaml:"on_failure"`
	OnSLAMiss     []string       `yaml:"on_sla_miss"`
	Tasks         []yamlTask     `yaml:"tasks"`
}

func dur(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

// notifiers turns connection ids into callbacks (connection type google_chat).
func notifiers(ids []string) []Callback {
	var out []Callback
	for _, id := range ids {
		out = append(out, GoogleChatNotifier(id))
	}
	return out
}

func (c yamlCommon) options() ([]TaskOption, error) {
	var opts []TaskOption
	rd, err := dur(c.RetryDelay)
	if err != nil {
		return nil, fmt.Errorf("retry_delay: %w", err)
	}
	if c.Retries != nil {
		opts = append(opts, WithRetries(*c.Retries, rd))
	} else if rd > 0 {
		opts = append(opts, func(t *Task) { t.retryDelay = rd })
	}
	if to, err := dur(c.Timeout); err != nil {
		return nil, fmt.Errorf("timeout: %w", err)
	} else if to > 0 {
		opts = append(opts, WithTimeout(to))
	}
	if sla, err := dur(c.SLA); err != nil {
		return nil, fmt.Errorf("sla: %w", err)
	} else if sla > 0 {
		opts = append(opts, WithSLA(sla))
	}
	if c.Pool != "" {
		opts = append(opts, WithPool(c.Pool))
	}
	if c.PoolSlots > 0 {
		opts = append(opts, WithPoolSlots(c.PoolSlots))
	}
	if c.PriorityWeight != 0 {
		opts = append(opts, WithPriorityWeight(c.PriorityWeight))
	}
	opts = append(opts, WithOnFailureCallback(notifiers(c.OnFailure)...),
		WithOnSuccessCallback(notifiers(c.OnSuccess)...), WithOnRetryCallback(notifiers(c.OnRetry)...))
	return opts, nil
}

// ParseYAML builds DAGs from one YAML file (multiple documents allowed).
func ParseYAML(path string, data []byte) ([]*DAG, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var out []*DAG
	for {
		var y yamlDAG
		if err := dec.Decode(&y); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		d, err := y.build(filepath.Dir(path))
		if err != nil {
			return nil, fmt.Errorf("dag %q: %w", y.DagID, err)
		}
		out = append(out, d)
	}
	return out, nil
}

func (y yamlDAG) build(baseDir string) (*DAG, error) {
	if y.DagID == "" {
		return nil, errors.New("dag_id is required")
	}
	opts := []DAGOption{WithCatchup(y.Catchup), WithMaxActiveRuns(y.MaxActiveRuns),
		WithDescription(y.Description), WithTags(y.Tags...), WithParams(y.Params),
		WithOnSuccess(notifiers(y.OnSuccess)...), WithOnFailure(notifiers(y.OnFailure)...),
		WithSLAMiss(notifiers(y.OnSLAMiss)...)}
	if len(y.Datasets) > 0 {
		opts = append(opts, WithDatasetSchedule(y.Datasets...))
	}
	if y.StartDate != "" {
		t, err := time.ParseInLocation("2006-01-02", y.StartDate, time.Local)
		if err != nil {
			return nil, fmt.Errorf("start_date must be YYYY-MM-DD")
		}
		opts = append(opts, WithStartDate(t))
	}
	owner := y.DefaultArgs.Owner
	opts = append(opts, func(d *DAG) { d.DefaultArgs.Owner = owner })
	d := NewDAG(y.DagID, y.Schedule, opts...)
	defaults, err := y.DefaultArgs.options()
	if err != nil {
		return nil, fmt.Errorf("default_args: %w", err)
	}

	for _, yt := range y.Tasks {
		if yt.ID == "" {
			return nil, errors.New("every task needs an id")
		}
		if _, dup := d.taskMap[yt.ID]; dup {
			return nil, fmt.Errorf("duplicate task id %q", yt.ID)
		}
		own, err := yt.options()
		if err != nil {
			return nil, fmt.Errorf("task %s: %w", yt.ID, err)
		}
		topts := append(append([]TaskOption{}, defaults...), own...)
		if yt.TriggerRule != "" {
			topts = append(topts, WithTriggerRule(TriggerRule(yt.TriggerRule)))
		}
		if len(yt.Outlets) > 0 {
			topts = append(topts, WithOutlets(yt.Outlets...))
		}
		if yt.Expand != nil {
			if yt.Expand.XCom != nil {
				topts = append(topts, Expand(FromXCom(yt.Expand.XCom.Task, firstNonEmpty(yt.Expand.XCom.Key, "return_value"))))
			} else {
				topts = append(topts, Expand(FromItems(yt.Expand.Items...)))
			}
		}
		if err := yt.add(d, baseDir, topts); err != nil {
			return nil, fmt.Errorf("task %s: %w", yt.ID, err)
		}
	}
	for _, yt := range y.Tasks {
		t := d.taskMap[yt.ID]
		for _, u := range yt.Upstream {
			ut, ok := d.taskMap[u]
			if !ok {
				return nil, fmt.Errorf("task %s: unknown upstream %q", yt.ID, u)
			}
			ut.link(t)
		}
	}
	return d, nil
}

func (yt yamlTask) sensorConfig() (SensorConfig, error) {
	pi, err := dur(yt.PokeInterval)
	if err != nil {
		return SensorConfig{}, fmt.Errorf("poke_interval: %w", err)
	}
	to, err := dur(yt.SensorTimeout)
	if err != nil {
		return SensorConfig{}, fmt.Errorf("sensor_timeout: %w", err)
	}
	return SensorConfig{PokeInterval: pi, Timeout: to, Mode: SensorMode(yt.Mode), SoftFail: yt.SoftFail}, nil
}

func (yt yamlTask) httpReq() HTTPRequest {
	return HTTPRequest{Method: yt.Method, URL: yt.URL, Endpoint: yt.Endpoint, ConnID: yt.ConnID,
		Headers: yt.Headers, Body: yt.Body, ExpectStatus: yt.ExpectStatus}
}

func (yt yamlTask) add(d *DAG, baseDir string, opts []TaskOption) error {
	switch yt.Operator {
	case "", "empty":
		d.NewEmptyTask(yt.ID, opts...)
	case "bash":
		if yt.Command == "" {
			return errors.New("bash needs command")
		}
		d.NewBashTask(yt.ID, yt.Command, yt.Env, opts...)
	case "bigquery":
		file := yt.SQLFile
		if file != "" && !filepath.IsAbs(file) {
			file = filepath.Join(baseDir, file)
		}
		params := map[string]any{}
		for k, v := range yt.Params {
			params[k] = v
		}
		if yt.SQL != "" {
			params["sql"] = yt.SQL
		}
		if file == "" && yt.SQL == "" {
			return errors.New("bigquery needs sql or sql_file")
		}
		d.NewBigQueryTask(yt.ID, file, params, opts...)
	case "http":
		d.NewHTTPTask(yt.ID, yt.httpReq(), opts...)
	case "trigger_dagrun":
		if yt.TriggerDagID == "" {
			return errors.New("trigger_dagrun needs trigger_dag_id")
		}
		d.NewTriggerDagRunTask(yt.ID, yt.TriggerDagID, yt.Conf, yt.Wait, opts...)
	case "file_sensor", "time_delta_sensor", "external_task_sensor", "http_sensor":
		cfg, err := yt.sensorConfig()
		if err != nil {
			return err
		}
		switch yt.Operator {
		case "file_sensor":
			d.NewFileSensor(yt.ID, yt.Path, cfg, opts...)
		case "time_delta_sensor":
			delta, err := dur(yt.Delta)
			if err != nil {
				return fmt.Errorf("delta: %w", err)
			}
			d.NewTimeDeltaSensor(yt.ID, delta, cfg, opts...)
		case "external_task_sensor":
			d.NewExternalTaskSensor(yt.ID, yt.ExternalDagID, yt.ExternalTaskID, cfg, opts...)
		case "http_sensor":
			d.NewHTTPSensor(yt.ID, yt.httpReq(), cfg, opts...)
		}
	default:
		return fmt.Errorf("unknown operator %q (empty, bash, bigquery, http, trigger_dagrun, file_sensor, time_delta_sensor, external_task_sensor, http_sensor)", yt.Operator)
	}
	return nil
}

// ---- folder loader with change detection --------------------------------------

var (
	folderMu    sync.Mutex
	folderStamp = map[string]string{} // path -> modtime|size
)

// LoadDagFolder (re)loads every *.yaml / *.yml under dir. Only changed files are
// re-parsed; deleted files are unregistered. Safe to call repeatedly.
func LoadDagFolder(dir string) {
	folderMu.Lock()
	defer folderMu.Unlock()
	seen := map[string]bool{}
	_ = filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(p))
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}
		seen[p] = true
		info, err := e.Info()
		if err != nil {
			return nil
		}
		stamp := fmt.Sprintf("%d|%d", info.ModTime().UnixNano(), info.Size())
		if folderStamp[p] == stamp {
			return nil
		}
		folderStamp[p] = stamp
		data, err := os.ReadFile(p)
		if err != nil {
			ReplaceSource(p, nil, err)
			return nil
		}
		dags, err := ParseYAML(p, data)
		ReplaceSource(p, dags, err)
		return nil
	})
	for src := range Sources() {
		ext := strings.ToLower(filepath.Ext(src))
		if !seen[src] && (ext == ".yaml" || ext == ".yml") && strings.HasPrefix(src, filepath.Clean(dir)) {
			delete(folderStamp, src)
			ReplaceSource(src, nil, nil)
		}
	}
}
