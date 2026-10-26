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
	Queue          string   `yaml:"queue"`
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
	ID          string         `yaml:"id"`
	Operator    string         `yaml:"operator"`
	Node        string         `yaml:"node"` // operator: custom
	With        map[string]any `yaml:"with"`
	Upstream    []string       `yaml:"upstream"`
	TriggerRule string         `yaml:"trigger_rule"`
	Outlets     []string       `yaml:"outlets"`
	Expand      *yamlExpand    `yaml:"expand"`

	PodOverride map[string]any `yaml:"pod_override"` // kubernetes executor

	Code    string            `yaml:"code"`    // go
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

	Table      string          `yaml:"table"` // postgres (+ sql, conn_id)
	RowsFrom   string          `yaml:"rows_from"`
	Limit      int             `yaml:"limit"`
	OnConflict string          `yaml:"on_conflict"`
	Input      string          `yaml:"input"` // transform
	Steps      []TransformStep `yaml:"steps"`

	ProjectID               string                `yaml:"project_id"` // bigquery_* / gsheet_*
	Location                string                `yaml:"location"`
	Dataset                 string                `yaml:"dataset"`
	SpreadsheetID           string                `yaml:"spreadsheet_id"`
	Range                   string                `yaml:"range"`
	NoHeader                bool                  `yaml:"no_header"`
	Columns                 []string              `yaml:"columns"`
	WriteMode               string                `yaml:"write_mode"`
	UserEntered             bool                  `yaml:"user_entered"`
	KeepHeaders             bool                  `yaml:"keep_headers"` // gsheet_read / file_read
	File                    string                `yaml:"file"`         // file_read
	Format                  string                `yaml:"format"`
	Sheet                   string                `yaml:"sheet"`
	Delimiter               string                `yaml:"delimiter"`
	Inputs                  []string              `yaml:"inputs"` // merge
	On                      []string              `yaml:"on"`
	JoinType                string                `yaml:"join_type"`
	Bucket                  string                `yaml:"bucket"` // gcs_write / s3_write
	Object                  string                `yaml:"object"`
	Key                     string                `yaml:"key"`
	Region                  string                `yaml:"region"`
	Gzip                    bool                  `yaml:"gzip"`
	SourceTable             string                `yaml:"source_table"` // bigquery_load_table (+ table, rows_from)
	MergeKeys               []string              `yaml:"merge_keys"`
	WhenMatched             string                `yaml:"when_matched"`
	Conditions              []LoadCondition       `yaml:"conditions"`
	ConditionMode           string                `yaml:"condition_mode"`
	PartitionBy             string                `yaml:"partition_by"`
	ClusterBy               []string              `yaml:"cluster_by"`
	ColumnMap               map[string]ColumnSpec `yaml:"column_map"`
	Tables                  []string              `yaml:"tables"` // bigquery_delete_table
	FailIfMissing           bool                  `yaml:"fail_if_missing"`
	Expiration              string                `yaml:"expiration"` // bigquery_external_table / bigquery_load_table
	PartitionExpirationDays float64               `yaml:"partition_expiration_days"`
	Workgroup               string                `yaml:"workgroup"` // athena_*
	OutputLocation          string                `yaml:"output_location"`
	Catalog                 string                `yaml:"catalog"`
	Database                string                `yaml:"database"` // athena_* / redshift_*
	TableLocation           string                `yaml:"table_location"`
	Partitioning            []string              `yaml:"partitioning"`
	S3Location              string                `yaml:"s3_location"`
	ClusterIdentifier       string                `yaml:"cluster_identifier"` // redshift_*
	WorkgroupName           string                `yaml:"workgroup_name"`
	DbUser                  string                `yaml:"db_user"`
	SecretArn               string                `yaml:"secret_arn"`
	IAMRole                 string                `yaml:"iam_role"`
	S3Region                string                `yaml:"s3_region"`
	DistKey                 string                `yaml:"dist_key"`
	SortKey                 []string              `yaml:"sort_key"`
	CastMode                string                `yaml:"cast_mode"`
	Regions                 []string              `yaml:"regions"` // bigquery_catalog_sync / redshift_catalog_sync
	Datasets                []string              `yaml:"datasets"`
	Schemas                 []string              `yaml:"schemas"`
	LookbackHours           int                   `yaml:"lookback_hours"`
	MaxQueries              int                   `yaml:"max_queries"`
	NoQueries               bool                  `yaml:"no_queries"`
	Metadata                []string              `yaml:"metadata"`
	MetadataPrefix          string                `yaml:"metadata_prefix"`
	MetadataNames           map[string]string     `yaml:"metadata_names"`
	InsertedAt              string                `yaml:"inserted_at"`
	JobDate                 string                `yaml:"job_date"`
	Timezone                string                `yaml:"timezone"`
	Rename                  map[string]string     `yaml:"rename"`    // gsheet_read / file_read: column -> new name
	AllFiles                bool                  `yaml:"all_files"` // bigquery_external_table (+ table, rows_from, format, columns)
	DefaultType             string                `yaml:"default_type"`
	ColumnTypes             map[string]string     `yaml:"column_types"`
	IfNotExists             bool                  `yaml:"if_not_exists"`
	URIs                    []string              `yaml:"uris"`

	TriggerDagID string         `yaml:"trigger_dag_id"` // trigger_dagrun
	Conf         map[string]any `yaml:"conf"`
	Wait         bool           `yaml:"wait_for_completion"`

	Left  string   `yaml:"left"` // branch (If node), + input, path, value
	Op    string   `yaml:"op"`
	Value any      `yaml:"value"`
	Then  []string `yaml:"then"`
	Else  []string `yaml:"else"`

	MaxBytes int64  `yaml:"max_bytes"` // http
	SaveTo   string `yaml:"save_to"`

	Path           string `yaml:"path"` // sensors, branch
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

	// Layout is written by the visual builder (node positions); ignored here.
	Layout map[string]any `yaml:"layout"`
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
	if c.Queue != "" {
		opts = append(opts, WithQueue(c.Queue))
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
		if yt.PodOverride != nil {
			topts = append(topts, WithPodOverride(yt.PodOverride))
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
		Headers: yt.Headers, Body: yt.Body, ExpectStatus: yt.ExpectStatus, MaxBytes: yt.MaxBytes, SaveTo: yt.SaveTo}
}

func (yt yamlTask) add(d *DAG, baseDir string, opts []TaskOption) error {
	switch yt.Operator {
	case "", "empty":
		d.NewEmptyTask(yt.ID, opts...)
	case "custom":
		return yt.addCustom(d, baseDir, opts)
	case "bash":
		if yt.Command == "" {
			return errors.New("bash needs command")
		}
		d.NewBashTask(yt.ID, yt.Command, yt.Env, opts...)
	case "go":
		if strings.TrimSpace(yt.Code) == "" {
			return errors.New("go needs code")
		}
		d.NewGoTask(yt.ID, yt.Code, yt.Env, opts...)
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
	case "postgres":
		d.NewPostgresTask(yt.ID, PostgresConfig{ConnID: yt.ConnID, SQL: yt.SQL, Table: yt.Table,
			RowsFrom: yt.RowsFrom, Limit: yt.Limit, OnConflict: yt.OnConflict}, opts...)
	case "bigquery_query":
		d.NewBigQueryQueryTask(yt.ID, BigQueryQueryConfig{ConnID: yt.ConnID, ProjectID: yt.ProjectID,
			Location: yt.Location, SQL: yt.SQL, Limit: yt.Limit}, opts...)
	case "bigquery_insert":
		d.NewBigQueryInsertTask(yt.ID, BigQueryInsertConfig{ConnID: yt.ConnID, ProjectID: yt.ProjectID,
			Dataset: yt.Dataset, Table: yt.Table, RowsFrom: yt.RowsFrom}, opts...)
	case "gsheet_read":
		d.NewGSheetReadTask(yt.ID, GSheetReadConfig{ConnID: yt.ConnID, SpreadsheetID: yt.SpreadsheetID,
			Range: yt.Range, NoHeader: yt.NoHeader, KeepHeaders: yt.KeepHeaders, Limit: yt.Limit, Rename: yt.Rename}, opts...)
	case "gsheet_write":
		d.NewGSheetWriteTask(yt.ID, GSheetWriteConfig{ConnID: yt.ConnID, SpreadsheetID: yt.SpreadsheetID,
			Range: yt.Range, RowsFrom: yt.RowsFrom, Mode: yt.WriteMode, Columns: yt.Columns,
			UserEntered: yt.UserEntered}, opts...)
	case "file_read":
		d.NewFileReadTask(yt.ID, FileReadConfig{File: yt.File, Format: yt.Format, Sheet: yt.Sheet, Range: yt.Range,
			Delimiter: yt.Delimiter, NoHeader: yt.NoHeader, KeepHeaders: yt.KeepHeaders, Limit: yt.Limit, Rename: yt.Rename}, opts...)
	case "gcs_write":
		d.NewGCSWriteTask(yt.ID, StoreWriteConfig{ConnID: yt.ConnID, Bucket: yt.Bucket, Object: yt.Object, Format: yt.Format,
			Columns: yt.Columns, Delim: yt.Delimiter, Gzip: yt.Gzip, RowsFrom: yt.RowsFrom}, opts...)
	case "bigquery_external_table":
		d.NewBigQueryExternalTableTask(yt.ID, BigQueryExternalConfig{ConnID: yt.ConnID, ProjectID: yt.ProjectID, Location: yt.Location,
			Table: yt.Table, RowsFrom: yt.RowsFrom, AllFiles: yt.AllFiles, DefaultType: yt.DefaultType, ColumnTypes: yt.ColumnTypes,
			IfNotExists: yt.IfNotExists, Expiration: yt.Expiration, URIs: yt.URIs, Format: yt.Format, Columns: yt.Columns}, opts...)
	case "athena_query", "athena_external_table", "athena_load_table", "athena_delete_table",
		"redshift_query", "redshift_load_table", "redshift_delete_table", "redshift_catalog_sync":
		yt.addAWS(d, opts)
	case "bigquery_catalog_sync":
		d.NewBigQueryCatalogSyncTask(yt.ID, BigQueryCatalogSyncConfig{ConnID: yt.ConnID, ProjectID: yt.ProjectID, Regions: yt.Regions,
			Datasets: yt.Datasets, LookbackHours: yt.LookbackHours, MaxJobs: yt.MaxQueries, NoJobs: yt.NoQueries}, opts...)
	case "bigquery_delete_table":
		d.NewBigQueryDeleteTableTask(yt.ID, BigQueryDeleteConfig{ConnID: yt.ConnID, ProjectID: yt.ProjectID,
			Tables: yt.Tables, FailIfMissing: yt.FailIfMissing}, opts...)
	case "bigquery_load_table":
		d.NewBigQueryLoadTableTask(yt.ID, BigQueryLoadConfig{ConnID: yt.ConnID, ProjectID: yt.ProjectID, Location: yt.Location,
			Table: yt.Table, SourceTable: yt.SourceTable, RowsFrom: yt.RowsFrom, Mode: yt.Mode, MergeKeys: yt.MergeKeys,
			WhenMatched: yt.WhenMatched, Conditions: yt.Conditions, ConditionMode: yt.ConditionMode, PartitionBy: yt.PartitionBy,
			ClusterBy: yt.ClusterBy, ColumnMap: yt.ColumnMap, CastMode: yt.CastMode, Expiration: yt.Expiration, PartitionExpirationDays: yt.PartitionExpirationDays, Metadata: yt.Metadata, MetadataPrefix: yt.MetadataPrefix, MetadataNames: yt.MetadataNames,
			InsertedAt: yt.InsertedAt, JobDate: yt.JobDate, Timezone: yt.Timezone}, opts...)
	case "s3_write":
		d.NewS3WriteTask(yt.ID, StoreWriteConfig{ConnID: yt.ConnID, Bucket: yt.Bucket, Object: firstNonEmpty(yt.Key, yt.Object),
			Region: yt.Region, Format: yt.Format, Columns: yt.Columns, Delim: yt.Delimiter, Gzip: yt.Gzip, RowsFrom: yt.RowsFrom}, opts...)
	case "merge":
		d.NewMergeTask(yt.ID, MergeConfig{Mode: yt.Mode, Inputs: yt.Inputs, On: yt.On, JoinType: yt.JoinType}, opts...)
	case "transform":
		d.NewTransformTask(yt.ID, TransformConfig{Input: yt.Input, Steps: yt.Steps}, opts...)
	case "branch":
		cfg := IfConfig{Input: yt.Input, Path: yt.Path, Left: yt.Left, Op: yt.Op, Value: yt.Value, Then: yt.Then, Else: yt.Else}
		if err := cfg.validate(); err != nil {
			return err
		}
		d.NewIfTask(yt.ID, cfg, opts...)
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
		return fmt.Errorf("unknown operator %q (empty, bash, go, bigquery, http, postgres, transform, bigquery_query, bigquery_insert, gsheet_read, gsheet_write, file_read, merge, branch, gcs_write, s3_write, bigquery_external_table, bigquery_load_table, bigquery_delete_table, bigquery_catalog_sync, athena_query, athena_external_table, athena_load_table, athena_delete_table, redshift_query, redshift_load_table, redshift_delete_table, redshift_catalog_sync, trigger_dagrun, file_sensor, time_delta_sensor, external_task_sensor, http_sensor, custom)", yt.Operator)
	}
	return nil
}

// ---- folder loader with change detection --------------------------------------

var (
	folderMu    sync.Mutex
	folderStamp = map[string]string{} // path -> modtime|size
)

// InvalidateYAML forces the file to be re-parsed on the next LoadDagFolder.
func InvalidateYAML(path string) {
	folderMu.Lock()
	delete(folderStamp, path)
	folderMu.Unlock()
}

// LoadDagFolder (re)loads every *.yaml / *.yml under dir. Only changed files are
// re-parsed; deleted files are unregistered. Safe to call repeatedly.
func LoadDagFolder(dir string) {
	folderMu.Lock()
	defer folderMu.Unlock()
	if LoadCustomNodes(filepath.Join(dir, "builder", "nodes")) {
		folderStamp = map[string]string{} // DAGs may use the node types that changed
	}
	seen := map[string]bool{}
	_ = filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if e.IsDir() {
			if e.Name() == "nodes" && filepath.Base(filepath.Dir(p)) == "builder" {
				return fs.SkipDir // node definitions, not DAGs
			}
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
		for _, d := range dags {
			d.File = p
		}
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

// addAWS adds the Athena and Redshift tasks.
func (yt yamlTask) addAWS(d *DAG, opts []TaskOption) {
	ath := AthenaConn{ConnID: yt.ConnID, Region: yt.Region, Workgroup: yt.Workgroup, OutputLocation: yt.OutputLocation, Catalog: yt.Catalog, Database: yt.Database}
	rs := RedshiftConn{ConnID: yt.ConnID, Region: yt.Region, ClusterIdentifier: yt.ClusterIdentifier, WorkgroupName: yt.WorkgroupName,
		Database: yt.Database, DbUser: yt.DbUser, SecretArn: yt.SecretArn}
	lo := LoadOptions{Mode: yt.Mode, MergeKeys: yt.MergeKeys, WhenMatched: yt.WhenMatched, Conditions: yt.Conditions, ConditionMode: yt.ConditionMode,
		ColumnMap: yt.ColumnMap, CastMode: yt.CastMode, Metadata: yt.Metadata, MetadataPrefix: yt.MetadataPrefix, MetadataNames: yt.MetadataNames,
		InsertedAt: yt.InsertedAt, JobDate: yt.JobDate, Timezone: yt.Timezone}
	switch yt.Operator {
	case "athena_query":
		d.NewAthenaQueryTask(yt.ID, AthenaQueryConfig{AthenaConn: ath, SQL: yt.SQL, Limit: yt.Limit}, opts...)
	case "athena_external_table":
		d.NewAthenaExternalTableTask(yt.ID, AthenaExternalConfig{AthenaConn: ath, Table: yt.Table, RowsFrom: yt.RowsFrom, DefaultType: yt.DefaultType,
			ColumnTypes: yt.ColumnTypes, IfNotExists: yt.IfNotExists, Location: yt.S3Location, Format: yt.Format, Columns: yt.Columns}, opts...)
	case "athena_load_table":
		d.NewAthenaLoadTableTask(yt.ID, AthenaLoadConfig{AthenaConn: ath, LoadOptions: lo, Table: yt.Table, TableLocation: yt.TableLocation,
			SourceTable: yt.SourceTable, RowsFrom: yt.RowsFrom, PartitionBy: yt.Partitioning}, opts...)
	case "athena_delete_table":
		d.NewAthenaDeleteTableTask(yt.ID, AthenaDeleteConfig{AthenaConn: ath, Tables: yt.Tables, FailIfMissing: yt.FailIfMissing}, opts...)
	case "redshift_query":
		d.NewRedshiftQueryTask(yt.ID, RedshiftQueryConfig{RedshiftConn: rs, SQL: yt.SQL, Limit: yt.Limit}, opts...)
	case "redshift_load_table":
		d.NewRedshiftLoadTableTask(yt.ID, RedshiftLoadConfig{RedshiftConn: rs, LoadOptions: lo, Table: yt.Table, IAMRole: yt.IAMRole, RowsFrom: yt.RowsFrom,
			SourceTable: yt.SourceTable, AllFiles: yt.AllFiles, S3Region: yt.S3Region, DistKey: yt.DistKey, SortKey: yt.SortKey}, opts...)
	case "redshift_catalog_sync":
		d.NewRedshiftCatalogSyncTask(yt.ID, RedshiftCatalogSyncConfig{RedshiftConn: rs, Schemas: yt.Schemas, LookbackHours: yt.LookbackHours,
			MaxQueries: yt.MaxQueries, NoQueries: yt.NoQueries}, opts...)
	case "redshift_delete_table":
		d.NewRedshiftDeleteTableTask(yt.ID, RedshiftDeleteConfig{RedshiftConn: rs, Tables: yt.Tables, FailIfMissing: yt.FailIfMissing}, opts...)
	}
}
