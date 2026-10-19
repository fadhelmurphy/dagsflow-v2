package dag

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type SensorMode string

const (
	ModePoke       SensorMode = "poke"       // holds a worker slot while waiting
	ModeReschedule SensorMode = "reschedule" // releases its slot between pokes (≈ deferrable)
)

// SensorFunc returns true when the condition is met.
type SensorFunc func(ctx *TaskContext) (bool, error)

// SensorConfig ~ BaseSensorOperator(poke_interval, timeout, mode, soft_fail).
type SensorConfig struct {
	PokeInterval time.Duration // default 60s
	Timeout      time.Duration // default 7 days
	Mode         SensorMode    // default poke
	SoftFail     bool          // on timeout: skipped instead of failed
}

type SensorSpec struct {
	SensorConfig
	Poke SensorFunc
}

// NewSensor creates a custom sensor task.
func (d *DAG) NewSensor(id string, poke SensorFunc, cfg SensorConfig, opts ...TaskOption) *Task {
	if cfg.PokeInterval <= 0 {
		cfg.PokeInterval = 60 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 7 * 24 * time.Hour
	}
	if cfg.Mode == "" {
		cfg.Mode = ModePoke
	}
	return d.addTask(id, nil, nil, &SensorSpec{SensorConfig: cfg, Poke: poke}, opts)
}

// NewFileSensor waits until a (templated) path exists. Glob patterns allowed.
func (d *DAG) NewFileSensor(id, pathTmpl string, cfg SensorConfig, opts ...TaskOption) *Task {
	opts = append([]TaskOption{WithTemplateField("filepath", pathTmpl, "text"), WithOperatorName("FileSensor")}, opts...)
	return d.NewSensor(id, func(ctx *TaskContext) (bool, error) {
		p, err := ctx.RenderTemplateField("filepath")
		if err != nil {
			return false, err
		}
		matches, err := filepath.Glob(p)
		if err != nil {
			return false, err
		}
		if len(matches) == 0 {
			if _, err := os.Stat(p); err != nil {
				ctx.Logf("waiting for %s", p)
				return false, nil
			}
		}
		return true, nil
	}, cfg, opts...)
}

// NewTimeDeltaSensor waits until data_interval_end + delta.
func (d *DAG) NewTimeDeltaSensor(id string, delta time.Duration, cfg SensorConfig, opts ...TaskOption) *Task {
	opts = append([]TaskOption{WithOperatorName("TimeDeltaSensor")}, opts...)
	return d.NewSensor(id, func(ctx *TaskContext) (bool, error) {
		target := ctx.DataIntervalEnd.Add(delta)
		ctx.Logf("waiting until %s", target.Format(time.RFC3339))
		return !time.Now().Before(target), nil
	}, cfg, opts...)
}

// NewExternalTaskSensor waits for a task (or, with externalTaskID "", the whole
// run) of another DAG with the same logical date. Fails if it failed.
func (d *DAG) NewExternalTaskSensor(id, externalDagID, externalTaskID string, cfg SensorConfig, opts ...TaskOption) *Task {
	opts = append([]TaskOption{WithOperatorName("ExternalTaskSensor")}, opts...)
	return d.NewSensor(id, func(ctx *TaskContext) (bool, error) {
		st, found, err := ctx.ExternalTaskState(externalDagID, externalTaskID, ctx.LogicalDate)
		if err != nil {
			return false, err
		}
		if !found {
			ctx.Logf("no run of %s at %s yet", externalDagID, ctx.LogicalDate.Format(time.RFC3339))
			return false, nil
		}
		ctx.Logf("%s.%s is %s", externalDagID, externalTaskID, st)
		switch st {
		case StateSuccess:
			return true, nil
		case StateFailed, StateUpstreamFailed:
			return false, fmt.Errorf("external %s.%s is %s", externalDagID, externalTaskID, st)
		}
		return false, nil
	}, cfg, opts...)
}

// NewHTTPSensor pokes an endpoint until it returns an expected status.
func (d *DAG) NewHTTPSensor(id string, req HTTPRequest, cfg SensorConfig, opts ...TaskOption) *Task {
	opts = append([]TaskOption{WithTemplateFields(httpTemplateFields(req)...), WithOperatorName("HttpSensor")}, opts...)
	return d.NewSensor(id, func(ctx *TaskContext) (bool, error) {
		resp, err := httpDo(ctx, req)
		if err != nil {
			ctx.Logf("poke failed: %v", err)
			return false, nil
		}
		_ = resp.Body.Close()
		return statusOK(resp.StatusCode, req.ExpectStatus), nil
	}, cfg, opts...)
}
