package dag

import (
	"fmt"
	"time"
)

// DatasetEvent ~ Airflow's DatasetEvent: an update of a dataset by a producer
// task, with optional metadata (extra) set by that task.
type DatasetEvent struct {
	Timestamp   time.Time      `json:"timestamp"`
	SourceDagID string         `json:"source_dag_id"`
	SourceTask  string         `json:"source_task_id"`
	SourceRunID string         `json:"source_run_id"`
	Extra       map[string]any `json:"extra,omitempty"`
}

// SetOutletExtra attaches metadata to the dataset event this task emits for
// uri when it succeeds (Airflow: outlet_events[dataset].extra = {...}).
// Consumers read it with TriggeringDatasetEvents / DatasetExtraAs.
// Values must be JSON-serialisable.
func (c *TaskContext) SetOutletExtra(uri string, extra map[string]any) error {
	ok := false
	for _, o := range c.Task.Outlets() {
		ok = ok || o == uri
	}
	if !ok {
		return fmt.Errorf("task %s has no outlet %q (declare it with dag.WithOutlets)", c.Task.ID, uri)
	}
	return c.run.SetOutletExtra(c.Task.ID, c.MapIndex, uri, extra)
}

// TriggeringDatasetEvents returns, per dataset URI, the events that triggered
// this dataset-scheduled run (oldest first). Empty for other run types.
func (c *TaskContext) TriggeringDatasetEvents() (map[string][]DatasetEvent, error) {
	raw, ok := c.Conf["triggering_dataset_events"]
	if !ok || raw == nil {
		return map[string][]DatasetEvent{}, nil
	}
	return convert[map[string][]DatasetEvent](raw)
}

// DatasetExtraAs decodes the extra of the latest triggering event of uri into T
// (a struct or map). ok is false when the run was not triggered by uri or the
// producer set no extra.
func DatasetExtraAs[T any](c *TaskContext, uri string) (v T, ok bool, err error) {
	evs, err := c.TriggeringDatasetEvents()
	if err != nil {
		return v, false, err
	}
	list := evs[uri]
	if len(list) == 0 || list[len(list)-1].Extra == nil {
		return v, false, nil
	}
	v, err = convert[T](list[len(list)-1].Extra)
	return v, err == nil, err
}
