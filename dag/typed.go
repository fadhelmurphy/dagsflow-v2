package dag

import "fmt"

// Typed, TaskFlow-style API (Airflow's @task): a task's return value is its
// output, and passing an output to another task both wires the dependency and
// hands over the value (via XCom "return_value"). Types are checked by the Go
// compiler instead of at runtime.
//
//	branches := dag.Fn(d, "list_branches", func(ctx *dag.TaskContext) ([]string, error) {
//		return []string{"jakarta", "bandung"}, nil
//	})
//	rows := dag.Map(d, "load_branch", branches, func(ctx *dag.TaskContext, b string) (int, error) {
//		return load(ctx, b)
//	})
//	dag.Consume(d, "publish", rows, func(ctx *dag.TaskContext, rows []int) error {
//		ctx.Logf("loaded %d rows", sum(rows))
//		return nil
//	})
//
// Values travel as JSON (they are persisted and may cross process
// boundaries), so T must be JSON-serialisable: numbers, strings, slices,
// maps and structs with exported fields.

// ReturnKey is the XCom key typed tasks push their result under.
const ReturnKey = "return_value"

// Out is the typed output of a task. It is also a Node, so it can be used in
// Chain / After / Parallel like the task itself.
type Out[T any] struct {
	task *Task
	key  string
}

func (o Out[T]) nodeTasks() []*Task { return []*Task{o.task} }

// Task returns the task that produces the output (for options, Then, ...).
func (o Out[T]) Task() *Task { return o.task }

// Get reads the value from inside another task of the same run. Prefer passing
// the output to Fn1/Fn2/Map/Consume, which also wires the dependency.
func (o Out[T]) Get(ctx *TaskContext) (T, error) { return XComPullAs[T](ctx, o.task.ID, o.key) }

// Then continues a chain: out.Then(t) == out.Task() >> t.
func (o Out[T]) Then(next *Task) *Task { return o.task.Then(next) }

// OutputOf wraps an XCom of a classic task as a typed output, e.g. the last
// stdout line of a bash task: dag.OutputOf[string](bash, dag.ReturnKey).
func OutputOf[T any](t *Task, key string) Out[T] {
	if key == "" {
		key = ReturnKey
	}
	return Out[T]{task: t, key: key}
}

func push[T any](ctx *TaskContext, v T) error { return ctx.XComPush(ReturnKey, v) }

func pull[T any](ctx *TaskContext, o Out[T]) (T, error) {
	v, err := XComPullAs[T](ctx, o.task.ID, o.key)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("input from %s: %w", o.task.ID, err)
	}
	return v, nil
}

// checkInput records a definition error when an output from another DAG is used.
func checkInput(d *DAG, id string, ins ...*Task) {
	for _, in := range ins {
		if in == nil {
			d.buildErr(fmt.Errorf("task %q: input is an empty output", id))
		} else if in.dag != d {
			d.buildErr(fmt.Errorf("task %q: input %s.%s belongs to another DAG", id, in.dag.ID, in.ID))
		}
	}
}

// Fn creates a task without inputs whose return value is its output.
func Fn[T any](d *DAG, id string, fn func(*TaskContext) (T, error), opts ...TaskOption) Out[T] {
	t := d.NewTask(id, func(ctx *TaskContext) error {
		v, err := fn(ctx)
		if err != nil {
			return err
		}
		return push(ctx, v)
	}, opts...)
	return Out[T]{task: t, key: ReturnKey}
}

// Fn1 creates a task that receives one upstream output (and runs after it).
func Fn1[A, T any](d *DAG, id string, a Out[A], fn func(*TaskContext, A) (T, error), opts ...TaskOption) Out[T] {
	checkInput(d, id, a.task)
	t := d.NewTask(id, func(ctx *TaskContext) error {
		av, err := pull(ctx, a)
		if err != nil {
			return err
		}
		v, err := fn(ctx, av)
		if err != nil {
			return err
		}
		return push(ctx, v)
	}, opts...)
	if a.task != nil {
		t.After(a)
	}
	return Out[T]{task: t, key: ReturnKey}
}

// Fn2 creates a task that receives two upstream outputs.
func Fn2[A, B, T any](d *DAG, id string, a Out[A], b Out[B], fn func(*TaskContext, A, B) (T, error), opts ...TaskOption) Out[T] {
	checkInput(d, id, a.task, b.task)
	t := d.NewTask(id, func(ctx *TaskContext) error {
		av, err := pull(ctx, a)
		if err != nil {
			return err
		}
		bv, err := pull(ctx, b)
		if err != nil {
			return err
		}
		v, err := fn(ctx, av, bv)
		if err != nil {
			return err
		}
		return push(ctx, v)
	}, opts...)
	if a.task != nil && b.task != nil {
		t.After(a, b)
	}
	return Out[T]{task: t, key: ReturnKey}
}

// Consume creates a task that receives one output and returns nothing.
func Consume[A any](d *DAG, id string, a Out[A], fn func(*TaskContext, A) error, opts ...TaskOption) *Task {
	checkInput(d, id, a.task)
	t := d.NewTask(id, func(ctx *TaskContext) error {
		av, err := pull(ctx, a)
		if err != nil {
			return err
		}
		return fn(ctx, av)
	}, opts...)
	if a.task != nil {
		t.After(a)
	}
	return t
}

// Map creates a mapped task (Airflow's .expand()): one instance per element
// of the upstream list. Its output is the list of the instances' results, in
// map-index order.
func Map[A, T any](d *DAG, id string, in Out[[]A], fn func(*TaskContext, A) (T, error), opts ...TaskOption) Out[[]T] {
	checkInput(d, id, in.task)
	src := ""
	if in.task != nil {
		src = in.task.ID
	}
	opts = append([]TaskOption{Expand(FromXCom(src, in.key))}, opts...)
	t := d.NewTask(id, func(ctx *TaskContext) error {
		item, err := ItemAs[A](ctx)
		if err != nil {
			return fmt.Errorf("mapped item %d: %w", ctx.MapIndex, err)
		}
		v, err := fn(ctx, item)
		if err != nil {
			return err
		}
		return push(ctx, v)
	}, opts...)
	return Out[[]T]{task: t, key: ReturnKey}
}

// MapItems maps over a list known when the DAG is defined.
func MapItems[A, T any](d *DAG, id string, items []A, fn func(*TaskContext, A) (T, error), opts ...TaskOption) Out[[]T] {
	anyItems := make([]any, len(items))
	for i, it := range items {
		anyItems[i] = it
	}
	opts = append([]TaskOption{Expand(FromItems(anyItems...))}, opts...)
	t := d.NewTask(id, func(ctx *TaskContext) error {
		item, err := ItemAs[A](ctx)
		if err != nil {
			return fmt.Errorf("mapped item %d: %w", ctx.MapIndex, err)
		}
		v, err := fn(ctx, item)
		if err != nil {
			return err
		}
		return push(ctx, v)
	}, opts...)
	return Out[[]T]{task: t, key: ReturnKey}
}
