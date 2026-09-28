package bundle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"dagsflow-go/dag"
)

// Main is the entrypoint of a compiled DAG bundle (generated main.go calls it).
func Main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "dagsflow DAG bundle: use `describe` or `run` (started by the scheduler)")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "describe":
		_ = json.NewEncoder(os.Stdout).Encode(describe())
	case "run":
		os.Exit(runChild())
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

// rpcClient is the bundle side of the host connection. It implements
// dag.RunState and dag.SecretsBackend by calling back into the scheduler.
type rpcClient struct {
	c     *rpc.Client
	token string
}

func (r *rpcClient) call(method string, a Args) (Reply, error) {
	a.Token = r.token
	var rep Reply
	err := r.c.Call("Host."+method, &a, &rep)
	return rep, err
}

func (r *rpcClient) XComPush(taskID string, mapIndex int, key string, v any) error {
	_, err := r.call("XComPush", Args{TaskID: taskID, MapIndex: mapIndex, Key: key, Value: v})
	return err
}

func (r *rpcClient) XComPull(taskID, key string) (any, bool) {
	rep, err := r.call("XComPull", Args{TaskID: taskID, Key: key})
	return rep.Value, err == nil && rep.Found
}

func (r *rpcClient) TriggerDagRun(_ context.Context, dagID string, conf map[string]any, wait bool) (string, error) {
	rep, err := r.call("TriggerDagRun", Args{DagID: dagID, Conf: conf, Wait: wait})
	return rep.Str, err
}

func (r *rpcClient) ExternalTaskState(dagID, taskID string, logical time.Time) (dag.State, bool, error) {
	rep, err := r.call("ExternalTaskState", Args{DagID: dagID, TaskID: taskID, Logical: logical})
	return dag.State(rep.Str), rep.Found, err
}

func (r *rpcClient) Name() string { return "scheduler" }

func (r *rpcClient) GetVariable(key string) (string, bool, error) {
	rep, err := r.call("GetVariable", Args{Name: key})
	return rep.Str, rep.Found, err
}

func (r *rpcClient) GetConnection(id string) (*dag.Connection, bool, error) {
	rep, err := r.call("GetConnection", Args{Name: id})
	return rep.Conn, rep.Found, err
}

func runChild() int {
	var req Request
	if err := json.NewDecoder(os.Stdin).Decode(&req); err != nil {
		fmt.Fprintln(os.Stderr, "bundle: bad request:", err)
		return 2
	}
	conn, err := jsonrpc.Dial("tcp", os.Getenv("DAGSFLOW_RPC_ADDR"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "bundle: cannot reach scheduler:", err)
		return 2
	}
	rc := &rpcClient{c: conn, token: os.Getenv("DAGSFLOW_RPC_TOKEN")}
	defer conn.Close()
	dag.SetSecretsBackends(rc)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	res := execute(ctx, req, rc)
	res.Done = true
	if _, err := rc.call("Report", Args{Result: res}); err != nil {
		fmt.Fprintln(os.Stderr, "bundle: report:", err)
		return 2
	}
	if res.Err != "" {
		return 1
	}
	return 0
}

func execute(ctx context.Context, req Request, rc *rpcClient) (res Result) {
	defer func() {
		if p := recover(); p != nil {
			res = Result{Err: fmt.Sprintf("panic: %v\n%s", p, debug.Stack())}
		}
	}()
	d, ok := dag.Get(req.DagID)
	if !ok {
		return Result{Err: fmt.Sprintf("dag %s not in bundle (import errors: %v)", req.DagID, dag.ImportErrors())}
	}
	if req.Action == "callback" {
		return runCallbacks(ctx, d, req)
	}
	t, ok := d.Task(req.TaskID)
	if !ok {
		return Result{Err: fmt.Sprintf("task %s not in dag %s", req.TaskID, req.DagID)}
	}
	tctx := dag.NewTaskContext(ctx, t, req.Info, rc, os.Stdout)
	switch req.Action {
	case "poke":
		sn := t.Sensor()
		if sn == nil {
			return Result{Err: "task is not a sensor"}
		}
		ok, err := sn.Poke(tctx)
		return Result{PokeOK: ok, Err: errStr(err)}
	case "task":
		sel, err := t.Run(tctx)
		return Result{Selected: sel, Err: errStr(err)}
	}
	return Result{Err: "unknown action " + req.Action}
}

func runCallbacks(ctx context.Context, d *dag.DAG, req Request) Result {
	var cbs []dag.Callback
	switch req.Callback {
	case "dag_on_success":
		cbs = d.OnSuccess
	case "dag_on_failure":
		cbs = d.OnFailure
	case "dag_on_sla_miss":
		cbs = d.OnSLAMiss
	default:
		t, ok := d.Task(req.TaskID)
		if !ok {
			return Result{Err: "task not found"}
		}
		switch req.Callback {
		case "task_on_success":
			cbs = t.OnSuccessCallbacks()
		case "task_on_failure":
			cbs = t.OnFailureCallbacks()
		case "task_on_retry":
			cbs = t.OnRetryCallbacks()
		}
	}
	cc := req.CC.context()
	var errs []error
	for _, cb := range cbs {
		if err := cb(ctx, cc); err != nil {
			errs = append(errs, err)
		}
	}
	return Result{Err: errStr(errors.Join(errs...))}
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
