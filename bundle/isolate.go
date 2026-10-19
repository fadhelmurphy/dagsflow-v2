package bundle

import (
	"os"

	"dagsflow-go/dag"
)

// TaskCommand is the hidden command of the dagsflow binary that runs one task
// of a compiled-in Go DAG in a child process (`dagsflow-go __task run`).
const TaskCommand = "__task"

// IsolateCompiledIn makes every task of a compiled-in Go DAG run in a child
// process of self (normally os.Executable()), the same way tasks of DAG
// bundles do: a timeout, a "mark as failed" or a scheduler/worker shutdown
// kills the whole process group, even when the task's code ignores ctx.
// XComs, variables, connections and the other TaskContext calls reach the
// host through the bundle RPC server.
func IsolateCompiledIn(self string) {
	p := &proxy{bin: self, args: []string{TaskCommand}}
	dag.SetIsolatedRunner(func(ctx *dag.TaskContext, t *dag.Task, action string) ([]string, bool, error) {
		r, err := p.invoke(ctx, ctx, Request{Action: action, DagID: t.DAG().ID, TaskID: t.ID, Info: ctx.RunInfo}, ctx.LogWriter())
		return r.Selected, r.PokeOK, err
	})
}

// TaskMain is the entrypoint of `dagsflow-go __task run`: it runs one action
// of a compiled-in task and reports back to the host. It never returns.
func TaskMain(args []string) {
	if len(args) != 1 || args[0] != "run" {
		os.Stderr.WriteString("dagsflow-go __task: internal command, started by the scheduler or a worker\n")
		os.Exit(2)
	}
	dag.SetIsolatedRunner(nil) // the child runs the real code
	os.Exit(runChild())
}
