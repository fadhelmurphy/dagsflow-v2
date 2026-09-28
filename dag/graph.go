package dag

import (
	"fmt"
	"io"
)

// PrintGraph prints the DAG as a tree. states (optional) annotates each task
// with its state for a given DagRun.
func (d *DAG) PrintGraph(w io.Writer, states map[string]State) {
	fmt.Fprintf(w, "DAG: %s  (schedule=%q)\n", d.ID, d.Schedule)
	visited := map[string]bool{}
	var roots []*Task
	for _, t := range d.topo {
		if len(t.upstream) == 0 {
			roots = append(roots, t)
		}
	}
	for i, t := range roots {
		d.printTree(w, t, "", i == len(roots)-1, visited, states)
	}
}

func (d *DAG) printTree(w io.Writer, t *Task, prefix string, last bool, visited map[string]bool, states map[string]State) {
	conn, next := "├── ", prefix+"│   "
	if last {
		conn, next = "└── ", prefix+"    "
	}
	label := t.ID
	if t.triggerRule != AllSuccess {
		label += fmt.Sprintf(" [%s]", t.triggerRule)
	}
	if states != nil {
		label += fmt.Sprintf("  (%s)", states[t.ID])
	}
	if visited[t.ID] {
		fmt.Fprintf(w, "%s%s%s ↺\n", prefix, conn, label)
		return
	}
	visited[t.ID] = true
	fmt.Fprintf(w, "%s%s%s\n", prefix, conn, label)
	for i, c := range t.downstream {
		d.printTree(w, c, next, i == len(t.downstream)-1, visited, states)
	}
}
