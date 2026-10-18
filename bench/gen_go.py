"""Generate the Go DAG workloads: python3 gen_go.py <dir> <filler DAGs>.

Writes <dir>/df_dags with chain50.go (50 sequential Go tasks), fan200.go
(200 independent Go tasks) and N filler DAGs (one file each, 5 tasks in a chain).
Every task is a Go function that returns nil, so the run time is orchestration overhead.
"""
import os
import shutil
import sys

root, n = sys.argv[1], int(sys.argv[2])
df = f"{root}/df_dags"
shutil.rmtree(df, ignore_errors=True)
os.makedirs(df)

HEAD_FMT = 'package main\n\nimport (\n\t"fmt"\n\n\t"dagsflow-go/dag"\n)\n\nfunc noop(ctx *dag.TaskContext) error { return nil }\n\n'

with open(f"{df}/chain50.go", "w") as f:
    f.write(HEAD_FMT + '''func init() {
	d := dag.NewDAG("chain50", "")
	var prev *dag.Task
	for i := 0; i < 50; i++ {
		t := d.NewTask(fmt.Sprintf("t%d", i), noop)
		if prev != nil {
			prev.Then(t)
		}
		prev = t
	}
	dag.Register(d)
}
''')

with open(f"{df}/fan200.go", "w") as f:
    f.write(HEAD_FMT + '''func init() {
	d := dag.NewDAG("fan200", "")
	for i := 0; i < 200; i++ {
		d.NewTask(fmt.Sprintf("t%d", i), noop)
	}
	dag.Register(d)
}
''')

for k in range(n):
    with open(f"{df}/d{k:04d}.go", "w") as f:
        f.write(f'''package main

import "dagsflow-go/dag"

func noop{k:04d}(ctx *dag.TaskContext) error {{ return nil }}

func init() {{
	d := dag.NewDAG("d{k:04d}", "")
	a := d.NewTask("t0", noop{k:04d})
	b := d.NewTask("t1", noop{k:04d})
	c := d.NewTask("t2", noop{k:04d})
	e := d.NewTask("t3", noop{k:04d})
	f := d.NewTask("t4", noop{k:04d})
	dag.Chain(a, b, c, e, f)
	dag.Register(d)
}}
''')
