<p align="center"><img src="docs/banner.jpg" alt="dagsflow — Orchestrator" width="100%"></p>

# dagsflow-go

A workflow orchestrator in Go with the execution model of **Apache Airflow**:
one scheduler for all DAGs, every execution is a **DagRun** made of **TaskInstances**
whose state is persisted, so runs can be rerun, cleared, backfilled and resumed after a restart.

![dagsflow demo: DAG list, grid, mapped task logs, graph, trigger and roles](docs/demo.gif)

**Visual builder**: build an ETL by drag and drop (Postgres, BigQuery, Google Sheets, HTTP, no-code transforms); it saves a normal YAML DAG.

![dagsflow builder: drag and drop nodes, connect, run, BigQuery to Google Sheets](docs/builder-demo.gif)

## Concepts (Airflow equivalents)

| Airflow                          | dagsflow-go                                       |
|----------------------------------|---------------------------------------------------|
| DAG file / DagBag                | DAG folder (`dags/`): `.go` files compiled automatically, `.yaml` files; or `builtin_dags/` compiled into the binary |
| `airflow scheduler`              | `dagsflow-go scheduler [-D]`                       |
| Metadata DB                      | Postgres (`DAGSFLOW_DATABASE_URL`) or JSON files in `$DAGSFLOW_HOME/state/` |
| DagRun / TaskInstance            | `engine.DagRun` / `engine.TaskInstance`            |
| logical_date, data_interval      | `ctx.LogicalDate`, `ctx.DataIntervalStart/End`     |
| catchup, max_active_runs, start_date | `dag.WithCatchup`, `dag.WithMaxActiveRuns`, `dag.WithStartDate` |
| default_args (retries, retry_delay, execution_timeout) | `dag.WithDefaultArgs` / `dag.WithRetries`, `dag.WithTimeout` |
| params + dag_run.conf            | `dag.WithParams`, `--conf`, `dag.ParamAs[T]`       |
| XCom (per task, per run)         | `ctx.XComPush`, `dag.XComPullAs[T](ctx, task, key)`|
| PythonOperator / BranchPythonOperator / EmptyOperator | `NewTask` / `NewBranchTask` / `NewEmptyTask` |
| TriggerDagRunOperator            | `NewTriggerDagRunTask` (wait_for_completion)       |
| BigQueryInsertJobOperator        | `NewBigQueryTask` (templated SQL)                  |
| Jinja `{{ ds }}`                 | Go template `{{ .ds }}`, `{{ xcom_pull "t" "k" }}` (Jinja style also accepted) |
| Trigger rules                    | all_success, all_failed, all_done, one_success, one_failed, none_failed, none_failed_min_one_success, none_skipped, always |
| Task log per attempt             | `logs/dag_id=…/run_id=…/task_id=…/attempt=N.log` (or in Postgres) |
| CeleryExecutor / KubernetesExecutor | `--executor worker` / `--executor kubernetes`   |

## Example DAG

```go
func init() {
	d := dag.NewDAG("dag1", "@daily",
		dag.WithParams(map[string]any{"param2": 0}),
		dag.WithDefaultArgs(dag.DefaultArgs{Retries: 2, RetryDelay: time.Minute}),
	)
	a := d.NewTask("a", func(ctx *dag.TaskContext) error {
		ctx.Logf("run for %s", ctx.LogicalDate)
		return ctx.XComPush("key", "value")
	})
	b := d.NewTask("b", func(ctx *dag.TaskContext) error {
		v, err := dag.XComPullAs[string](ctx, "a", "key")
		if err != nil {
			return err
		}
		ctx.Logf("got %s", v)
		return nil
	})
	a.Then(b) // a >> b ; branch.ThenAll(x, y) ; dag.Chain(a, b, c)
	dag.Register(d)
}
```

A task returns an `error` to fail (panics are recovered too). `ctx` is a `context.Context`
that is cancelled on timeout or scheduler stop: pass it to your DB/HTTP clients.

Each task of a compiled-in Go DAG runs in a child process of the dagsflow binary
(`dagsflow-go __task run`), like Airflow runs every task in its own process. A timeout,
**Mark failed**, a scheduler shutdown or a cancel on a worker kills that process group,
so code that ignores `ctx` (a `time.Sleep`, a library without context support) is
stopped too. XComs, variables, connections and the other `TaskContext` calls go back to
the scheduler or worker over a local RPC connection. Package-level variables are not
shared between tasks (each task is a new process); keep state in XComs. Set
`DAGSFLOW_GO_TASKS=inprocess` to run them as goroutines in the scheduler/worker process
instead, as before (then a task that ignores `ctx` keeps running after a timeout).

> XComs and conf are stored as JSON, so numbers come back as `float64`.
> Use `dag.XComPullAs[int]` / `dag.ParamAs[int]` instead of a `.(int)` type assertion.

> After a branch, the join task needs `dag.WithTriggerRule(dag.NoneFailedMinOneSuccess)`
> (same as Airflow), because the paths that were not chosen are `skipped`.

## Writing DAGs in Go (`with DAG` and `>>` equivalents)

Go has neither context managers nor operator overloading. Instead, tasks are created through DAG methods (`d.NewTask`, `d.NewBashTask`, ...) and dependencies are written with functions.

| Airflow | dagsflow |
|---|---|
| `with DAG(...) as dag:` | `d := dag.NewDAG(...)` then `d.NewBashTask(...)`, `d.NewBigQueryTask(...)` |
| `a >> b >> c` | `dag.Chain(a, b, c)` or `a.Then(b).Then(c)` |
| `a >> [b, c] >> d` | `dag.Chain(a, dag.Parallel(b, c), d)` |
| `[a, b] >> c` | `c.After(a, b)` |
| `@task` (TaskFlow) | `dag.Fn`, `dag.Fn1`, `dag.Fn2`, `dag.Consume` |
| `task.expand(x=...)` | `dag.Map(d, id, output, fn)` / `dag.MapItems(d, id, list, fn)` |

### Typed TaskFlow

A task's return value is its output. Passing an output to another task makes it a dependency and sends the value through XCom, with types checked by the compiler:

```go
branches := dag.Fn(d, "list_branches", func(ctx *dag.TaskContext) ([]string, error) {
	return []string{"jakarta", "bandung"}, nil
})
rows := dag.Map(d, "load_branch", branches, func(ctx *dag.TaskContext, b string) (int, error) {
	return loadBranch(ctx, b) // one instance per branch
})
dag.Consume(d, "publish", rows, func(ctx *dag.TaskContext, rows []int) error {
	ctx.Logf("total %d", sum(rows))
	return nil
})
```

- `Out[T]` works in `Chain`/`After`/`Parallel` like a regular task; `out.Task()` returns its `*Task`.
- Output of a classic task (e.g. the last stdout line of a bash task): `dag.OutputOf[string](bashTask, dag.ReturnKey)`.
- Values travel as JSON, so `T` must be serialisable (numbers, strings, slices, maps, structs with exported fields).
- The output of `Map` is a slice ordered by map index.

The older API (`NewTask`, `XComPush`, `XComPullAs`, `Expand`) keeps working.

### Definition errors no longer crash

Duplicate or empty task ids, self-dependencies, links across DAGs and `nil` tasks are recorded as an **import error** of that DAG. The scheduler/webserver process keeps running, including for DAGs in `builtin_dags/`.

### Templates: Go and Jinja style

Templates use Go's `text/template` (`{{ .ds }}`, `{{ .params.x }}`). So that SQL/bash files from Airflow/Composer can be copied as they are, the common Jinja forms are accepted too and can be mixed with Go style:

| Jinja (Airflow) | becomes |
|---|---|
| `{{ ds }}`, `{{ ds_nodash }}`, `{{ ts }}`, `{{ run_id }}` | `{{ .ds }}` etc. |
| `{{ params.project_id }}` | `{{ .params.project_id }}` |
| `{{ dag_run.conf['x'] }}` / `{{ dag_run.conf.x }}` | `{{ .conf.x }}` |
| `{{ var.value.region }}` | `{{ var "region" }}` |
| `{{ macros.ds_add(ds, -1) }}` | `{{ ds_add .ds -1 }}` |
| `{{ ti.xcom_pull(task_ids='a', key='k') }}` | `{{ xcom_pull "a" "k" }}` |

As in Airflow, a BigQuery task's `params` are merged into `{{ params }}`.

Jinja statements, operators and filters work too, so most Airflow SQL files run unchanged:

```sql
{# comments are dropped #}
{% set target = params.project_id ~ '.dw.' ~ params.table %}
INSERT INTO `{{ target }}`
SELECT {% for c in params.columns %}{{ c | lower }}{% if not loop.last %}, {% endif %}{% endfor %}
FROM `{{ params.project_id }}.raw.orders`
WHERE d BETWEEN '{{ macros.ds_add(ds, -7) }}' AND '{{ ds }}'
{% if params.env == 'prod' and dag_run.conf.get('full', false) is not true %}
  AND status = 'paid'
{% elif params.limit | default(0) | int > 0 %}
LIMIT {{ params.limit | int }}
{% endif %}
{% include 'common/where_active.sql' %}
```

- Statements: `if` / `elif` / `else`, `for` (with `else`, `loop.index`, `loop.index0`, `loop.first`, `loop.last`, `loop.length`, `for k, v in d.items()`), `set`, `include` (relative to the DAG file, `ignore missing` works), `raw`, `{# comments #}` and whitespace control (`{%-`, `-%}`).
- Expressions: `== != < <= > >=`, `and or not`, `in` / `not in`, `+ - * / // %`, `~` (concatenation), `x if c else y`, lists, `range()`, `dict()`, `.get()`, `.items()`, `.keys()`, `.values()`, `.upper()`, `.lower()`, `.strip()`, `.replace()`, `.split()`, `.startswith()`, `.endswith()`, tests `is defined / none / string / number / even / odd / divisibleby(n) / empty`.
- Filters: `upper lower trim title capitalize default (d) replace int float string length (count) join first last tojson round abs list sort unique reverse sum min max map(attribute=) select reject batch truncate indent format center wordcount striptags escape (e) quote sqlquote safe`.
- Macros: `macros.ds_add`, `macros.ds_format`, `ti.xcom_pull(task_ids=..., key=...)`, `var.value.x`, `var.json.x`.
- Inside conditions and with `default` / `is defined`, a missing key is *undefined* (falsy), like Jinja. A plain `{{ params.missing }}` still fails the task, so typos are caught.
- Not supported: `macro`, `call`, `block` / `extends`, `filter` blocks and block `set`; the task fails with a message naming the statement.

## CLI

```bash
go mod tidy && go build -o dagsflow-go .

./dagsflow-go scheduler              # foreground (Ctrl-C = graceful stop)
./dagsflow-go scheduler -D           # background, log: logs/scheduler.log
./dagsflow-go scheduler status|stop

./dagsflow-go dags list
./dagsflow-go dags pause|unpause <dag>
./dagsflow-go dags trigger <dag> --conf '{"param2": 42}'   # queued, executed by the scheduler
./dagsflow-go dags test <dag> [--conf ...]                 # run once now, without a scheduler
./dagsflow-go dags backfill <dag> --start 2026-09-01 --end 2026-09-05 [--rerun-failed]
./dagsflow-go dags list-runs <dag>
./dagsflow-go dags graph <dag> [--run-id <run>]

./dagsflow-go tasks list <dag>
./dagsflow-go tasks states <dag> <run_id>
./dagsflow-go tasks clear <dag> <run_id> -t <task> [--upstream] [--downstream] [--only-failed] [--run-now]
./dagsflow-go tasks test <dag> <task> [--xcom '{"start":{"val":10}}']
./dagsflow-go tasks logs <dag> <run_id> <task> [--try N] [-f]
```

## Web UI

```bash
./dagsflow-go users create --username admin --role Admin   # prompts for a password
./dagsflow-go standalone                 # scheduler + web UI in one process
# or separately (like Airflow):
./dagsflow-go scheduler -D
./dagsflow-go webserver --addr 127.0.0.1:8080
```

Open http://127.0.0.1:8080. Features:
- DAG list: pause/unpause, trigger (with JSON conf), last run status, next data interval.
- Grid view: runs (columns) × tasks (rows), bar height = run duration.
- Graph view: dependency graph coloured by the state of the selected run.
- Task instance details: state, attempts, error, log per attempt (auto-refresh while running),
  clear task / clear + downstream. Run details: clear failed tasks / clear the whole run.

The webserver only reads/writes metadata; execution is done by the scheduler.
Without users, auth is off and the webserver only listens on localhost. Before exposing it,
create users (see **Users & RBAC**) and put it behind HTTPS (or use `--tls-cert/--tls-key`).

## Scheduler behaviour

- A run for interval `[start, end)` is created once `end` has passed; `logical_date = start`.
- `catchup=false` (default): only the latest interval is created after downtime.
- No new runs are created while queued+running runs ≥ `max_active_runs` (default 1).
- Stopping the scheduler (executor `local`) cancels running tasks and puts the run back to `queued`;
  on the next start, tasks that already succeeded are not rerun. Orphaned runs (crash) are resumed too.
- With the `worker`/`kubernetes` executors, stopping or crashing a scheduler does **not** kill tasks:
  another scheduler (or the same one after a restart) takes the run over and re-attaches to the jobs still running.
- Paused DAGs run nothing (manual triggers included).

## Windows

dagsflow runs natively on Windows (no WSL needed):

```powershell
go build -o dagsflow-go.exe .
.\dagsflow-go.exe standalone
```

- **Bash tasks** run with Git Bash when installed (Git for Windows), so bash-style commands behave as on Linux. Otherwise `cmd` is used. Force a shell with `DAGSFLOW_SHELL=bash|pwsh|powershell|cmd|C:\path\to\bash.exe`. The shell used is written to the task log (`running (bash): ...`). WSL's `bash.exe` (`System32`) is skipped on purpose.
- **Timeouts / mark state** kill the whole process tree (`taskkill /T`), including children such as python or the bq CLI.
- **`scheduler stop`** is graceful through a stop file: running tasks are stopped and the run resumes on the next start.
- **State writes** are retried while another process has the file open (Windows refuses to replace a file that is being read).
- **Passwords** in `users create` are not echoed (cmd, PowerShell, Windows Terminal).
- CRLF line endings in bash commands are normalised.

Note: don't build a project that lives inside WSL (`\\wsl.localhost\...`) with the Windows Go toolchain; WSL shares don't support file locking (`RLock ... Incorrect function`). Put the project on a Windows drive, or build from inside WSL.

## Docker Compose

```bash
cp .env.example .env            # set DAGSFLOW_ADMIN_USERNAME / PASSWORD and POSTGRES_PASSWORD
docker compose run --rm --no-deps dagsflow-webserver config gen-key   # paste into DAGSFLOW_FERNET_KEY
docker compose up -d --build
```

Open http://localhost:8080 and sign in with the admin from `.env`.

| Service | Purpose |
|---|---|
| `postgres` | metadata database |
| `dagsflow-init` | migrates the schema, creates the admin user (idempotent), compiles Go DAGs once, then exits |
| `dagsflow-scheduler` | 2 replicas (HA): one is leader (creates runs), all execute runs |
| `dagsflow-worker` | 2 replicas: execute tasks from the queue (`--scale dagsflow-worker=N`) |
| `dagsflow-webserver` | UI + REST API on port 8080 |

- `./dags` is mounted as the DAG folder. Add or change `.go`/`.yaml` files on the host and the DAG shows up within ~30 seconds, without rebuilding the image.
- Metadata, task logs, XComs and the task queue live in Postgres, so services can be recreated or moved to other hosts without a shared volume. Compiled bundles and the Go build cache live in named volumes.
- The image contains the Go toolchain and the dagsflow-go source, needed to compile Go DAGs. The build cache is warmed in the image, so the first Go DAG compiles quickly.
- BigQuery: put a service account in `./keys` and set `GOOGLE_APPLICATION_CREDENTIALS=/opt/dagsflow-home/keys/<file>.json` in `.env`.
- More users: `docker compose exec dagsflow-webserver dagsflow-go users create --username fadhel --role Op`.
- Behind an HTTPS reverse proxy, forward `X-Forwarded-Proto: https` so the login cookie is marked Secure.

## Login & UI

The webserver has a login page (`/login`) with a session cookie (HttpOnly, SameSite=Lax, valid 12 hours, HMAC-signed with `DAGSFLOW_SECRET_KEY` or a key generated once into the metadata store). Changing a user's password or deleting the user ends their sessions. The REST API also accepts Basic auth or Bearer tokens.

The DAGs page has All/Active/Paused and Running/Failed filters, tag filter, search, sorting, pagination, Runs and Recent Tasks status circles, and Trigger / Grid / Delete actions (Delete removes run history and logs; the DAG definition stays). The owner comes from `DefaultArgs.Owner` (YAML: `default_args.owner`).

### DAG page

The layout follows Airflow 2: a header (pause toggle, Schedule, Next Run, Trigger / Reparse / Delete buttons), a filter bar (runs up to a date, run type, run state, number of runs), the grid on the left (run duration chart + status squares per task) and a details panel on the right with a DAG / Run / Task breadcrumb.

| Selection | Tabs |
|---|---|
| none | Details, Graph, Code, Event Log |
| run (click a bar) | Details, Graph, Gantt, Code, Event Log |
| task (click a task name) | Details, Graph, Task Duration, Code, Event Log |
| task instance (click a square) | Details, (Mapped Tasks), Graph, Gantt, Code, Event Log, Logs, XCom, Task Duration |

Selection and filters live in the URL, so they can be shared and survive auto-refresh. Keyboard: ←/→ change run, ↑/↓ change task, Esc clears the selection.

- **Code**: the DAG file with line numbers, Go/YAML highlighting, Toggle Wrap and *Parsed at*. DAGs in `builtin_dags/` are shown too (their source is embedded in the binary).
- **Reparse DAG** (`dag.edit`): reloads the DAG file right away instead of waiting for the next rescan. The webserver reloads immediately; schedulers pick the request up within ~2 seconds (through the metadata store, so it works across containers). CLI: `dagsflow-go dags reparse <dag_id>`. DAGs in `builtin_dags/` can't be reparsed because they are compiled into the binary.
- **Mark state as success/failed** (`dag.edit`) for a task instance (with upstream/downstream options) or a whole run. Like Airflow's `set_task_instance_state`, downstream tasks that are `failed`/`upstream_failed` are cleared and the run is queued again so it continues. If the run is executing, the request goes to the scheduler: running tasks are stopped (process group killed) and finish with the chosen state.
- **Rendered Template** (More Details page): template fields after rendering, e.g. `bash_command`, `env`, `sql` (including `sql_file` contents with `params`), `project_id`, `url`/`endpoint`, `data`, `headers`, `conf`, `filepath`. Values are recorded when the task runs (`rendered_fields` on the run). If the task hasn't run in that run yet, the template is rendered on the fly from the run's context (params, conf, XCom, mapped item), like Airflow. CLI: `dagsflow-go tasks render <dag_id> <task_id> <run_id> [--map-index N]`.
  Custom Go tasks can show up too:
  ```go
  d.NewTask("count_rows", func(ctx *dag.TaskContext) error {
      q, err := ctx.RenderTemplateField("sql") // render + record
      ...
  }, dag.WithTemplateField("sql", "SELECT COUNT(*) FROM `p.d.{{ .params.table }}` WHERE dt='{{ .ds }}'", "sql"))
  ```
  For templates built dynamically: `ctx.RenderAndRecord("name", raw)`.
- **Task instance Details tab** (like Airflow): **More Details** and **List All Instances** links, **Task Instance Notes** (Add/Edit Note; DAG Run Notes on run details), **Extra Links** (BigQuery Job Detail and BigQuery Table for BigQuery tasks), then Status / Task ID / Run ID (with copy buttons) / Map Index / Operator / Trigger Rule / times / tries / pool.
- **More Details** opens the Task Instance page with Grid · Task Instance Details (Dependencies Blocking Task From Getting Scheduled, Task Instance Attributes, Task Attributes) · Rendered Template · Log · XCom tabs.
- **List All Instances** / Browse → Task Instances: every task instance, filterable by DAG, task and state.
- Custom operators can have their own name and extra links: `dag.WithOperatorName("GcsExportOperator")`, `dag.WithExtraLink("GCS Object", "gcs_url")`, and in the task: `ctx.XComPush("gcs_url", url)`.
- **Clear & try number** follow Airflow: the try number doesn't go back to 1 after a clear but continues (attempt 2, 3, ...), and retries are counted again (`max_tries = try_number + retries`). Every attempt has its own log. For dynamic task mapping, clearing one instance (e.g. `load_branch [1]`) reruns only that instance; the other instances and their XComs stay. CLI: `tasks clear <dag> <run> -t load_branch --map-index 1`.
- **Clear task** with downstream / upstream / only failed options and a preview of the affected tasks.
- **Filter DAG by task**: show only the upstream, downstream or both of the selected task.
- **Event Log**: every action (trigger, pause, clear, mark, reparse, login, changes to users/roles/pools/variables/connections) is recorded; available per DAG/run/task and in Browse → Event Log.

### Users & RBAC

Security → **List Users** (`users.manage`): add, edit (name, email, **several roles**, active/inactive, password reset), issue API tokens, delete.

Security → **List Roles**: built-in roles (Viewer, User, Op, Admin; read-only, but they can be *copied*) and **custom roles** that can be added, edited and deleted (`roles.manage`). A role is a set of permissions. A user with several roles gets the union of their permissions.

| Permission | Meaning |
|---|---|
| `dag.read` / `dag.edit` / `dag.trigger` / `dag.delete` | view · pause/clear/mark/notes/reparse · trigger · delete runs. Scoped per DAG, see below |
| `pools.read`, `pools.edit`, `variables.read`, `variables.edit`, `connections.read`, `connections.edit` | Admin pages |
| `events.read`, `datasets.read`, `system.read` | event log, datasets, import errors + components |
| `users.manage`, `roles.manage` | manage users/tokens and roles |
| `builder.edit` | use the visual builder (writes workflow files; treat like write access to DAG code) |

DAG permissions take a *resource*:
- `*` for every DAG,
- one `dag_id`,
- a pattern such as `sales_*`,
- or a tag such as `tag:finance`.

`edit`, `trigger` and `delete` include `read`. DAGs a user can't read don't appear in the DAG list, task instances or event log. Buttons the user isn't allowed to use are hidden, and the API answers `403`.

| Built-in role | Permissions |
|---|---|
| Viewer | `dag.read:*`, pools/events/datasets/system read |
| User | Viewer + `dag.edit:*`, `dag.trigger:*` |
| Op | User + `dag.delete:*`, pools/variables/connections edit |
| Admin | Op + `users.manage`, `roles.manage`, `builder.edit` |

```bash
dagsflow-go roles create --name sales-team --description "Sales pipelines" \
  --perm 'dag.read:sales_*' --perm 'dag.trigger:sales_*' --perm 'dag.edit:tag:sales' --perm pools.read
dagsflow-go users create --username fadhel --role Viewer,sales-team
dagsflow-go users update fadhel --role sales-team
dagsflow-go roles list
dagsflow-go roles update sales-team --perm 'dag.read:sales_*'   # the new full permission list
dagsflow-go roles delete sales-team                              # only if no user has it
```
API: `GET /roles`, `GET /permissions`, `POST /roles`, `PATCH|DELETE /roles/{role}` with body `{"name":"sales-team","description":"...","permissions":["dag.read:sales_*","pools.read"]}`. Users: `{"roles":["Viewer","sales-team"]}`.

Inactive users can't sign in and their API tokens are rejected. A password reset revokes all of the user's tokens. The last active Admin can't be demoted, deactivated or deleted, and users can't delete their own account.

CLI: `users update <username> --role Op,sales-team --active=false --email x@y --reset-password`; `users create` accepts `--first-name --last-name --email`.

Other endpoints: `POST /dags/{dag}/reparse`, `GET /dags/{dag}/source`, `POST /dags/{dag}/dagRuns/{run}/mark` and `.../taskInstances/{task}/mark` (body `{"new_state":"success","include_downstream":true,"map_index":0}`), `GET /eventLogs?dag_id=&run_id=&task_id=`, `GET|PATCH /users/{user}`. `GET /dags/{dag}` accepts `runs`, `base_date`, `run_type`, `state`.

## Go DAGs without rebuilding (compiled on the fly)

Put a `.go` file with `package main` in the DAG folder. The scheduler and webserver compile it into a *DAG bundle* (a separate binary), and every task runs as a subprocess of that bundle.

```go
// dags/sales.go
package main

import "dagsflow-go/dag"

func init() {
	d := dag.NewDAG("sales", "@daily")
	d.NewTask("extract", func(ctx *dag.TaskContext) error { ctx.Logf("hi"); return nil })
	dag.Register(d)
}
```

- No `func main()` needed; it is generated.
- One file in the root of the DAG folder = one bundle. One subfolder = one multi-file bundle, so helpers can live in other files of the same folder.
- A changed file is rebuilt on the next scan (`--dag-dir-list-interval`). A deleted file removes its DAGs. Compile errors show up in **Import errors**.
- Builds are cached in `$DAGSFLOW_HOME/build/bin` per hash of the files + SDK source. A rebuild with a warm Go cache takes about 0.5 seconds.
- Tasks run in their own process. Timeouts kill the whole process group, so a panic or a stuck task doesn't bring the scheduler down.
- XComs, variables, connections, `TriggerDagRun` and `ExternalTaskState` are reached from the bundle through RPC to the host (127.0.0.1 + a per-task token). The encryption key only needs to be on the host.
- Every feature is available: branches, sensors, mapping, callbacks, pools, SLAs, datasets.

Runtime requirements: the Go toolchain and a checkout of the dagsflow-go source. Its location is detected from the working directory or the binary's location, or set with `DAGSFLOW_SDK_DIR`; `DAGSFLOW_GO` sets the path to `go`. Dependencies follow the SDK's `go.mod`. If a DAG imports other libraries, the build uses `-mod=mod` and needs GOPROXY access. Nodes without Go can still use binaries already in a shared build cache.

If the DAG folder sits inside another Go module (like `dags/` in this repo), put `//go:build dagsflow` on the first line so the repo's `go build ./...` ignores it. See `dags/features.go`.

DAGs can still be compiled into the binary (no toolchain on the server): put them in `builtin_dags/`, which `main.go` imports.

## YAML DAGs (deploy without rebuilding)

Put `*.yaml` files in the DAG folder (`$DAGSFLOW_DAGS_FOLDER`, default `$DAGSFLOW_HOME/dags`). The scheduler and webserver rescan it every `--dag-dir-list-interval` (default 30s): new/changed files are loaded, deleted files are unregistered, broken files show up under **Import errors**.

```yaml
dag_id: yaml_example
schedule: "@daily"          # or schedule_datasets: [bq://proj.ds.table]
default_args: {retries: 2, retry_delay: 10s, on_failure: [gchat_alerts]}
tasks:
  - id: list_files
    operator: bash
    command: echo '["a.csv","b.csv"]'      # last stdout line -> XCom return_value
  - id: process_file
    operator: bash
    command: echo "{{ .item }} {{ .ds }}"
    expand: {xcom: {task: list_files}}     # dynamic task mapping
    pool: bigquery
  - id: load
    operator: bigquery
    sql_file: sql/load.sql                 # relative to the YAML file
    upstream: [process_file]
    outlets: [bq://proj.dw.sales]
```

Operators: `empty`, `bash`, `go`, `bigquery`, `http`, `postgres`, `transform`, `branch`, `bigquery_query`, `bigquery_insert`, `gsheet_read`, `gsheet_write`, `file_read`, `merge`, `gcs_write`, `s3_write`, `bigquery_external_table`, `bigquery_load_table`, `bigquery_delete_table`, `trigger_dagrun`, `file_sensor`, `time_delta_sensor`, `external_task_sensor`, `http_sensor`, `custom`. Custom logic needs a Go DAG (or a `go` / `bash` task).

ETL operators (rows travel between tasks as the XCom `return_value`, a JSON array of objects):

```yaml
tasks:
  - id: fetch
    operator: http
    url: "https://api.example.com/orders?date={{ .ds }}"
  - id: shape
    operator: transform                # no-code steps, run top to bottom
    upstream: [fetch]                  # reads the only upstream task (or set `input: fetch`)
    steps:
      - {op: extract, path: data}      # dot path into the JSON response
      - {op: filter, field: status, cmp: "==", value: paid}
      - {op: set, field: loaded_at, value: "{{ .ds }}"}   # {{ .row.name }} works too
      - {op: select, fields: [id, total, loaded_at]}
  - id: load
    operator: postgres                 # table = insert the upstream rows (one transaction)
    conn_id: warehouse
    table: public.orders
    on_conflict: do_nothing            # optional
    upstream: [shape]
  - id: check
    operator: postgres                 # sql = run a statement, returned rows are the output
    conn_id: warehouse
    sql: "SELECT count(*) AS n FROM public.orders"
    limit: 10000                       # optional cap on rows kept (default: no limit)
    upstream: [load]
```

`transform` steps: `extract`, `select`, `drop`, `rename`, `filter` (`== != > >= < <= contains exists not_exists`), `set`, `sort`, `limit`, `dedupe`, `snake_case` (rename every column to lowercase letters and `_`). A Postgres connection holds either `uri` (`postgres://user:pass@host/db`) or `host`, `port`, `login`, `password`, `database`, `sslmode`.

HTTP responses: the body becomes the output up to `max_bytes` (default 10 MB, or `DAGSFLOW_HTTP_MAX_BYTES`). A bigger response fails the task with a clear error instead of being cut. For bulk exports, `save_to` streams the response of any size to a file and the output becomes `{path, bytes, status_code}`; a bare file name goes to the builder's uploads folder, so a `file_read` task can read it (CSV, Excel, JSON or NDJSON):

```yaml
  - id: download
    operator: http
    url: "https://api.example.com/export?date={{ .ds }}"
    save_to: "orders_{{ .ds }}.json"   # streamed, no size limit (2 h default timeout)
  - id: orders
    operator: file_read
    file: "orders_{{ .ds }}.json"      # a JSON array of objects or NDJSON; keys are kept as they are
    upstream: [download]
```

Branching without Go (`branch`, the builder's **If** node): check a value, run the tasks in `then` when it holds and the ones in `else` otherwise; the other side is skipped (like `BranchPythonOperator`):

```yaml
  - id: has_rows
    operator: branch
    upstream: [orders]
    path: length            # a dotted path into the upstream output; "length" = number of rows / keys / characters
    op: ">"                 # == != > >= < <= contains not_contains empty not_empty (default) exists not_exists
    value: 0                # templates work
    then: [load]
    else: [notify_empty]
  # or check a template instead of an upstream output:
  #   left: "{{ .params.mode }}"
  - id: report              # joins both paths
    upstream: [load, notify_empty]
    trigger_rule: none_failed_min_one_success
```

The output of a `branch` task is `true` or `false`. Text from a shell or an HTTP body is trimmed before comparing. Numbers compare as numbers when both sides parse as numbers.

Files and combining sources:

```yaml
  - id: sales
    operator: file_read                # CSV / TSV / Excel (.xlsx) / JSON / NDJSON
    file: sales_jan.xlsx               # an upload from the builder, or a path on the worker ({{ .ds }} works)
    sheet: Data Jan                    # name or number; default: first sheet
    range: B3:F                        # A1 notation: header on row 3, columns B to F, to the last row
  - id: customers
    operator: file_read
    file: customers.csv                # delimiter is detected (, ; tab |); override with delimiter:
  - id: both
    operator: merge                    # mode: union (append rows, default) | join (exactly two inputs)
    mode: join
    join_type: left                    # inner (default) | left
    on: [customer_id]                  # or [left_col=right_col]
    upstream: [sales, customers]       # the first input is the left side
```

Header names: `gsheet_read` and `file_read` turn the header row into column names in **snake_case, lowercase letters a-z and `_` only** (`Total (Rp)` becomes `total_rp`, `orderID` becomes `order_id`; accents are dropped, and digits and punctuation act as word separators, so `Qty 1` becomes `qty`). Names that end up equal get a letter suffix (`name`, `name_b`, `name_c`). Set `keep_headers: true` to keep the original text. `rename: {old_name: new name}` then renames columns (the names after snake_case, or the original ones with `keep_headers`); in the builder, "Rename columns" lists the columns of the data after a "Run node" (or the last run), with a box for the new name next to each. For other sources use the `snake_case` transform step. Empty cells become `null`; fully blank lines are skipped. Excel dates come out as `2024-01-31` (or `2024-01-31 14:30:00`), numbers as numbers; CSV values stay text. `.xls` (old format) and password protected files are not supported; uploads are limited to 25 MB.

Dump rows to object storage (a file per run, plain REST, no SDK):

```yaml
  - id: dump_gcs
    operator: gcs_write                # Google Cloud Storage
    bucket: my-bucket
    object: exports/orders/{{ .ds }}/orders   # templated; .csv (or .json / .ndjson, +.gz) is added when missing
    format: csv                        # csv (default) | tsv | json | ndjson
    gzip: true
    upstream: [read_bq]
  - id: dump_s3
    operator: s3_write                 # Amazon S3, MinIO, Cloudflare R2, …
    bucket: my-bucket
    key: exports/orders/{{ .ds }}/orders.ndjson
    region: ap-southeast-3             # optional, default from the connection, else us-east-1
    format: ndjson
    upstream: [read_bq]
```

`gcs_write` uses the same service account as BigQuery and Sheets (it needs write access to the bucket). `s3_write` reads the `aws_default` connection (or `conn_id`) with `access_key_id`, `secret_access_key`, optional `session_token`, `region` and `endpoint_url` (for MinIO or R2; those use path style URLs), else the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` / `AWS_REGION` / `AWS_ENDPOINT_URL_S3` environment variables. Without keys it uses the IAM role of where it runs, in the AWS SDK order: web identity (EKS IRSA: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`), container credentials (ECS task role, EKS Pod Identity), then the EC2 instance profile through IMDSv2 (`AWS_EC2_METADATA_DISABLED=true` skips it). A `role_arn` (and optional `external_id`) on the connection is assumed with STS on top of those credentials, for cross-account buckets. Temporary credentials are cached until shortly before they expire. The task output is `{uri, rows, bytes, format, columns, gzip, delimiter}` (`columns` lists each column with a type guessed from the data). CSV columns default to all fields, sorted; set `columns: [...]` for an order. The whole file is built in memory and uploaded in one request, so it suits files up to a few hundred MB. Both are tested against fake servers (S3 signing also against the examples published by AWS), not yet against real GCS or S3.

Expose the dumped file to BigQuery as an external table, with exactly the column names of the file:

```yaml
  - id: ext
    operator: bigquery_external_table
    table: 7r_staging.testing_fadhel   # or project.dataset.table; CREATE OR REPLACE on every run
    default_type: STRING               # STRING (default) for every column | auto: from the data
    column_types: {amount: NUMERIC, order_date: DATE}   # optional overrides
    all_files: true                    # uris = the folder of the file + /* (e.g. one file per day)
    expiration: 7d                     # optional: 7d, 12h, 90m, 2w (a bare number = days) or a date; counts from each run
    upstream: [dump_gcs]               # reads uri, format and columns from the gcs_write output
```

`auto` gives INT64, FLOAT64, BOOL, JSON (ndjson only) or STRING from the values that were written. In the builder, "Type per column" lists the columns of the file (from the GCS node's last output, or by running the node that feeds it) with a type picker for each; leave a column on (default) to use `default_type`. CSV/TSV files get `skip_leading_rows = 1`, the delimiter and `compression = 'GZIP'` when gzipped. `format: json` (one array) can't be read by BigQuery: use `ndjson` or `csv`. `if_not_exists: true` keeps an existing table. Without a `gcs_write` before it, set `uris`, `format` and `columns` (all STRING) yourself.

Then load it into a real table, with metadata columns (all inside BigQuery, no rows pass through dagsflow):

```yaml
  - id: load
    operator: bigquery_load_table
    table: dw.orders                   # created on the first run
    mode: merge                        # append (default, historical) | replace (latest load only) | merge
    merge_keys: [order_id]             # merge: update matching rows, insert the others
    when_matched: update               # or skip: insert new keys only
    conditions:                        # optional filters on the source rows (templated values)
      - {column: status, op: "=", value: paid}
      - {column: amount, op: ">", value: "100000"}
    condition_mode: all                # all (AND) | any (OR)
    metadata: [path, inserted_at, job_id, job_date]   # default: all four
    metadata_prefix: metaingest_       # default metadata_
    metadata_names: {inserted_at: ingest_at}          # full names for some of them
    inserted_at: utc                   # utc (TIMESTAMP, default) | local (DATETIME)
    job_date: local                    # local (default) | utc
    timezone: Asia/Jakarta             # "local"; default: the server's time zone
    column_map:                        # optional, per source column: new name and/or conversion
      order_id: {name: id}
      amount: {name: total, cast: NUMERIC}                 # a BigQuery type
      tgl: {name: order_date, cast: "my_project.utils.to_date({col})"}   # your own SQL; {col} = the column
      paid: {cast: my_project.utils.to_bool}               # a bare function gets the column as argument
    cast_mode: safe                    # safe (default): SAFE_CAST, bad values become NULL | strict: CAST, fails
    expiration: 90d                    # optional: delete the table 90 days after the last load (or a date: 2026-12-31)
    partition_expiration_days: 365     # optional: drop partitions older than a year (needs partition_by)
    partition_by: metaingest_job_date  # optional DATE / DATETIME / TIMESTAMP column; cluster_by: [...]
    upstream: [ext]                    # the external table (or set source_table: dataset.table)
```

The metadata columns: `path` is the `gs://` file each row came from (`_FILE_NAME` of the external table), `inserted_at` the load time, `job_id` the load time as an integer (`20261021191023`, local time), `job_date` the load date. The source columns are read from `INFORMATION_SCHEMA`, so they follow the data: a column that appears later is added to the table (`ADD COLUMN IF NOT EXISTS`) before the insert or merge. Condition ops: `= != > >= < <= contains not_contains in not_in is_null not_null is_empty not_empty`; `> >= < <=` compare as numbers when the value is a number, else as text. Merge keys, `partition_by` and `cluster_by` use the names after `column_map`; conditions use the source names. The type of a column converted with your own SQL comes from a BigQuery dry run of the query, which also checks the SQL before anything is written. A merge keeps one source row per key (duplicates in the source would make MERGE fail). In the builder the merge keys, the condition columns and the partition column are picked from the columns of the data. Tested against a fake BigQuery, not yet a real one.

Delete tables (tables, views or external tables, through the BigQuery API):

```yaml
  - id: cleanup
    operator: bigquery_delete_table
    tables:                            # dataset.table or project.dataset.table, templated
      - 7r_staging.orders_ext
      - staging.orders_{{ .ds_nodash }}
    fail_if_missing: false             # default: a table that isn't there is skipped
    upstream: [load]
```

BigQuery and Google Sheets as source or destination (plain REST, no extra dependencies):

```yaml
  - id: read_bq
    operator: bigquery_query           # rows -> JSON array (no row limit unless limit: is set)
    sql: SELECT id, name FROM `proj.ds.users` WHERE d = '{{ .ds }}'
  - id: to_sheet
    operator: gsheet_write             # write_mode: append (default) | overwrite
    spreadsheet_id: https://docs.google.com/spreadsheets/d/<id>/edit
    range: Sheet1
    upstream: [read_bq]
  - id: from_sheet
    operator: gsheet_read              # first row of the range = header; no_header: true for col_1, col_2…
    spreadsheet_id: <id>
    range: Sheet1!A1:F
  - id: to_bq
    operator: bigquery_insert          # streaming insert into an existing table
    dataset: ds
    table: users
    upstream: [from_sheet]
```

Credentials come from the `google_cloud_default` connection (or `conn_id`): `credentials_path` or `credentials_json` of a service account, else `GOOGLE_APPLICATION_CREDENTIALS`, else the GCE/GKE metadata server. Share the sheet with the service account's email. These four operators are covered by tests against a fake Google API, not yet against a real GCP project.

## Visual workflow builder (drag and drop)

![Visual workflow builder: drag nodes, connect, configure, run; BigQuery to Google Sheets](docs/builder-demo.gif)

**Builder** in the top menu (permission `builder.edit`, Admin by default). Drag nodes from the palette onto the canvas (or click one to add it after the selected node), drag from a node's right port to another node to connect them, and fill in the side panel. Dropping a connection on an empty spot opens a node picker.

- **Nodes**: HTTP request, Postgres query / insert, BigQuery read rows / insert rows / SQL, Google Sheets read / write, CSV / Excel / JSON file (upload, sheet, start row and columns, with a preview), Transform rows, Merge / join, Google Cloud Storage dump, Amazon S3 dump, Shell command, Go code, If / branch, Trigger another DAG, No-op, and four sensors. Every node also has retries, timeout, trigger rule, pool, dataset outlets, failure alerts and *run once per item* (dynamic mapping).
- **Live validation**: missing fields, bad durations, loops and unknown connections are flagged on the node while you edit; saving is refused while there are errors.
- **Save & Run** saves, triggers a run and colours the nodes by state; select a node to see its output rows, error and log.
- **Run node** (side panel) runs only the selected node right away, without saving, and shows its output as a table (or JSON / text), its log and how long it took, so you can preview a source or check a transform before running the workflow. Its input comes from earlier *Run node* results in the editor, otherwise from the latest run of the saved workflow, otherwise the read-only nodes before it (sources, transform, merge, If, GET requests) are run too. A node before it that may change data (a shell command, a load, a BigQuery job) is never run automatically: run it first. Nothing is stored, *run once per item* nodes preview their first item, sensors check once, and a preview stops after 2 minutes. Nodes that write data ask for confirmation. It needs `dag.edit` and `dag.trigger` on the workflow. It runs where the workflow's tasks run: when a worker serving the `default` queue is alive (`--executor worker`), the preview goes through the task queue to a worker (the result says which one), otherwise it runs on the webserver. `DAGSFLOW_PREVIEW_ON=webserver` or `worker` forces one side. Kubernetes pods only run real task instances, so with that executor the preview runs on the webserver.
- **If / branch**: connect the nodes that run when the condition holds and the ones that run otherwise; each connection leaving an If shows **true** or **false** (click the label, or select the connection, to switch it). A node after the If that joins both paths gets the trigger rule *none failed, min one success* automatically.
- **Undo/redo**, auto-arrange, fit view, duplicate (`Ctrl+D`), delete, `Ctrl+S`. Unsaved edits survive an accidental reload (kept in the browser) and leaving the page asks first.
- Workflows are saved as ordinary YAML DAGs in `<dags folder>/builder/<id>.yaml` (node positions in a `layout:` block), so the scheduler, executors, RBAC, Grid and Graph views treat them like any other DAG, and you can edit the YAML by hand. New scheduled workflows start **paused**: switch them on in the builder.
- **Several sources and loads**: connect as many nodes as you like; one node can feed several destinations. To use two sources together, connect both to a **Merge / join** node (union or a two-way join). Other nodes read one input, chosen under *Read rows from* when several are connected.
- Hand-written YAML DAGs open read-only (**Open in builder** on the DAG page); **Make an editable copy** saves a builder copy under a new id. The builder edits one DAG per file and cannot open Go DAGs.
- **Run actions per node**: after a run, select a node for **Re-run** (clears the task so the scheduler runs it again), **Mark success** / **Mark failed** and **Show log**, with *downstream / upstream / only failed* options. The run summary (no node selected) has **Re-run failed** and **Mark run success / failed**. They use the normal clear/mark APIs, so they need `dag.edit` on the DAG and are hidden in read-only workflows.
- **Custom nodes**: see below.
- Saving needs `builder.edit` plus `dag.edit` on that DAG id. Two people editing the same workflow: the second save is refused (`409`) instead of overwriting.

Uploaded files are kept in `<dags folder>/builder/files/` (so that folder must be shared in a multi-host setup, like the workflows). Security note: a workflow can run shell commands and SQL on your workers, read any file the worker can read through the file node, and so `builder.edit` is as powerful as write access to DAG files. Keep it for people you would trust with that. In a multi-host setup the webserver writes into the DAG folder, so it must be a volume shared with the scheduler and workers (new or changed files are picked up within ~2 s).

### Running Go code from the builder

The **Go code** node (`operator: go`) runs a standalone Go program (`package main` with `func main`, standard library only) with `go run`, no rebuild of dagsflow needed. The worker needs the Go toolchain (`DAGSFLOW_GO` sets the path to `go`). Like the shell node, the last stdout line is the node's output, `DAGSFLOW_CTX_DAG_ID / TASK_ID / RUN_ID / DS` are set, and `env` values are templated, so `{{ xcom_pull "extract" "return_value" }}` hands data in via `os.Getenv`. Compile errors and panics show in the task log. It is as powerful as the shell node. In **Create node** the Go program (or shell command, or SQL) has its own code editor; only the other settings stay as small JSON.

In **Create node**, the *Runs as* list shows plain names (Shell command, Go code, SQL: Postgres, SQL: BigQuery...), and picking one fills in a starter for its settings. So a custom node can wrap Go code or SQL with `${field}` placeholders.

### Custom nodes (YAML, UI or Go)

A custom node is your own palette entry. All three ways feed the same registry and appear under **Custom**.

**1. YAML**: drop a file in `<dags folder>/builder/nodes/<type>.yaml` (picked up within ~2 s, no restart):

```yaml
type: slack_message
label: Slack message
description: Post a message to a Slack webhook
operator: http            # the built-in operator it runs as
fields:                   # the form the user fills in
  - {name: text, label: Message, kind: text, required: true}
  - {name: channel, label: Channel, kind: text}
template:                 # operator settings; ${field} is replaced by the node's value
  method: POST
  url: https://hooks.slack.com/services/XXX
  body: '{"text": "${text}", "channel": "${channel}"}'
```

A value that is exactly `${field}` keeps its type (number, list) and an empty one drops the key; unknown names such as a shell `${HOME}` are left alone. Field kinds: `text`, `code`, `number`, `bool`, `select`, `list`, `kv`, `json`, `conn`.

**2. UI**: **+ Create node** under the palette (or *edit definition* on a custom node) writes that same file. Deleting a node opens a dialog listing every workflow that uses it; if you confirm, those nodes (and their connections) are removed from all of them, and a workflow left with no other node is deleted. Over the API, `DELETE` answers `409` with the list unless you pass `?force=1`; `GET /builder/custom-nodes/{type}/usage` returns the list.

**3. Go**: register a type with real code before the scheduler / webserver start (for example in `main.go`):

```go
dag.RegisterNodeType(dag.NodeType{
    Type: "go_ping", Label: "Ping host",
    Fields: []dag.NodeField{{Name: "host", Label: "Host", Kind: "text", Required: true}},
}, func(d *dag.DAG, id string, with map[string]any, opts ...dag.TaskOption) error {
    d.NewTask(id, func(ctx *dag.TaskContext) error { /* ping with["host"] */ return nil }, opts...)
    return nil
})
```

In a workflow file the node is stored as `operator: custom`, `node: <type>`, `with: {...}`, plus the usual common settings (retries, timeout...). If a workflow uses a type that is not defined any more (YAML file removed, or no longer registered in Go), the DAG shows an import error naming it, and the builder still opens the workflow with that node marked red as missing: delete it and save. YAML and UI nodes are as powerful as the operator they wrap (a `bash` template runs shell commands), so treat `builder.edit` accordingly.

API (all need `builder.edit`): `GET|PUT|DELETE /builder/custom-nodes/{type}`, `GET /builder/nodes`, `GET /builder/workflows`, `GET|PUT|DELETE /builder/workflows/{dag}`, `POST /builder/validate`, `POST /builder/run-node` (`{"graph": {...}, "node": "id", "outputs": {"node": value}}`). A workflow is `{"dag_id", "settings": {schedule, tags, ...}, "nodes": [{id, type, x, y, config}], "edges": [{from, to, branch}]}` (`branch` is `true` / `false` on connections leaving an If node).

## More Airflow features

| Feature | Go | YAML |
|---|---|---|
| Pools | `dag.WithPool("bigquery")`, `WithPoolSlots(n)`; `dagsflow-go pools set bigquery 4` | `pool:` |
| Priority weight | `dag.WithPriorityWeight(n)` (weight_rule = downstream) | `priority_weight:` |
| Sensors | `d.NewSensor(id, pokeFn, dag.SensorConfig{PokeInterval, Timeout, Mode: dag.ModeReschedule, SoftFail})`, `NewFileSensor`, `NewTimeDeltaSensor`, `NewExternalTaskSensor`, `NewHTTPSensor` | `*_sensor`, `mode: reschedule` |
| Dynamic task mapping | `dag.Expand(dag.FromXCom("t","return_value"))` / `dag.FromItems(...)`; in the task: `ctx.Item`, `dag.ItemAs[T](ctx)`; pulling from a mapped task → list | `expand:` |
| Datasets | `dag.WithOutlets(uri)` on a task, `dag.WithDatasetSchedule(uri...)` on a DAG | `outlets:`, `schedule_datasets:` |
| SLA | `dag.WithSLA(d)` + `dag.WithSLAMiss(cb)` (deadline = data_interval_end + sla, scheduled runs only) | `sla:`, `on_sla_miss:` |
| Callbacks | task: `WithOnFailureCallback/OnSuccess/OnRetry`, DAG: `WithOnFailure/WithOnSuccess`, or `DefaultArgs.OnFailure` | `on_failure: [conn_id]` |
| Queues / pod overrides | `dag.WithQueue("heavy")`, `dag.WithPodOverride(map[string]any{...})` | `queue:`, `pod_override:` |

`mode: reschedule` frees the executor/pool slot between pokes (similar to deferrable operators).

Dataset event metadata (≈ `outlet_events[...].extra` / `triggering_dataset_events` in Airflow 2.10+):

```go
// producer: attach metadata to the dataset event
return ctx.SetOutletExtra("bq://proj.dw.sales", map[string]any{"rows": total})
// consumer (a DAG scheduled on that dataset)
info, ok, err := dag.DatasetExtraAs[salesInfo](ctx, "bq://proj.dw.sales") // extra of the latest event
events, _ := ctx.TriggeringDatasetEvents()                                 // every triggering event
```

### Google Chat alerts

```bash
dagsflow-go connections add gchat_alerts --conn-type google_chat \
  --config '{"webhook_url":"https://chat.googleapis.com/v1/spaces/.../messages?key=...&token=..."}'
export DAGSFLOW_BASE_URL=https://dagsflow.internal   # "Open in dagsflow" button in the card
```
```go
alert := dag.GoogleChatNotifier("gchat_alerts")
d := dag.NewDAG("x", "@daily", dag.WithOnFailure(alert),
    dag.WithDefaultArgs(dag.DefaultArgs{OnFailure: []dag.Callback{alert}}))
```
Custom callbacks: `func(ctx context.Context, cc dag.CallbackContext) error`.

## Variables, connections & secrets

```bash
export DAGSFLOW_FERNET_KEY=$(dagsflow-go config gen-key)   # AES-256-GCM; without it values are stored in plain text (with a warning)
dagsflow-go variables set region APAC
dagsflow-go variables import vars.json
dagsflow-go connections add my_api --conn-type http --config '{"host":"https://api.x","token":"..."}'
```
In tasks: `dag.GetVariable("region")`, `dag.GetVariableJSON[T]`, template `{{ var "region" }}`, `dag.GetConnection(id)`.

Lookup order (secrets backends): env (`DAGSFLOW_VAR_REGION`, `DAGSFLOW_CONN_MY_API` holding JSON) → encrypted metastore → `connections/*.json`. Other backends (e.g. GCP Secret Manager) only need to implement `dag.SecretsBackend` and be passed to `dag.SetSecretsBackends(...)`.

## REST API

```bash
dagsflow-go users create --username fadhel --role Admin   # Viewer | User | Op | Admin | a custom role (see Users & RBAC)
dagsflow-go users token fadhel                            # API token (shown once)
```
Without users, auth is off and the webserver only listens on localhost.

Auth: HTTP Basic or `Authorization: Bearer <token>`. Non-GET requests with Basic auth need the header `X-Dagsflow: 1` (CSRF).

```bash
H="Authorization: Bearer $TOKEN"; B=http://localhost:8080/api/v1
curl -H "$H" $B/dags
curl -H "$H" -X POST $B/dags/custom_dag/dagRuns -d '{"conf":{"val":70}}'
curl -H "$H" $B/dags/custom_dag/dagRuns/<run_id>/taskInstances
curl -H "$H" "$B/dags/custom_dag/dagRuns/<run_id>/taskInstances/<task>/logs/1?map_index=0"
curl -H "$H" -X PATCH $B/dags/custom_dag -d '{"is_paused":true}'
```
Other endpoints: `dagRuns/{run}/clear`, `DELETE dagRuns/{run}`, `pools`, `variables`, `connections` (sensitive values masked), `datasets`, `importErrors`, `components`, `users`, `users/{u}/tokens`, `roles`, `health`, `me`.

## Production

Use **Postgres** as the metadata database in production. Without `DAGSFLOW_DATABASE_URL`, dagsflow uses JSON files (fine for a laptop or a single machine).

With the JSON files, a second scheduler on the same machine (or on a shared folder with working file locks) starts as a **hot standby**: it waits on the lock and takes over within ~2 seconds when the active scheduler stops or dies, continuing its running DagRuns. Several *active* schedulers, workers on other machines and Kubernetes pods share state over the network, so they need Postgres.

```bash
export DAGSFLOW_DATABASE_URL='postgres://dagsflow:***@db:5432/dagsflow?sslmode=require'   # or DAGSFLOW_DATABASE_URL_FILE
dagsflow-go db migrate                      # also done automatically on start
dagsflow-go scheduler --executor worker     # run 2+ on different machines
dagsflow-go worker -c 8 -q default          # as many as needed
dagsflow-go webserver --addr 0.0.0.0:8080   # several replicas behind a load balancer are fine
```

**HA schedulers**
- Several schedulers can run at once. One is the *leader* (15-second lease): it creates scheduled/dataset runs, adopts orphaned runs and runs retention. Every scheduler executes runs.
- Runs are *claimed* atomically (`queued → running`), so no run is executed twice. `max_active_runs` and pools apply across all schedulers.
- A running run is owned by one scheduler with a 30-second lease renewed every 5 seconds. If that scheduler dies, the leader re-queues its runs and another scheduler continues them. An old owner that turns out to be alive (e.g. after being paused) stops writing immediately (fencing).
- Writes from webservers/CLI use optimistic concurrency: if the run changed elsewhere, the API answers `409` and the request can simply be retried.

**Executors**

| `--executor` | Tasks run in | Notes |
|---|---|---|
| `local` (default) | the scheduler process | like the LocalExecutor |
| `worker` | `dagsflow-go worker` processes | like the CeleryExecutor, queue in Postgres. Routing: `dag.WithQueue("heavy")` / `queue:` in YAML, then `worker -q heavy` |
| `kubernetes` | one pod per task | like the KubernetesExecutor. See `deploy/kubernetes/dagsflow.yaml`. Per task: `dag.WithPodOverride(...)` / `pod_override:` in YAML |

- Workers need the same DAG folder and connections as the schedulers. XComs, rendered templates, dataset extras and logs go through the database, so no shared volume is needed.
- A dead worker (no heartbeat for 60 seconds) fails the attempt, and `retries` apply. Pods that fail to start (ImagePullBackOff, OOMKilled, ...) fail the attempt too instead of hanging.
- `worker --grace 10m`: on shutdown, running tasks get this long to finish.
- Sensors on workers/pods always run in `poke` mode.

**Observability**
- `GET /metrics` (webserver; scheduler and worker via `--metrics-addr :9102`) in Prometheus format: run/task durations and counts per state, running/queued tasks, pool usage, live components, queue depth, import errors and leader status. On the webserver it can require a token (`DAGSFLOW_METRICS_TOKEN`) or be turned off (`DAGSFLOW_METRICS=off`).
- `GET /health` (liveness), `GET /ready` (database reachable), `GET /api/v1/health` (metadatabase + scheduler status).
- Admin → **Components** (or `dagsflow-go db check`) lists schedulers, webservers and workers with their heartbeats.
- `DAGSFLOW_LOG_FORMAT=json`: component logs as one JSON object per line (Cloud Logging, Loki, ELK).

**Security**
- HTTPS directly: `webserver --tls-cert cert.pem --tls-key key.pem`. Behind a proxy, forward `X-Forwarded-Proto`.
- Login and Basic auth are locked for 15 minutes after 5 failures per username/IP. The failures are kept in the metadata database (hashed, a row-locked update on Postgres), so the lockout holds across every webserver replica; each process also keeps its own count in case the database is unreachable. `X-Forwarded-For` is only trusted with `DAGSFLOW_TRUST_PROXY_HEADERS=true`.
- Security headers: CSP, HSTS (over HTTPS), X-Frame-Options, nosniff. Requests that change data need the CSRF header (`X-Dagsflow`) or a Bearer token. The server has timeouts and body size limits.
- Set `DAGSFLOW_FERNET_KEY` (encrypted variables/connections) and `DAGSFLOW_SECRET_KEY` (the same on every webserver replica). If `SECRET_KEY` is empty, a key is generated once and stored in the metadata database.

**Operations**
- Retention: `--retention-days 90` / `DAGSFLOW_RETENTION_DAYS` (run by the leader every hour), or manually `dagsflow-go db clean --older-than 90d --yes`.
- Backup/restore: `dagsflow-go db export -f backup.json` / `db import -f backup.json`. To move from JSON files to Postgres, run `db export` without `DAGSFLOW_DATABASE_URL`, then `db import` with it set. Task logs are not exported. For regular Postgres backups use `pg_dump` / managed database snapshots.
- Upgrades: the schema is migrated automatically on start, under a lock so several processes starting together are safe. An older binary refuses to run against a newer schema. Rolling restarts of schedulers are safe (see HA above).

## Benchmark vs Airflow

A small benchmark of **orchestration overhead** against Apache Airflow 2.10.5 (October 2026). Scripts and how to reproduce: [`bench/`](bench/README.md).

**Setup.** One machine with 2 vCPU and 8 GB RAM. Both systems use Postgres 16 as the metadata database, parallelism 16 and no example DAGs. Airflow runs `scheduler` + `webserver` with the LocalExecutor and default settings. dagsflow runs `standalone` (scheduler + web UI in one process). Every task does nothing (`true` for bash, `return nil` for Go), so the numbers show what the orchestrator itself costs. Each run was repeated 3 times and the median is reported. RAM is the PSS of all processes of the service.

| Metric | Airflow 2.10.5 | dagsflow, YAML DAGs | dagsflow, Go DAGs |
|---|---|---|---|
| `chain50`: 50 sequential tasks | 55.9 s | 1.06 s | 1.05 s |
| `fan200`: 200 independent tasks | 81.5 s | 3.0 s | 1.9 s |
| Startup until `/health` is OK | 4.4 s | 0.1 s | 1.3 s (2 bundles compiled) |
| RAM, idle | 519 MB | 18 MB | 18 MB |
| RAM, peak during `fan200` | 1,671 MB | 58 MB | 64 MB |
| 1,002 DAG files: first start until all are listed | 159 s | 0.3 s | 543 s |
| 1,002 DAG files: restart with the bundle cache | n/a | n/a | 11.8 s |
| 1,002 DAG files: `chain50` | 67.0 s | 1.1 s | 1.05 s |
| 1,002 DAG files: RAM, idle | 720 MB | 27 MB | 36 MB |

What this does and does not show:

- **It measures overhead, not work.** Airflow spends about 1.1 s per task (every task starts a new Python process), dagsflow about 20 ms. For tasks that run for minutes, such as a BigQuery query, the difference hardly matters.
- **Go DAG files are compiled one by one.** Every `.go` file in the root of the DAG folder becomes its own bundle (about 0.5 s and 6.7 MB each), so 1,000 separate files take about 9 minutes on the first start and 6.7 GB of disk. A restart reuses the cache. For many Go DAGs, put them in one subfolder (one bundle) instead; that layout was not measured here.
- **YAML vs Python parsing is not the same work.** dagsflow reads YAML, while Airflow executes every Python file.
- **Airflow was not tuned**, and Airflow 3, the CeleryExecutor and bigger machines were not tested. The numbers only hold for this machine.
- The dagsflow binary was built without `builtin_dags` (the same as `load_examples=False` in Airflow); everything else is the normal code.

## Current limitations

- With the JSON store one scheduler is active at a time (others wait as standbys); several active schedulers, workers on other machines and pods share state over the network and need Postgres.
- The builder edits YAML workflows; Go DAGs are shown read-only in the DAG views but can't be opened in the builder (they are code).
- GCS and S3 uploads are tested against fake servers (S3 signing also against the examples published by AWS), not yet against real buckets.

## Connections

`connections/*.json` (or `GOOGLE_APPLICATION_CREDENTIALS` + `GOOGLE_CLOUD_PROJECT` → `google_cloud_default`):

```json
{ "google_cloud_default": { "Type": "google_cloud_platform",
  "Config": { "project_id": "my-project", "credentials_path": "/path/sa.json", "location": "asia-southeast2" } } }
```

`NewBigQueryTask` uses the `gcp_conn_id` param (default `google_cloud_default`).
All data (state, logs, connections) is relative to `$DAGSFLOW_HOME` (default: the working directory).
