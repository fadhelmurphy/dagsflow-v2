# dagsflow-go

Workflow orchestrator berbasis Go dengan model eksekusi ala **Apache Airflow**:
satu scheduler untuk semua DAG, setiap eksekusi adalah **DagRun** dengan **TaskInstance**
yang state-nya dipersist, sehingga bisa di-rerun/clear/backfill dan resume setelah restart.

## Konsep (padanan Airflow)

| Airflow                          | dagsflow-go                                       |
|----------------------------------|---------------------------------------------------|
| DAG file / DagBag                | DAG folder (`dags/`): file `.go` di-compile otomatis, `.yaml`; atau `builtin_dags/` yang ikut ter-compile di binary |
| `airflow scheduler`              | `dagsflow-go scheduler [-D]`                       |
| Metadata DB                      | `$DAGSFLOW_HOME/state/` (JSON)                     |
| DagRun / TaskInstance            | `engine.DagRun` / `engine.TaskInstance`            |
| logical_date, data_interval      | `ctx.LogicalDate`, `ctx.DataIntervalStart/End`     |
| catchup, max_active_runs, start_date | `dag.WithCatchup`, `dag.WithMaxActiveRuns`, `dag.WithStartDate` |
| default_args (retries, retry_delay, execution_timeout) | `dag.WithDefaultArgs` / `dag.WithRetries`, `dag.WithTimeout` |
| params + dag_run.conf            | `dag.WithParams`, `--conf`, `dag.ParamAs[T]`       |
| XCom (per task, per run)         | `ctx.XComPush`, `dag.XComPullAs[T](ctx, task, key)`|
| PythonOperator / BranchPythonOperator / EmptyOperator | `NewTask` / `NewBranchTask` / `NewEmptyTask` |
| TriggerDagRunOperator            | `NewTriggerDagRunTask` (wait_for_completion)       |
| BigQueryInsertJobOperator        | `NewBigQueryTask` (SQL di-template)                |
| Jinja `{{ ds }}`                 | Go template `{{ .ds }}`, `{{ xcom_pull "t" "k" }}` |
| Trigger rules                    | all_success, all_failed, all_done, one_success, one_failed, none_failed, none_failed_min_one_success, none_skipped, always |
| Task log per attempt             | `logs/dag_id=…/run_id=…/task_id=…/attempt=N.log`   |

## Contoh DAG

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

Task mengembalikan `error` untuk gagal (panic juga ditangkap). `ctx` adalah `context.Context`
yang dibatalkan saat timeout/scheduler stop — teruskan ke client DB/HTTP.

> XCom & conf disimpan sebagai JSON, jadi angka kembali sebagai `float64`.
> Pakai `dag.XComPullAs[int]` / `dag.ParamAs[int]`, bukan type assertion `.(int)`.

> Setelah branch, task join perlu `dag.WithTriggerRule(dag.NoneFailedMinOneSuccess)`
> (sama seperti Airflow), karena path yang tidak dipilih berstatus `skipped`.

## CLI

```bash
go mod tidy && go build -o dagsflow-go .

./dagsflow-go scheduler              # foreground (Ctrl-C = graceful stop)
./dagsflow-go scheduler -D           # background, log: logs/scheduler.log
./dagsflow-go scheduler status|stop

./dagsflow-go dags list
./dagsflow-go dags pause|unpause <dag>
./dagsflow-go dags trigger <dag> --conf '{"param2": 42}'   # queued, dijalankan scheduler
./dagsflow-go dags test <dag> [--conf ...]                 # jalankan 1 run sekarang, tanpa scheduler
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
./dagsflow-go standalone                 # scheduler + web UI dalam satu proses
# atau terpisah (seperti Airflow):
./dagsflow-go scheduler -D
./dagsflow-go webserver --addr 127.0.0.1:8080
```

Buka http://127.0.0.1:8080. Fitur:
- Daftar DAG: pause/unpause, trigger (dengan conf JSON), status run terakhir, jadwal interval berikutnya.
- Grid view: run (kolom) × task (baris), tinggi bar = durasi run.
- Graph view: dependency graph, diwarnai sesuai state run yang dipilih.
- Detail task instance: state, attempt, error, log per attempt (auto-refresh saat running),
  clear task / clear + downstream. Detail run: clear failed tasks / clear seluruh run.

Webserver hanya membaca/menulis metadata; eksekusi tetap dilakukan scheduler.
Default hanya listen di localhost. Kalau diekspos, aktifkan basic auth:
`DAGSFLOW_WEB_USER=admin DAGSFLOW_WEB_PASSWORD=... ./dagsflow-go webserver --addr 0.0.0.0:8080`
(dan taruh di belakang reverse proxy HTTPS).

## Perilaku scheduler

- Run untuk interval `[start, end)` dibuat setelah `end` lewat; `logical_date = start`.
- `catchup=false` (default): hanya interval terakhir yang dibuat saat telat/restart.
- Tidak membuat run baru selama jumlah run queued+running ≥ `max_active_runs` (default 1).
- Stop scheduler → task yang sedang jalan dibatalkan & run kembali `queued`; saat start lagi,
  task yang sudah sukses tidak diulang. Run yatim (crash) juga di-resume.
- DAG yang di-pause tidak menjalankan run apa pun (termasuk manual trigger).

## Docker Compose

```bash
cp .env.example .env            # isi DAGSFLOW_ADMIN_USERNAME / PASSWORD
docker compose run --rm --no-deps dagsflow-webserver config gen-key   # tempel ke DAGSFLOW_FERNET_KEY
docker compose up -d --build
```

Buka http://localhost:8080 dan login dengan admin dari `.env`.

| Service | Fungsi |
|---|---|
| `dagsflow-init` | membuat user admin (idempotent) dan meng-compile DAG Go sekali, lalu selesai |
| `dagsflow-scheduler` | scheduler + executor; **jalankan 1 replika saja** (store masih JSON) |
| `dagsflow-webserver` | UI + REST API di port 8080 |

- `./dags` di-mount sebagai DAG folder. Tambah/ubah file `.go` atau `.yaml` di host, dan dalam ±30 detik DAG-nya muncul tanpa rebuild image.
- State, logs, hasil compile bundle, dan build cache Go disimpan di named volume, jadi aman saat container di-recreate.
- Image berisi Go toolchain + source dagsflow-go karena dibutuhkan untuk compile DAG Go. Build cache sudah dipanaskan di image, jadi compile DAG pertama cepat.
- BigQuery: taruh service account di `./keys` lalu set `GOOGLE_APPLICATION_CREDENTIALS=/opt/dagsflow-home/keys/<file>.json` di `.env`.
- User tambahan: `docker compose exec dagsflow-webserver dagsflow-go users create --username fadhel --role Op`.
- Di belakang HTTPS reverse proxy, teruskan header `X-Forwarded-Proto: https` supaya cookie login ditandai Secure.

## Login & UI

Webserver punya halaman login (`/login`) dengan session cookie (HttpOnly, SameSite=Lax, berlaku 12 jam, ditandatangani HMAC dengan `DAGSFLOW_SECRET_KEY` atau key yang di-generate ke `state/`). Mengganti password atau menghapus user langsung membatalkan session-nya. REST API tetap bisa memakai Basic auth atau Bearer token.

Halaman DAG menampilkan filter All/Active/Paused dan Running/Failed, filter tag, pencarian, sorting, pagination, lingkaran status Runs dan Recent Tasks, serta aksi Trigger / Grid / Delete (menghapus riwayat run dan log; definisi DAG tetap ada). Owner diambil dari `DefaultArgs.Owner` (YAML: `default_args.owner`).

### Halaman DAG

Tata letaknya mengikuti Airflow 2: header (toggle pause, Schedule, Next Run, tombol Trigger / Reparse / Delete), bar filter (runs sampai tanggal tertentu, run type, run state, jumlah run), grid di kiri (chart durasi run + kotak status per task), dan panel detail di kanan dengan breadcrumb DAG / Run / Task.

| Pilihan | Tab |
|---|---|
| tidak ada | Details, Graph, Code, Event Log |
| run (klik bar) | Details, Graph, Gantt, Code, Event Log |
| task (klik nama task) | Details, Graph, Task Duration, Code, Event Log |
| task instance (klik kotak) | Details, (Mapped Tasks), Graph, Gantt, Code, Event Log, Logs, XCom, Task Duration |

Pilihan dan filter disimpan di URL, jadi bisa dibagikan dan tidak hilang saat auto-refresh. Keyboard: ←/→ pindah run, ↑/↓ pindah task, Esc hapus pilihan.

- **Code**: isi file DAG dengan nomor baris, highlight Go/YAML, Toggle Wrap, dan waktu *Parsed at*. DAG di `builtin_dags/` ikut ditampilkan (source di-embed ke binary).
- **Reparse DAG** (role User): memuat ulang file DAG saat itu juga, tanpa menunggu rescan. Webserver langsung reload; scheduler menerima permintaannya dalam ±2 detik (lewat `state/reparse_requests.json`, aman untuk container terpisah). CLI: `dagsflow-go dags reparse <dag_id>`. DAG di `builtin_dags/` tidak bisa di-reparse karena ter-compile di binary.
- **Mark state as success/failed** (role User), untuk task instance (opsi upstream/downstream) atau seluruh run. Seperti `set_task_instance_state` Airflow, task downstream yang `failed`/`upstream_failed` di-clear dan run di-queue lagi supaya lanjut jalan. Kalau run sedang berjalan, permintaan dikirim ke scheduler: task yang sedang jalan dihentikan (process group di-kill) lalu selesai dengan state yang dipilih.
- **Rendered Template** (halaman More Details): nilai field template setelah dirender, misalnya `bash_command`, `env`, `sql` (termasuk isi `sql_file` dengan `params`), `project_id`, `url`/`endpoint`, `data`, `headers`, `conf`, `filepath`. Nilai direkam saat task jalan (`rendered_fields` di run). Kalau task belum jalan di run itu, template dirender on the fly dari konteks run (params, conf, XCom, item mapping), sama seperti Airflow. CLI: `dagsflow-go tasks render <dag_id> <task_id> <run_id> [--map-index N]`.
  Task Go custom bisa ikut tampil:
  ```go
  d.NewTask("count_rows", func(ctx *dag.TaskContext) error {
      q, err := ctx.RenderTemplateField("sql") // render + rekam
      ...
  }, dag.WithTemplateField("sql", "SELECT COUNT(*) FROM `p.d.{{ .params.table }}` WHERE dt='{{ .ds }}'", "sql"))
  ```
  Untuk template yang dibangun dinamis: `ctx.RenderAndRecord("nama", raw)`.
- **Tab Details task instance** (seperti Airflow): link **More Details** dan **List All Instances**, **Task Instance Notes** (Add/Edit Note, juga ada DAG Run Notes di detail run), **Extra Links** (BigQuery Job Detail dan BigQuery Table untuk task BigQuery), lalu tabel Status / Task ID / Run ID (dengan tombol copy) / Map Index / Operator / Trigger Rule / waktu / tries / pool.
- **More Details** membuka halaman Task Instance dengan tab Grid · Task Instance Details (Dependencies Blocking Task From Getting Scheduled, Task Instance Attributes, Task Attributes) · Rendered Template · Log · XCom.
- **List All Instances** / Browse → Task Instances: semua task instance, bisa difilter per DAG, task, dan state.
- Operator custom bisa punya nama dan extra link sendiri: `dag.WithOperatorName("GcsExportOperator")`, `dag.WithExtraLink("GCS Object", "gcs_url")`, lalu di task: `ctx.XComPush("gcs_url", url)`.
- **Clear task** dengan opsi downstream / upstream / only failed, lengkap dengan pratinjau task yang terdampak.
- **Filter DAG by task**: tampilkan hanya upstream, downstream, atau keduanya dari task terpilih.
- **Event Log**: setiap aksi (trigger, pause, clear, mark, reparse, login, perubahan user/pool/variable/connection) dicatat ke `state/audit.log`; tersedia per DAG/run/task dan di Browse → Event Log.

### Users & RBAC

Security → **List Users** (Admin): tambah, edit (nama, email, role, aktif/nonaktif, reset password), buat API token, hapus. Security → **List Roles** menampilkan izin tiap role:

| Role | Izin |
|---|---|
| Viewer | lihat DAG, run, log, XCom, code, pools, datasets, event log |
| User | + trigger, pause, clear, mark state, reparse |
| Op | + pools, variables, connections, hapus run/metadata DAG |
| Admin | + kelola user dan API token |

User nonaktif tidak bisa login dan token API-nya ditolak. Reset password mencabut semua token user itu. Admin aktif terakhir tidak bisa diturunkan, dinonaktifkan, atau dihapus, dan user tidak bisa menghapus akunnya sendiri.

CLI: `users update <username> --role Op --active=false --email x@y --reset-password`, dan `users create` sekarang menerima `--first-name --last-name --email`.

API baru: `POST /dags/{dag}/reparse`, `GET /dags/{dag}/source`, `POST /dags/{dag}/dagRuns/{run}/mark` dan `.../taskInstances/{task}/mark` (body `{"new_state":"success","include_downstream":true,"map_index":0}`), `GET /eventLogs?dag_id=&run_id=&task_id=`, `GET /roles`, `GET|PATCH /users/{user}`. `GET /dags/{dag}` menerima `runs`, `base_date`, `run_type`, `state`.

## DAG Go tanpa rebuild (compile on the fly)

Taruh file `.go` berisi `package main` di DAG folder. Scheduler dan webserver meng-compile-nya jadi *DAG bundle* (binary terpisah), lalu setiap task dijalankan sebagai subprocess dari bundle itu.

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

- Tidak perlu `func main()`, karena sudah di-generate.
- Satu file di root DAG folder = satu bundle. Satu subfolder = satu bundle multi-file, jadi helper bisa di file lain dalam folder yang sama.
- File diubah → di-rebuild pada scan berikutnya (`--dag-dir-list-interval`). File dihapus → DAG hilang. Error compile muncul di **Import errors**.
- Build di-cache di `$DAGSFLOW_HOME/build/bin`, per hash isi file + source SDK. Build ulang dengan cache Go yang sudah hangat hanya sekitar 0,5 detik.
- Task berjalan di proses sendiri. Timeout me-kill seluruh process group, jadi panic atau task macet tidak menjatuhkan scheduler.
- XCom, variables, connections, `TriggerDagRun`, dan `ExternalTaskState` diakses dari bundle lewat RPC ke scheduler (127.0.0.1 + token per task). Kunci enkripsi cukup ada di scheduler.
- Semua fitur tersedia: branch, sensor, mapping, callbacks, pools, SLA, datasets.

Kebutuhan runtime: Go toolchain dan checkout source dagsflow-go. Lokasinya dideteksi otomatis dari working dir atau lokasi binary, atau bisa di-set lewat `DAGSFLOW_SDK_DIR`; `DAGSFLOW_GO` untuk path `go`. Dependency ikut `go.mod` SDK. Kalau DAG meng-import library lain, build memakai `-mod=mod` sehingga perlu akses ke GOPROXY. Node tanpa Go tetap bisa memakai binary yang sudah ada di build cache bersama.

Kalau DAG folder berada di dalam module Go lain (seperti `dags/` di repo ini), beri `//go:build dagsflow` di baris pertama supaya `go build ./...` repo mengabaikannya. Contohnya `dags/features.go`.

DAG yang ingin ikut ter-compile ke binary (tanpa toolchain di server) tetap bisa: taruh di `builtin_dags/`, yang di-import oleh `main.go`.

## DAG YAML (deploy tanpa rebuild)

Taruh file `*.yaml` di DAG folder (`$DAGSFLOW_DAGS_FOLDER`, default `$DAGSFLOW_HOME/dags`). Scheduler & webserver me-rescan tiap `--dag-dir-list-interval` (default 30s): file baru/berubah di-load, file dihapus di-unregister, file rusak muncul di **Admin → Import errors**.

```yaml
dag_id: yaml_example
schedule: "@daily"          # atau schedule_datasets: [bq://proj.ds.table]
default_args: {retries: 2, retry_delay: 10s, on_failure: [gchat_alerts]}
tasks:
  - id: list_files
    operator: bash
    command: echo '["a.csv","b.csv"]'      # baris stdout terakhir -> XCom return_value
  - id: process_file
    operator: bash
    command: echo "{{ .item }} {{ .ds }}"
    expand: {xcom: {task: list_files}}     # dynamic task mapping
    pool: bigquery
  - id: load
    operator: bigquery
    sql_file: sql/load.sql                 # relatif ke file YAML
    upstream: [process_file]
    outlets: [bq://proj.dw.sales]
```

Operator: `empty`, `bash`, `bigquery`, `http`, `trigger_dagrun`, `file_sensor`, `time_delta_sensor`, `external_task_sensor`, `http_sensor`. Logika custom tetap pakai DAG Go (butuh rebuild).

## Fitur Airflow lain

| Fitur | Go | YAML |
|---|---|---|
| Pools | `dag.WithPool("bigquery")`, `WithPoolSlots(n)`; `dagsflow-go pools set bigquery 4` | `pool:` |
| Priority weight | `dag.WithPriorityWeight(n)` (weight_rule = downstream) | `priority_weight:` |
| Sensors | `d.NewSensor(id, pokeFn, dag.SensorConfig{PokeInterval, Timeout, Mode: dag.ModeReschedule, SoftFail})`, `NewFileSensor`, `NewTimeDeltaSensor`, `NewExternalTaskSensor`, `NewHTTPSensor` | `*_sensor`, `mode: reschedule` |
| Dynamic task mapping | `dag.Expand(dag.FromXCom("t","return_value"))` / `dag.FromItems(...)`; di task: `ctx.Item`, `dag.ItemAs[T](ctx)`; pull dari task mapped → list | `expand:` |
| Datasets | `dag.WithOutlets(uri)` di task, `dag.WithDatasetSchedule(uri...)` di DAG | `outlets:`, `schedule_datasets:` |
| SLA | `dag.WithSLA(d)` + `dag.WithSLAMiss(cb)` (deadline = data_interval_end + sla, run terjadwal saja) | `sla:`, `on_sla_miss:` |
| Callbacks | task: `WithOnFailureCallback/OnSuccess/OnRetry`, DAG: `WithOnFailure/WithOnSuccess`, atau `DefaultArgs.OnFailure` | `on_failure: [conn_id]` |

`mode: reschedule` melepas slot executor/pool di antara poke (mirip deferrable).

### Alert Google Chat

```bash
dagsflow-go connections add gchat_alerts --conn-type google_chat \
  --config '{"webhook_url":"https://chat.googleapis.com/v1/spaces/.../messages?key=...&token=..."}'
export DAGSFLOW_BASE_URL=https://dagsflow.internal   # tombol "Open in dagsflow" di card
```
```go
alert := dag.GoogleChatNotifier("gchat_alerts")
d := dag.NewDAG("x", "@daily", dag.WithOnFailure(alert),
    dag.WithDefaultArgs(dag.DefaultArgs{OnFailure: []dag.Callback{alert}}))
```
Callback custom: `func(ctx context.Context, cc dag.CallbackContext) error`.

## Variables, connections & secrets

```bash
export DAGSFLOW_FERNET_KEY=$(dagsflow-go config gen-key)   # AES-256-GCM; tanpa ini disimpan plain text (ada warning)
dagsflow-go variables set region APAC
dagsflow-go variables import vars.json
dagsflow-go connections add my_api --conn-type http --config '{"host":"https://api.x","token":"..."}'
```
Di task: `dag.GetVariable("region")`, `dag.GetVariableJSON[T]`, template `{{ var "region" }}`, `dag.GetConnection(id)`.

Urutan lookup (secrets backend): env (`DAGSFLOW_VAR_REGION`, `DAGSFLOW_CONN_MY_API` berisi JSON) → metastore terenkripsi → `connections/*.json`. Backend lain (mis. GCP Secret Manager) cukup implement `dag.SecretsBackend` lalu `dag.SetSecretsBackends(...)`.

## RBAC & REST API

```bash
dagsflow-go users create --username fadhel --role Admin   # Viewer | User | Op | Admin
dagsflow-go users token fadhel                            # API token (tampil sekali)
```
Tanpa user, auth mati dan webserver hanya boleh di localhost. Role: **Viewer** baca; **User** + trigger/pause/clear; **Op** + pools/variables/connections/hapus run; **Admin** + users.

Auth: HTTP Basic atau `Authorization: Bearer <token>`. Request non-GET via Basic wajib header `X-Dagsflow: 1` (CSRF).

```bash
H="Authorization: Bearer $TOKEN"; B=http://localhost:8080/api/v1
curl -H "$H" $B/dags
curl -H "$H" -X POST $B/dags/custom_dag/dagRuns -d '{"conf":{"val":70}}'
curl -H "$H" $B/dags/custom_dag/dagRuns/<run_id>/taskInstances
curl -H "$H" "$B/dags/custom_dag/dagRuns/<run_id>/taskInstances/<task>/logs/1?map_index=0"
curl -H "$H" -X PATCH $B/dags/custom_dag -d '{"is_paused":true}'
```
Endpoint lain: `dagRuns/{run}/clear`, `DELETE dagRuns/{run}`, `pools`, `variables`, `connections` (nilai sensitif di-mask), `datasets`, `importErrors`, `users`, `users/{u}/tokens`, `health`, `me`.

## Batasan saat ini

- Executor masih satu proses (LocalExecutor). Executor terdistribusi butuh metadata DB transaksional (Postgres) dan direncanakan berikutnya.
- Metadata disimpan sebagai JSON; tulis bersamaan dari beberapa proses = last-writer-wins. Jalankan satu scheduler.
- Task Go berjalan sebagai goroutine; task yang tidak menghormati `ctx` tetap jalan setelah timeout. Bash task di-kill satu process group.

## Connections

`connections/*.json` (atau `GOOGLE_APPLICATION_CREDENTIALS` + `GOOGLE_CLOUD_PROJECT` → `google_cloud_default`):

```json
{ "google_cloud_default": { "Type": "google_cloud_platform",
  "Config": { "project_id": "my-project", "credentials_path": "/path/sa.json", "location": "asia-southeast2" } } }
```

`NewBigQueryTask` memakai param `gcp_conn_id` (default `google_cloud_default`).
Semua data (state, logs, connections) relatif ke `$DAGSFLOW_HOME` (default: direktori kerja).
