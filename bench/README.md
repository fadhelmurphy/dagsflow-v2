# Benchmark: dagsflow vs Airflow

Scripts behind the [Benchmark vs Airflow](../README.md#benchmark-vs-airflow) section. They measure orchestration overhead: every task does nothing.

| File | What it does |
|---|---|
| `gen.py` | Writes the YAML (dagsflow) and Python (Airflow) DAGs: `chain50`, `fan200` and N filler DAGs |
| `gen_go.py` | Writes the same DAGs as Go files (Go function tasks) |
| `afenv.sh` | Airflow settings: Postgres, LocalExecutor, parallelism 16, no examples |
| `bench.py` | Starts one system, measures startup, DAG loading and RAM, triggers `chain50` and `fan200` 3 times each through the REST API, then stops it and writes `<out>.json` |

## Requirements

- Postgres with a superuser `bench` / `bench` and the databases `airflow` and `dagsflow` (or set `BENCH_PG`, e.g. `postgres://user:pass@host`).
- Python 3.11 and the Go toolchain (Go DAGs are compiled at runtime).
- A dagsflow binary. For a fair comparison, build it without the example DAGs: remove the `dagsflow-go/builtin_dags` import from `main.go` and run `go build -o dagsflow-bench .`. Point `DAGSFLOW_BIN` at it.

## Run

```sh
cd bench
python3 -m venv af
af/bin/pip install "apache-airflow[postgres]==2.10.5" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt
. ./afenv.sh && airflow db migrate && airflow users create -u admin -p admin -r Admin -f a -l b -e a@b.c

python3 gen.py wl/small 0 && python3 gen.py wl/big 1000
python3 gen_go.py wl/gosmall 0 && python3 gen_go.py wl/gobig 1000
mkdir -p results
export DAGSFLOW_BIN=$PWD/../dagsflow-bench DAGSFLOW_SDK_DIR=$PWD/..

python3 bench.py dagsflow small   results/dagsflow_small
python3 bench.py airflow  small   results/airflow_small
python3 bench.py dagsflow big     results/dagsflow_big
python3 bench.py airflow  big     results/airflow_big
python3 bench.py dagsflow gosmall results/dagsflow_gosmall
python3 bench.py dagsflow gobig   results/dagsflow_gobig
```

Run one system at a time so they do not compete for the CPU. `bench.py` resets its database (and `dfhome/`) before each run. `big` and `gobig` add 1,000 filler DAGs; `gobig` compiles 1,000 bundles and needs about 7 GB of disk. Pass `quick` as the fourth argument to wait 5 s instead of 60 s before measuring idle RAM.

Each `results/*.json` contains `startup_health_s`, `first_dag_visible_s`, `all_1002_dags_visible_s` (big workloads), `idle_pss_mb`, and for `chain50` / `fan200` the three run times, their states, the median and the peak PSS.
