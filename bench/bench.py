"""Benchmark harness: python3 bench.py <airflow|dagsflow> <small|big|gosmall|gobig> <out-prefix> [full|quick]. See bench/README.md."""
import os, sys, time, json, subprocess, signal, base64, urllib.request, statistics
SYS = sys.argv[1]; WL = sys.argv[2]; OUT = sys.argv[3]; MODE = sys.argv[4] if len(sys.argv) > 4 else "full"
B = os.environ.get("BENCH_DIR", os.path.dirname(os.path.abspath(__file__))); res = {"system": SYS, "workload": WL}

def psql(db, q):
    return subprocess.run(["psql", os.environ.get("BENCH_PG", "postgres://bench:bench@127.0.0.1") + f"/{db}", "-tAc", q], capture_output=True, text=True).stdout.strip()

if SYS == "airflow":
    BASE = "http://127.0.0.1:8080"
    HDR = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode(), "Content-Type": "application/json"}
    env = dict(os.environ)
    for l in open(f"{B}/afenv.sh"):
        if l.startswith("export") and "PATH" not in l:
            k, v = l[len("export "):].split("=", 1); env[k] = v.strip()
    env["AIRFLOW__CORE__DAGS_FOLDER"] = f"{B}/wl/{WL}/af_dags"
    env["AIRFLOW__API__AUTH_BACKENDS"] = "airflow.api.auth.backend.basic_auth"
    env["PATH"] = f"{B}/af/bin:" + env["PATH"]
    psql("airflow", "TRUNCATE dag_run, task_instance, serialized_dag, dag, dag_code, log, job CASCADE")
    cmds = [["airflow", "scheduler"], ["airflow", "webserver", "-p", "8080"]]
else:
    BASE = "http://127.0.0.1:8095"; HDR = {"X-Dagsflow": "1", "Content-Type": "application/json"}
    os.system(f"rm -rf {B}/dfhome; mkdir -p {B}/dfhome")
    psql("dagsflow", "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    env = dict(os.environ, DAGSFLOW_HOME=f"{B}/dfhome", DAGSFLOW_DAGS_FOLDER=f"{B}/wl/{WL}/df_dags",
               DAGSFLOW_DATABASE_URL=os.environ.get("BENCH_PG", "postgres://bench:bench@127.0.0.1") + "/dagsflow?sslmode=disable",
               DAGSFLOW_SDK_DIR=os.environ.get("DAGSFLOW_SDK_DIR", os.path.dirname(B)))
    cmds = [[os.environ.get("DAGSFLOW_BIN", "dagsflow-go"), "standalone", "--addr", "127.0.0.1:8095", "--dag-dir-list-interval", "5s", "--parallelism", "16"]]

def req(path, data=None):
    r = urllib.request.Request(BASE + path, data=json.dumps(data).encode() if data is not None else None,
                               headers=HDR, method="POST" if data is not None else "GET")
    with urllib.request.urlopen(r, timeout=30) as f:
        return json.loads(f.read() or b"{}")

def ok(path):
    try:
        r = urllib.request.Request(BASE + path, headers=HDR)
        with urllib.request.urlopen(r, timeout=10) as f: return f.status == 200
    except Exception: return False

procs = []; t0 = time.time()
for c in cmds:
    procs.append(subprocess.Popen(c, env=env, stdout=open(f"{OUT}.{c[1]}.log", "w"), stderr=subprocess.STDOUT, start_new_session=True))

def pss():
    tot = 0; pids = set()
    for p in procs:
        pids |= set(subprocess.run(["pgrep", "-s", str(p.pid)], capture_output=True, text=True).stdout.split())
    for pid in pids:
        try:
            for l in open(f"/proc/{pid}/smaps_rollup"):
                if l.startswith("Pss:"): tot += int(l.split()[1])
        except Exception: pass
    return round(tot / 1024, 1), len(pids)

def run(dag, timeout=900):
    ts = time.time(); rid = req(f"/api/v1/dags/{dag}/dagRuns", {})["dag_run_id"]; peak = 0
    while True:
        st = req(f"/api/v1/dags/{dag}/dagRuns/{rid}")["state"]
        if st in ("success", "failed"): break
        if time.time() - ts > timeout: st = "timeout"; break
        peak = max(peak, pss()[0]); time.sleep(.2)
    return round(time.time() - ts, 2), st, peak

try:
    ppk = 0
    while not ok("/health"):
        if time.time() - t0 > 5400: raise SystemExit("no health")
        ppk = max(ppk, pss()[0]); time.sleep(.2)
    res["startup_health_s"] = round(time.time() - t0, 2)
    while not ok("/api/v1/dags/chain50"):
        if time.time() - t0 > 5400: raise SystemExit("chain50 never visible")
        time.sleep(.2)
    res["first_dag_visible_s"] = round(time.time() - t0, 2)
    if WL.endswith("big"):
        def cnt():
            if SYS == "airflow": return int(psql("airflow", "select count(*) from serialized_dag") or 0)
            return len(req("/api/v1/dags?limit=5000").get("dags", []))
        while cnt() < 1002 and time.time() - t0 < 5400:
            ppk = max(ppk, pss()[0]); time.sleep(1)
        res["all_1002_dags_visible_s"] = round(time.time() - t0, 1); res["dags_seen"] = cnt()
    res["load_peak_pss_mb"] = ppk
    time.sleep(60 if MODE == "full" else 5)
    res["idle_pss_mb"], res["procs"] = pss()
    for dag in ("chain50", "fan200"):
        rs = [run(dag) for _ in range(3)]
        res[dag] = {"runs_s": [r[0] for r in rs], "states": [r[1] for r in rs],
                    "median_s": statistics.median(r[0] for r in rs), "peak_pss_mb": max(r[2] for r in rs)}
finally:
    for p in procs:
        try: os.killpg(p.pid, signal.SIGTERM)
        except Exception: pass
    time.sleep(3)
    for p in procs:
        try: os.killpg(p.pid, signal.SIGKILL)
        except Exception: pass
    json.dump(res, open(f"{OUT}.json", "w"), indent=1); print(json.dumps(res))
