"""Generate the YAML (dagsflow) and Python (Airflow) workloads: python3 gen.py <dir> <filler DAGs>.

chain50 = 50 sequential `true` bash tasks, fan200 = 200 independent ones, plus N filler DAGs
(5 chained tasks each) to measure parsing.
"""
import os,sys,shutil
def gen(root,ndummy):
    af=f"{root}/af_dags"; df=f"{root}/df_dags"
    for d in (af,df): shutil.rmtree(d,ignore_errors=True); os.makedirs(d)
    # chain50: 50 sequential `true` tasks
    a=["from airflow import DAG","from airflow.operators.bash import BashOperator","import pendulum",
       "with DAG('chain50', schedule=None, start_date=pendulum.datetime(2024,1,1), catchup=False, max_active_tasks=16) as d:",
       "    prev=None","    for i in range(50):","        t=BashOperator(task_id=f't{i}', bash_command='true')","        if prev: prev>>t","        prev=t"]
    open(f"{af}/chain50.py","w").write("\n".join(a)+"\n")
    y=["dag_id: chain50","schedule: null","tasks:"]+sum([[f"  - id: t{i}","    operator: bash","    command: 'true'"]+([f"    upstream: [t{i-1}]"] if i else []) for i in range(50)],[])
    open(f"{df}/chain50.yaml","w").write("\n".join(y)+"\n")
    # fan200: 200 independent `true` tasks
    a=["from airflow import DAG","from airflow.operators.bash import BashOperator","import pendulum",
       "with DAG('fan200', schedule=None, start_date=pendulum.datetime(2024,1,1), catchup=False, max_active_tasks=16) as d:",
       "    for i in range(200): BashOperator(task_id=f't{i}', bash_command='true')"]
    open(f"{af}/fan200.py","w").write("\n".join(a)+"\n")
    y=["dag_id: fan200","schedule: null","tasks:"]+sum([[f"  - id: t{i}","    operator: bash","    command: 'true'"] for i in range(200)],[])
    open(f"{df}/fan200.yaml","w").write("\n".join(y)+"\n")
    # N filler DAGs with 5 tasks each (parse load)
    for k in range(ndummy):
        open(f"{af}/d{k:04d}.py","w").write("from airflow import DAG\nfrom airflow.operators.bash import BashOperator\nimport pendulum\n"
          f"with DAG('d{k:04d}', schedule=None, start_date=pendulum.datetime(2024,1,1), catchup=False) as d:\n"
          "    ts=[BashOperator(task_id=f't{i}', bash_command='true') for i in range(5)]\n    for a,b in zip(ts,ts[1:]): a>>b\n")
        open(f"{df}/d{k:04d}.yaml","w").write(f"dag_id: d{k:04d}\nschedule: null\ntasks:\n"+"".join(f"  - id: t{i}\n    operator: bash\n    command: 'true'\n"+(f"    upstream: [t{i-1}]\n" if i else "") for i in range(5)))
gen(sys.argv[1],int(sys.argv[2]))
