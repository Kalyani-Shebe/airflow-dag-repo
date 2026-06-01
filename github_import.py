from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import shutil
import os

DAGS_DIR = "/opt/airflow/dags"

def import_repo(**context):

    repo_url = context["dag_run"].conf.get("repo_url")

    if not repo_url:
        raise ValueError("repo_url missing")

    repo_name = repo_url.split("/")[-1].replace(".git", "")

    clone_dir = f"/tmp/{repo_name}"
    target_dir = f"{DAGS_DIR}/{repo_name}"

    if os.path.exists(clone_dir):
        shutil.rmtree(clone_dir)

    subprocess.run(
        ["git", "clone", repo_url, clone_dir],
        check=True
    )

    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)

    shutil.copytree(
        clone_dir,
        target_dir
    )

with DAG(
    dag_id="github_import",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    PythonOperator(
        task_id="import_repo",
        python_callable=import_repo,
    )
