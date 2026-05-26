from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="github_test_dag",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    task = BashOperator(
        task_id="hello",
        bash_command="echo Hello from GitHub DAG"
    )
