from airflow import DAG
<<<<<<< HEAD
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from GitSync DAG")

with DAG(
    dag_id="hello_world",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
=======

