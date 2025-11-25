"""Demo Airflow DAG: simple print and sleep to validate scheduler/webserver.
Runs every 15 minutes.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def hello():
    print("Hello from demo_ingest_dag at run time")


def do_work():
    import time
    time.sleep(2)
    print("Demo work complete")


## DAG is now defined at module level above
demo_ingest_dag = DAG(
    dag_id="demo_ingest_dag",
    start_date=datetime(2025, 11, 20),
    schedule=timedelta(minutes=15),
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["demo"],
)
t1 = PythonOperator(task_id="hello_task", python_callable=hello, dag=demo_ingest_dag)
t2 = PythonOperator(task_id="work_task", python_callable=do_work, dag=demo_ingest_dag)
t1 >> t2
