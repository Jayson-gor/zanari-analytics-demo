from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello Airflow!")

test_dag = DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 11, 23),
    schedule=None,
    catchup=False,
)

PythonOperator(
    task_id="hello_task",
    python_callable=hello,
    dag=test_dag,
)
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_minimal_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> end
