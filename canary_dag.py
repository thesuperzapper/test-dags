from datetime import datetime, timedelta
from airflow import DAG

# import with try/except to support both airflow 1 and 2
try:
    from airflow.operators.dummy import DummyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="canary_dag",
    default_args={
        "owner": "airflow",
    },
    schedule_interval="*/5 * * * *",
    start_date=datetime(2022, 1, 1),
    dagrun_timeout=timedelta(minutes=5),
    is_paused_upon_creation=False,
    catchup=False,
)

task = DummyOperator(
    task_id="canary_task",
    dag=dag,
)