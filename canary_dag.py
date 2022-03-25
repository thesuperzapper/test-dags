from datetime import datetime, timedelta
from airflow import DAG

# import using try/except to support both airflow 1 and 2
try:
    from airflow.operators.bash import BashOperator
except ModuleNotFoundError:
    from airflow.operators.bash_operator import BashOperator

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

# WARNING: while `DummyOperator` would use less resources, the probe can't detect those tasks
#          as they don't create LocalTaskJob instances
task = BashOperator(
    task_id="canary_task",
    bash_command="echo 'Hello World!'",
    dag=dag,
)