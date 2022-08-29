import datetime
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

# import with try/except to support both airflow 1 and 2
try:
    from airflow.operators.bash import BashOperator
except ModuleNotFoundError:
    from airflow.operators.bash_operator import BashOperator

args = {
    "owner": "airflow",
}

dag = DAG(
    dag_id="bash_sleep_then_fail",
    default_args=args,
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
)

run_this = BashOperator(
    task_id="sleep_then_fail",
    bash_command="sleep 15; exit 1;",
    retries=5,
    retry_delay=datetime.timedelta(seconds=15),
    dag=dag,
)
