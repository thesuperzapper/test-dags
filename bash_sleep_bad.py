import datetime
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.sensors.bash import BashSensor
from airflow.utils.dates import days_ago

args = {
    "owner": "airflow",
}

dag = DAG(
    dag_id="bash_sleep_bad",
    default_args=args,
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
)

run_this = BashSensor(
    task_id="sleep_60",
    bash_command="sleep 60",
    dag=dag,
)

# bad start date (after 9999-12-31)
run_this.start_date = 253370764800 + 365 * 24 * 60 * 60 + 1
