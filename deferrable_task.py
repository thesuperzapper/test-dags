from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

try:
    # wrapped in try/except because deferrable task operators were added in airflow 2.2.0
    from airflow.sensors.date_time import DateTimeSensorAsync

    args = {
        "owner": "airflow",
    }

    dag = DAG(
        dag_id="deferrable_task",
        default_args=args,
        schedule_interval="0 0 * * *",
        start_date=days_ago(2),
        dagrun_timeout=timedelta(minutes=60),
    )

    async_task_1 = DateTimeSensorAsync(
        task_id="async_task_1",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=1) }}""",
        dag=dag,
    )

except ModuleNotFoundError:
    pass
