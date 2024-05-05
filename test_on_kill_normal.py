import os
from datetime import timedelta
from typing import Optional

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.dates import days_ago


# define a PythonOperator extension which sets `on_kill` to write a file
class PythonOperatorWithOnKill(PythonOperator):
    statement_name: Optional[str]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.statement_name = None

    def execute(self, context: Context):
        self.statement_name = (
            f"airflow"
            f"::{self.dag.dag_id}"
            f"::{self.task_id}"
            f"::{pendulum.now(timezone.utc).isoformat()}"
        )
        super().execute(context)

    def on_kill(self):
        output_folder = f"/tmp/testing/on_kill_normal/{self.dag.dag_id}/{self.task_id}"
        os.makedirs(output_folder, exist_ok=True)
        with open(f"{output_folder}/log.txt", "a") as f:
            f.write(f"on_kill was called: {self.statement_name}\n")


def f_sleep(seconds: int):
    import time

    print(f"Sleeping for {seconds} seconds")
    time.sleep(seconds)
    print(f"Done sleeping for {seconds} seconds")


with DAG(
    dag_id="test_on_kill_normal",
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
) as dag:

    # task 1
    task_1 = PythonOperatorWithOnKill(
        task_id="task_1",
        python_callable=f_sleep,
        op_args=[120],
    )
