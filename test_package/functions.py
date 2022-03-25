# import with try/except to support both airflow 1 and 2
try:
    from airflow.operators.bash import BashOperator
except ModuleNotFoundError:
    from airflow.operators.bash_operator import BashOperator


def create_bash_task(dag):
    return BashOperator(
        task_id="bash_task_from_function",
        bash_command="sleep 60",
        dag=dag,
    )
