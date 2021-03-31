from airflow.operators.bash import BashOperator


def create_bash_task(dag):
    return BashOperator(
        task_id='bash_task_from_function',
        bash_command='sleep 60',
        dag=dag,
    )