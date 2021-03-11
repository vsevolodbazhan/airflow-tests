from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="simple_dag",
    description="Simple DAG for experimenting with tests.",
    start_date=days_ago(n=1),
) as dag:
    start_task = DummyOperator(
        task_id="start_task",
    )

    first_task = BashOperator(
        task_id="first_task",
        bash_command="echo 1",
    )

    second_task = BashOperator(
        task_id="second_task",
        bash_command="echo 2",
    )

    third_task = BashOperator(
        task_id="third_task",
        bash_command="echo 3",
    )

    end_task = DummyOperator(
        task_id="end_task",
    )

    start_task >> first_task >> [second_task, third_task] >> end_task
