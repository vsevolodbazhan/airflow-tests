from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="simple_dag",
    description="Simple DAG for experimenting with tests.",
    start_date=days_ago(n=1),
) as dag:
    BashOperator(
        task_id="echo_execution_date",
        bash_command="echo {{ execution_date }}",
    )
