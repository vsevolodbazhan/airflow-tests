import pytest
from airflow.models import DAG


def get_dag() -> DAG:
    return pytest.helpers.get_dag(dag_id="simple_dag")


def test_dag_structure() -> None:
    dag = get_dag()
    assert pytest.helpers.get_dag_structure(dag=dag) == {
        "echo_execution_date": ["echo_next_ds"],
        "echo_next_ds": [],
    }


def test_echo_execution_date_task() -> None:
    dag = get_dag()
    task = pytest.helpers.get_task(dag=dag, task_id="echo_execution_date")
    task.run()
