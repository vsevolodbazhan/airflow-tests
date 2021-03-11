import pytest
from airflow.utils.state import State


@pytest.fixture
def dag_id():
    return "simple_dag"


def test_dag_structure_is_valid(dag_id):
    dag = pytest.helpers.get_dag(dag_id=dag_id)
    assert pytest.helpers.get_dag_structure(dag=dag) == {
        "start_task": ["first_task"],
        "first_task": sorted(["second_task", "third_task"]),
        "second_task": ["end_task"],
        "third_task": ["end_task"],
        "end_task": [],
    }


def test_start_task_runs_successfully(dag_id):
    task_instance = pytest.helpers.build_task_instance(dag_id=dag_id, task_id="start_task")
    state = pytest.helpers.run_task(task_instance=task_instance)
    assert state == State.SUCCESS


def test_first_task_runs_successfully(dag_id):
    task_instance = pytest.helpers.build_task_instance(dag_id=dag_id, task_id="first_task")
    state, xcom_value = pytest.helpers.run_xcom_task(task_instance=task_instance)
    assert state == State.SUCCESS
    assert xcom_value == "1"


def test_second_task_runs_successfully(dag_id):
    task_instance = pytest.helpers.build_task_instance(dag_id=dag_id, task_id="second_task")
    state, xcom_value = pytest.helpers.run_xcom_task(task_instance=task_instance)
    assert state == State.SUCCESS
    assert xcom_value == "2"


def test_third_task_runs_successfully(dag_id):
    task_instance = pytest.helpers.build_task_instance(dag_id=dag_id, task_id="third_task")
    state, xcom_value = pytest.helpers.run_xcom_task(task_instance=task_instance)
    assert state == State.SUCCESS
    assert xcom_value == "3"


def test_end_task(dag_id):
    task_instance = pytest.helpers.build_task_instance(dag_id=dag_id, task_id="end_task")
    state = pytest.helpers.run_task(task_instance=task_instance)
    assert state == State.SUCCESS
