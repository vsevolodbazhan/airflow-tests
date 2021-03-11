import pytest


@pytest.fixture
def dag_id():
    return "postgres_dag"


def test_connection_for_retrieve_cars_task_exists(dag_id):
    dag = pytest.helpers.get_dag(dag_id=dag_id)
    task = pytest.helpers.get_task(dag=dag, task_id="retrieve_cars")
    assert pytest.helpers.does_connection_exist(conn_id=task.postgres_conn_id) is True


def test_dag_structure_is_valid(dag_id):
    dag = pytest.helpers.get_dag(dag_id=dag_id)
    assert dag
    assert pytest.helpers.get_dag_structure(dag=dag) == {
        "retrieve_cars": [],
    }
