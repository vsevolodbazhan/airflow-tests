from typing import Any, Dict, Tuple

import pendulum
import pytest
from airflow.models import DAG, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.exceptions import AirflowNotFoundException
from airflow.models.dagbag import DagBag
from airflow.utils.state import State

pytest_plugins = ["helpers_namespace"]

DEFAULT_EXECUTION_DATE = pendulum.datetime(year=2021, month=1, day=1)


@pytest.helpers.register
def build_task_instance(
    dag_id: str,
    task_id: str,
    execution_date: pendulum.datetime = DEFAULT_EXECUTION_DATE,
) -> TaskInstance:
    dag = get_dag(dag_id=dag_id)
    task = get_task(dag=dag, task_id=task_id)
    return create_task_instance(task=task, execution_date=execution_date)


@pytest.helpers.register
def get_dag(dag_id: str) -> DAG:
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id=dag_id)
    if dag is None:
        raise KeyError(f"DAG with ID '{dag_id}' does not exist.")
    return dag


@pytest.helpers.register
def get_task(dag: DAG, task_id: str) -> BaseOperator:
    for task in dag.tasks:
        if task.task_id == task_id:
            return task
    raise KeyError(f"Task with ID '{task_id}' does not exist.")


@pytest.helpers.register
def get_xcom_value(task_instance: TaskInstance):
    return task_instance.xcom_pull(task_ids=task_instance.task_id)


@pytest.helpers.register
def does_connection_exist(conn_id: str) -> bool:
    try:
        Connection.get_connection_from_secrets(conn_id=conn_id)
        return True
    except AirflowNotFoundException:
        return False


@pytest.helpers.register
def get_dag_structure(dag: DAG) -> Dict:
    structure = {}
    for task in dag.tasks:
        task_id, downstream_task_ids = task.task_id, list(task.downstream_task_ids)
        structure[task_id] = sorted(downstream_task_ids)
    return structure


@pytest.helpers.register
def create_task_instance(
    task: BaseOperator,
    execution_date: pendulum.datetime = DEFAULT_EXECUTION_DATE,
) -> TaskInstance:
    return TaskInstance(task=task, execution_date=execution_date)


@pytest.helpers.register
def run_task(task_instance: TaskInstance) -> State:
    task_instance._run_raw_task(test_mode=True)
    return task_instance.state


@pytest.helpers.register
def run_xcom_task(
    task_instance: TaskInstance,
    delete_xcom_entry: bool = True,
) -> Tuple[State, Any]:
    state = run_task(task_instance=task_instance)
    xcom_value = get_xcom_value(task_instance=task_instance)
    if delete_xcom_entry is True:
        task_instance.clear_xcom_data()
    return state, xcom_value
