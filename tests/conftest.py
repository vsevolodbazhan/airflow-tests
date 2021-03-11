from typing import Any, Dict

import pendulum
import pytest
from airflow.models import DAG, TaskInstance
from airflow.models.dagbag import DagBag
from airflow.models.baseoperator import BaseOperator

pytest_plugins = ["helpers_namespace"]


@pytest.helpers.register
def get_dag(dag_id: str) -> DAG:
    dag_bag = DagBag()
    return dag_bag.get_dag(dag_id=dag_id)


@pytest.helpers.register
def get_dag_structure(dag: DAG) -> Dict:
    structure = {}
    for task in dag.tasks:
        task_id, downstream_task_ids = task.task_id, list(task.downstream_task_ids)
        structure[task_id] = downstream_task_ids
    return structure


@pytest.helpers.register
def get_task(dag: DAG, task_id: str) -> BaseOperator:
    for task in dag.tasks:
        if task.task_id == task_id:
            return task
    raise KeyError(f"Task with ID '{task_id}' does not exist.")


@pytest.helpers.register
def run_task(task_instance: TaskInstance) -> None:
    task_instance.run(test_mode=True)


@pytest.helpers.register
def run_xcom_task(task_instance: TaskInstance, delete_xcom_entry: bool = True) -> Any:
    run_task(task_instance=task_instance)
    value = task_instance.xcom_pull(task_ids=[task_instance.task_id])
    if delete_xcom_entry is True:
        task_instance.clear_xcom_data()
    return value


@pytest.helpers.register
def get_task_instance(
    task: BaseOperator,
    execution_date: pendulum.datetime,
) -> TaskInstance:
    return TaskInstance(task=task, execution_date=execution_date)


@pytest.helpers.register
def get_xcom_value(task_instance: TaskInstance):
    return task_instance.xcom_pull(task_ids=[task_instance.task_id])
