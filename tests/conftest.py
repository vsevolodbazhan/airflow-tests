from typing import Dict

import pytest
from airflow.models import DAG
from airflow.models.dagbag import DagBag
from airflow.models.baseoperator import BaseOperator

pytest_plugins = ["helpers_namespace"]


@pytest.helpers.register
def get_dag(dag_id: str) -> DAG:
    dag_bag = DagBag()
    return dag_bag.get_dag(dag_id=dag_id)


@pytest.helpers.register
def get_task(dag: DAG, task_id: str) -> BaseOperator:
    for task in dag.tasks:
        if task.task_id == task_id:
            return task
    raise KeyError(f"Task with ID '{task_id}' does not exist.")


@pytest.helpers.register
def get_dag_structure(dag: DAG) -> Dict:
    structure = {}
    for task in dag.tasks:
        task_id, downstream_task_ids = task.task_id, list(task.downstream_task_ids)
        structure[task_id] = downstream_task_ids
    return structure
