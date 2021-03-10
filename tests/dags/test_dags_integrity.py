import importlib.util
from os import path
from pathlib import Path
from types import ModuleType

import pytest
from airflow import DAG
from airflow.utils.dag_cycle_tester import test_cycle as check_for_cycles

DAGS_DIRECTORY = Path(__file__).parent / ".." / ".." / "dags"
DAGS_PATHS = DAGS_DIRECTORY.glob("**/*.py")


def import_module(module_name: str, module_path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        name=module_name, location=module_path
    )
    module = importlib.util.module_from_spec(spec=spec)
    spec.loader.exec_module(module=module)  # type: ignore
    return module


@pytest.mark.parametrize("dag_path", DAGS_PATHS)
def test_dag_integrity(dag_path: Path) -> None:
    """Import a DAG file and check if it has a valid DAG instance."""
    dag_name = path.basename(dag_path)
    dag_module = import_module(dag_name, dag_path)

    # Look for all DAG instancies.
    dags = [var for var in vars(dag_module).values() if isinstance(var, DAG)]
    # Assert that there is at least one DAG instance.
    assert dags

    # Test all DAG instancies for cycles.
    for dag in dags:
        check_for_cycles(dag=dag)
