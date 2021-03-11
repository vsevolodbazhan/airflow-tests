from airflow.models.dagbag import DagBag


def test_dags_integrity():
    dag_bag = DagBag()
    # Assert that all DAGs can be imported, i.e., all parameters required
    # for DAGs are specified and no task cycles present.
    assert dag_bag.import_errors == {}

    for dag_id in dag_bag.dag_ids:
        dag = dag_bag.get_dag(dag_id=dag_id)
        # Assert that a DAG hash at least one task.
        assert dag is not None
        assert len(dag.tasks) > 0
