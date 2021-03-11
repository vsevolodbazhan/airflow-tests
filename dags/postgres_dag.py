from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="postgres_dag",
    description="DAG for experimenting with tests that involve external systems.",
    start_date=days_ago(n=1),
) as dag:
    retrieve_cars = PostgresOperator(
        task_id="retrieve_cars",
        postgres_conn_id="postgres_default",
        sql="SELECT * FROM cars;",
    )
