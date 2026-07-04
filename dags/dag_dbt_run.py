from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='dbt_transform_dag',
    start_date=datetime(2026, 7, 4),
    schedule='30 0 * * *',  # Roda às 00:30 (meia hora depois da extração diária)
    catchup=False,
    tags=["tmdb", "dbt", "transformation"]
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --project-dir /opt/airflow/dbt/tmdb_project --profiles-dir /opt/airflow/dbt/tmdb_project",
    )
