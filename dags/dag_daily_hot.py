# pyrefly: ignore [missing-import]
import sys
import os
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Ensina o Python do Airflow a olhar para a pasta raiz (/opt/airflow) para que ele consiga enxergar a pasta "extraction"
sys.path.insert(0, '/opt/airflow')
from extraction.load_trending import main as load_trending_movies_func

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")
BQ_PROJECT = os.getenv("GCP_PROJECT_ID")

with DAG(
    dag_id='daily_dag', 
    start_date=datetime(2026, 7, 4),
    schedule='0 6 * * *',  # todo dia às 06:00 da manhã
    catchup=False,
    tags=["tmdb", "daily"]
) as dag:

    load_movies_trending = PythonOperator(
        task_id="load_movies_trending",
        python_callable=load_trending_movies_func
    )

    trending_to_bq = GCSToBigQueryOperator(
        task_id="trending_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["raw/trending/{{ ds }}.json"],
        destination_project_dataset_table=f"{BQ_PROJECT}.bronze.raw_trending",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    popular_to_bq = GCSToBigQueryOperator(
        task_id="popular_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["raw/popular/{{ ds }}.json"],
        destination_project_dataset_table=f"{BQ_PROJECT}.bronze.raw_popular",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    now_playing_to_bq = GCSToBigQueryOperator(
        task_id="now_playing_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["raw/now_playing/{{ ds }}.json"],
        destination_project_dataset_table=f"{BQ_PROJECT}.bronze.raw_now_playing",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    load_movies_trending >> [trending_to_bq, popular_to_bq, now_playing_to_bq]