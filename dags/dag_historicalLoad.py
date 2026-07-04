import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

sys.path.insert(0, '/opt/airflow')
from extraction.load_movies_id import main as load_movies_id_func
from extraction.load_genres import main as load_genres_func

import os

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")
BQ_PROJECT = os.getenv("GCP_PROJECT_ID")

with DAG(
    dag_id="historic_dag",
    start_date=datetime(2026, 7, 3),
    schedule=None,
    catchup=False,
    tags=["tmdb", "historical"]
) as dag:

    load_movies_id = PythonOperator(
        task_id="load_movies_id",
        python_callable=load_movies_id_func
    )

    load_genres = PythonOperator(
        task_id="load_genres",
        python_callable=load_genres_func
    )

    movies_id_to_bq = GCSToBigQueryOperator(
        task_id="movies_id_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["raw/movie_ids/*.json"],
        destination_project_dataset_table=f"{BQ_PROJECT}.bronze.raw_movie_ids",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE", # A exportação do TMDB é full, então truncate é o ideal
        autodetect=True,
    )

    genres_to_bq = GCSToBigQueryOperator(
        task_id="genres_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["raw/genres/genres.json"],
        destination_project_dataset_table=f"{BQ_PROJECT}.bronze.raw_genres",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    load_movies_id >> movies_id_to_bq
    load_genres >> genres_to_bq