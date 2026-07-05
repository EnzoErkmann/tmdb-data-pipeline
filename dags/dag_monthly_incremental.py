import sys
import os
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Teaches Airflow Python to look at the root folder (/opt/airflow) so it can see the "extraction" module
sys.path.insert(0, '/opt/airflow')
from extraction.load_movie_details import main as load_monthly_movies_func

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")
BQ_PROJECT = os.getenv("GCP_PROJECT_ID")

with DAG(
    dag_id='monthly_dag', 
    start_date=datetime(2026, 6, 4),
    schedule='0 0 1 * *',  # every 1st of the month at midnight
    catchup=False,
    tags=["tmdb", "monthly"]
) as dag:

    load_movies_monthly = PythonOperator(
        task_id="load_monthly_movies",
        python_callable=load_monthly_movies_func
    )

    movies_to_bq = GCSToBigQueryOperator(
        task_id="movies_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["raw/movies/{{ ds }}/*.json"], # BQ does not accept double asterisks (*/*.json), it must be the exact day
        destination_project_dataset_table=f"{BQ_PROJECT}.bronze.raw_movies",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE", # Treating as a full snapshot for now (updates the table with the latest state of all movies in the folder)
        autodetect=True,
    )

    credits_to_bq = GCSToBigQueryOperator(
        task_id="credits_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["raw/credits/{{ ds }}/*.json"],
        destination_project_dataset_table=f"{BQ_PROJECT}.bronze.raw_credits",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    load_movies_monthly >> [movies_to_bq, credits_to_bq]