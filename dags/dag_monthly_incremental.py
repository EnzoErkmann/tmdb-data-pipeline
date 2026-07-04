# pyrefly: ignore [missing-import]
import sys
import os
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Ensina o Python do Airflow a olhar para a pasta raiz (/opt/airflow) para que ele consiga enxergar a pasta "extraction"
sys.path.insert(0, '/opt/airflow')
from extraction.load_movie_details import main as load_monthly_movies_func

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")
BQ_PROJECT = os.getenv("GCP_PROJECT_ID")

with DAG(
    dag_id='monthly_dag', 
    start_date=datetime(2027, 7, 4),
    schedule='0 0 1 * *',  # todo dia 1 do mês meia noite
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
        source_objects=["raw/movies/*/*.json"], # Usando wildcard para capturar o que foi gerado
        destination_project_dataset_table=f"{BQ_PROJECT}.bronze.raw_movies",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE", # Tratando como snapshot full por enquanto (atualiza a tabela com o último estado de todos os filmes da pasta)
        autodetect=True,
    )

    credits_to_bq = GCSToBigQueryOperator(
        task_id="credits_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["raw/credits/*/*.json"],
        destination_project_dataset_table=f"{BQ_PROJECT}.bronze.raw_credits",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    load_movies_monthly >> [movies_to_bq, credits_to_bq]