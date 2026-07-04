# pyrefly: ignore [missing-import]
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
# Ensina o Python do Airflow a olhar para a pasta raiz (/opt/airflow) para que ele consiga enxergar a pasta "extraction"
sys.path.insert(0, '/opt/airflow')
from extraction.load_movies_id import main as load_movies_id_func
from extraction.load_genres import main as load_genres_func

with DAG('historic_dag', 
        start_date = datetime(2026,7,3), 
        catchup = False) as dag:

    load_movies_id = PythonOperator(
        task_id = "load_movies_id",
        python_callable = load_movies_id_func
    )

    load_genres = PythonOperator(
        task_id = "load_genres",
        python_callable = load_genres_func
    )