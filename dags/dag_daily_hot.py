# pyrefly: ignore [missing-import]
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
# Ensina o Python do Airflow a olhar para a pasta raiz (/opt/airflow) para que ele consiga enxergar a pasta "extraction"
sys.path.insert(0, '/opt/airflow')
from extraction.load_trending import main as load_trending_movies_func

with DAG('daily_dag', 
        start_date = datetime(2027,7,4),
        schedule = '0 0 * * *',  # todo dia meia noite
        catchup = False) as dag:

    load_movies_trending = PythonOperator(
        task_id = "load_movies_trending",
        python_callable = load_trending_movies_func
    )