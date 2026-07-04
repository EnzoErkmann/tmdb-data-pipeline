# pyrefly: ignore [missing-import]
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
# Ensina o Python do Airflow a olhar para a pasta raiz (/opt/airflow) para que ele consiga enxergar a pasta "extraction"
sys.path.insert(0, '/opt/airflow')
from extraction.load_movie_details import main as load_monthly_movies_func

with DAG('monthly_dag', 
        start_date = datetime(2027,7,4),
        schedule = '0 0 1 * *',  # todo dia 1 do mês meia noite
        catchup = False) as dag:

    load_movies_monthly = PythonOperator(
        task_id = "load_monthly_movies",
        python_callable = load_monthly_movies_func
    )