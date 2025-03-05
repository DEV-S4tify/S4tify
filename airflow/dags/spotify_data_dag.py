from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from scripts.crawling_spotify_data import *
from scripts.join_spotify_data import *
from scripts.request_spotify_api import *

from airflow import DAG

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 28),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="GetSpotifyArtistData",
    default_args=default_args,
    catchup=False,
    tags=["final_project"],
    schedule_interval="0 11 * * *",
) as dag:

    extract_globalTop50_data = PythonOperator(
        task_id="crawling_global_top50", python_callable=data_crawling
    )

    transformation_data = PythonOperator(
        task_id="join_data",
        python_callable=read_and_merge,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_s3_bucket)

    extract_globalTop50_data >> transformation_data >> load_data
