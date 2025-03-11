from datetime import datetime, timedelta

from scripts.crawling_spotify_data import *
from scripts.load_spotify_data import *
from scripts.request_spotify_api import *

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 28),
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
}


with DAG(
    dag_id="GetSpotifyArtistData",
    default_args=default_args,
    catchup=False,
    tags=["final_project"],
    schedule_interval="0 11 * * *",
) as dag:

    extract_globalTop50_data = PythonOperator(
        task_id="extract_global_top50",
        python_callable=data_crawling,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    extract_artistInfo_data = PythonOperator(
        task_id="extract_artist_info",
        python_callable=get_artist_info,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    extract_artistTop10_data = PythonOperator(
        task_id="extract_artist_top10",
        python_callable=get_arti_top10,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    extract_globalTop50_data >> [
        extract_artistInfo_data,
        extract_artistTop10_data]
