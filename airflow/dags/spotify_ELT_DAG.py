from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

from scripts.crawling_spotify_data import *
from scripts.request_spotify_api import *
from scripts.load_spotify_data import *

#DAG 기본 설정
default_args = {
    'owner': 'yerin',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    dag_id='SpotifyDataProcessing',
    default_args=default_args,
    catchup=False,
    tags=['final_project'],
    schedule='0 12 * * *'
) as dag:
    
    artist_info_Top10_table = SparkSubmitOperator(
        task_id = 'artist_info_top10_table',
        application='dags/scripts/ELT_artist_info_top10.py',
        conn_id='spark_default',
        dag=dag
    )
    
    artist_info_globalTop50_table = SparkSubmitOperator(
        task_id = 'artist_info_globalTop50_table',
        application='dags/scripts/ELT_artist_info_globalTop50.py',
        conn_id='spark_default',
        dag=dag
    )
    
    [artist_info_Top10_table ,  artist_info_globalTop50_table]