import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

from dags.plugins.variables import SPARK_JARS

# DAG 설정
default_args = {
    "owner": "sanghyoek_boo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

dag = DAG(
    dag_id="ELT_eventsim_song_artist_count",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["ELT", "Eventsim"],
)

trigger_dag_task = TriggerDagRunOperator(
    task_id="trigger_eventsim_etl",
    trigger_dag_id="eventsim_ETL",  # 실행할 대상 DAG ID
    wait_for_completion=True,  # 완료될 때까지 대기
    poke_interval=10,  # DAG 상태 체크 주기 (초 단위)
    dag=dag,
)

spark_submit_task = SparkSubmitOperator(
    task_id="process_songs_and_artists_spark",
    application="dags/scripts/ELT_eventsim_script.py",
    conn_id="spark_conn",  
    executor_memory="2g",
    driver_memory="1g",
    jars=SPARK_JARS,
    dag=dag,
)

trigger_dag_task >> spark_submit_task
