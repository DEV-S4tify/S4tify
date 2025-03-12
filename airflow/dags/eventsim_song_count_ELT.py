from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SOURCE_TABLE = 'RAW_DATA.EVENTSIM_LOG'
TARGET_TABLE = 'ANALYTICS.EVENTSIM_SONG_COUNTS'

SQL_QUERY = f"""
    CREATE OR REPLACE TABLE {TARGET_TABLE} AS
    SELECT SONG, ARTIST, COUNT(*) AS song_count
    FROM {SOURCE_TABLE}
    GROUP BY SONG, ARTIST
    ORDER BY song_count DESC;
"""

default_args = {
    'owner': 'sanghyoek_boo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id = "evetnsim_song_count_ELT",
    default_args = default_args,
    schedule_interval = "@daily",
    catchup = False,
    tags=["ELT", "Eventsim", "Song Count"]
)

trigger_dag_task = TriggerDagRunOperator(
    task_id = 'trigger_task',
    trigger_dag_id = 'eventsim_ETL',
    wait_for_completion=True,
    poke_interval=10,
    dag=dag,
)

run_snowflake_query = SnowflakeOperator (
    task_id = "aggregate_song_counts",
    sql = SQL_QUERY,
    snowflake_conn_id = "snowflake_conn",
    dag=dag,
)

if __name__ == "__main__":
    trigger_dag_task