import os
from datetime import timedelta

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from spark_utils import execute_snowflake_query

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

load_dotenv()

# SNOWFLAKE 설정
SNOWFLAKE_SOURCE_TABLE = "EVENTSIM_LOG"
SNOWFLAKE_SOURCE_SCHEMA = os.environ.get("SNOWFLAKE_SILVER_SCHMEA", "RAW_DATA")
SNOWFLAKE_TARGET_SONG_TABLE = "EVENTSIM_SONG_COUNTS"
SNOWFLAKE_TARGET_ARTIST_TABLE = "EVENTSIM_ARTIST_COUNTS"
SNOWFLAKE_TARGET_SCHEMA = os.environ.get("SNOWFLAKE_GOLD_SCHEMA", "ANALYTICS")

SNOWFLAKE_PROPERTIES = {
    "user": os.environ.get("SNOWFLAKE_USER_BSH"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD_BSH"),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "db": os.environ.get("SNOWFLAKE_DB", "S4TIFY"),
    "warehouse": os.environ.get("SNOWFLAKE_WH", "COMPUTE_WH"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "ANALYTICS_USERS"),
}


def extract_data_from_snowflake():
    """
    Snowflake에서 데이터를 읽어오는 함수
    """
    SNOWFLAKE_PROPERTIES["schema"] = SNOWFLAKE_SOURCE_SCHEMA

    query = f"""
        SELECT SONG, ARTIST, LOCATION, SESSIONID, USERID, TS
        FROM {SNOWFLAKE_SOURCE_TABLE}
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_PROPERTIES["user"],
            password=SNOWFLAKE_PROPERTIES["password"],
            account=SNOWFLAKE_PROPERTIES["account"],
            database=SNOWFLAKE_PROPERTIES["db"],
            schema=SNOWFLAKE_PROPERTIES["schema"],
            warehouse=SNOWFLAKE_PROPERTIES["warehouse"],
            role=SNOWFLAKE_PROPERTIES["role"],
        )
        cur = conn.cursor()
        rows = cur.execute(query).fetchall()
        columns = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=columns)
        print(df.head(5))
        print("Data successfully extracted from Snowflake!")
        return df

    except Exception as e:
        print(f"Error extracting data from Snowflake: {e}")
        return None

    finally:
        cur.close()
        conn.close()


def process_song_counts(**kwargs):
    SNOWFLAKE_PROPERTIES["schema"] = SNOWFLAKE_TARGET_SCHEMA
    ti = kwargs["ti"]
    df = ti.xcom_pull(task_ids="extract_data")

    if df is None or df.empty:
        raise AirflowFailException(
            "No data available for processing song counts.")

    # 노래 카운트 계산
    song_counts = df.groupby(
        ["SONG", "ARTIST"]).size().reset_index(name="song_count")

    # 테이블 비우기
    execute_snowflake_query(
        f"TRUNCATE TABLE {SNOWFLAKE_TARGET_SONG_TABLE};", SNOWFLAKE_PROPERTIES
    )

    # SQL 쿼리 (executemany 사용)
    insert_query = f"""
        INSERT INTO {SNOWFLAKE_TARGET_SONG_TABLE} (SONG, ARTIST, song_count)
        VALUES (%s, %s, %s)
    """

    # DataFrame을 리스트로 변환 후 executemany 실행
    execute_snowflake_query(
        insert_query, SNOWFLAKE_PROPERTIES, data=song_counts.values.tolist()
    )

    print(f"Data written to {SNOWFLAKE_TARGET_SONG_TABLE} in Snowflake.")


def process_artist_counts(**kwargs):
    SNOWFLAKE_PROPERTIES["schema"] = SNOWFLAKE_TARGET_SCHEMA
    ti = kwargs["ti"]
    df = ti.xcom_pull(task_ids="extract_data")
    if df is None or df.empty:
        raise AirflowFailException(
            "No data available for processing artist counts.")

    artist_counts = df.groupby(
        "ARTIST").size().reset_index(name="artist_count")
    execute_snowflake_query(
        f"TRUNCATE TABLE {SNOWFLAKE_TARGET_ARTIST_TABLE};",
        SNOWFLAKE_PROPERTIES)
    insert_query = f"""
        INSERT INTO {SNOWFLAKE_TARGET_ARTIST_TABLE} (ARTIST, artist_count)
        VALUES (%s, %s)
    """
    execute_snowflake_query(
        insert_query, SNOWFLAKE_PROPERTIES, artist_counts.values.tolist()
    )
    print(f"Data written to {SNOWFLAKE_TARGET_ARTIST_TABLE} in Snowflake.")


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

extract_data_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data_from_snowflake,
    provide_context=True,
    dag=dag,
)

process_song_task = PythonOperator(
    task_id="process_song_counts",
    python_callable=process_song_counts,
    provide_context=True,
    dag=dag,
)

process_artist_task = PythonOperator(
    task_id="process_artist_counts",
    python_callable=process_artist_counts,
    provide_context=True,
    dag=dag,
)

trigger_dag_task >> extract_data_task >> [
    process_song_task, process_artist_task]
