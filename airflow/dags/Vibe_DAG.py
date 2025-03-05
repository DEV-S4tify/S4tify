import csv
import json
from datetime import datetime, timedelta

import snowflake.connector
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from vibe import ChartData  # vibe.py 모듈 import

from airflow import DAG

# 파일 경로 및 S3 버킷 정보
TODAY = datetime.now().strftime("%Y%m%d")
JSON_PATH = f"/opt/airflow/data/vibe_chart_{TODAY}.json"
CSV_PATH = f"/opt/airflow/data/vibe_chart_{TODAY}.csv"
S3_BUCKET = "de5-s4tify"
S3_JSON_KEY = f"raw_data/vibe_chart/vibe_chart_{TODAY}.json"
S3_CSV_KEY = f"raw_data/vibe_chart/vibe_chart_{TODAY}.csv"


# AWS S3 업로드 함수
def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id="S4tify_S3")  # Airflow Connection ID 사용
    file_name = CSV_PATH.split("/")[-1]
    s3_hook.load_file(
        filename=CSV_PATH, bucket_name=S3_BUCKET, key=file_name, replace=True
    )
    print(f"✅ S3 업로드 완료: s3://{S3_BUCKET}/{file_name}")


"""
# Snowflake 저장 함수
def save_to_snowflake():
    connection = BaseHook.get_connection('S4tify_SnowFlake')  # Snowflake 연결 가져오기

    # Snowflake 연결 생성
    conn = snowflake.connector.connect(
        user=connection.login,
        password=connection.password,
        account=connection.host,
        warehouse=connection.extra_dejson.get('warehouse'),
        database=connection.extra_dejson.get('database'),
        schema=connection.extra_dejson.get('schema'),
    )

    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM vibe_chart WHERE date = '{TODAY}'")
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        next(f)  # 헤더 스킵
        for line in f:
            values = line.strip().split(",")
            cursor.execute(
                "INSERT INTO vibe_chart (rank, title, artist, lastPos, isNew, image, date) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (*values, TODAY)
            )
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Snowflake 저장 완료")
"""


# Vibe 차트 데이터를 가져와 JSON으로 저장하는 함수
def fetch_vibe_chart():
    chart = ChartData(fetch=True)

    chart_data = {
        "date": chart.date.strftime("%Y-%m-%d %H:%M:%S"),
        "entries": [
            {
                "rank": entry.rank,
                "title": entry.title,
                "artist": entry.artist,
                "lastPos": entry.lastPos,
                "isNew": entry.isNew,
                "image": entry.image,
            }
            for entry in chart.entries
        ],
    }

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)

    print(f"✅ JSON 저장 완료: {JSON_PATH}")
    return JSON_PATH


# JSON을 CSV로 변환하는 함수
def convert_json_to_csv():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        chart_data = json.load(f)

    fields = ["rank", "title", "artist", "lastPos", "isNew", "image"]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for entry in chart_data["entries"]:
            writer.writerow(entry)

    print(f"✅ CSV 변환 완료: {CSV_PATH}")
    return CSV_PATH


# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "vibe_chart_dag",
    default_args=default_args,
    schedule_interval="45 0 * * *",  # 매일 00:45 실행
    catchup=False,
) as dag:

    fetch_vibe_chart_task = PythonOperator(
        task_id="fetch_vibe_chart",
        python_callable=fetch_vibe_chart,
    )

    convert_json_to_csv_task = PythonOperator(
        task_id="convert_json_to_csv",
        python_callable=convert_json_to_csv,
    )

    """upload_json_to_s3_task = PythonOperator(
        task_id="upload_json_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={"file_path": JSON_PATH, "bucket_name": S3_BUCKET, "object_name": S3_JSON_KEY},
    )"""

    upload_csv_to_s3_task = PythonOperator(
        task_id="upload_csv_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "file_path": CSV_PATH,
            "bucket_name": S3_BUCKET,
            "object_name": S3_CSV_KEY,
        },
    )

    """save_to_snowflake_task = PythonOperator(
        task_id="save_to_snowflake",
        python_callable=save_to_snowflake,
    )"""

    # 작업 순서 정의
    (fetch_vibe_chart_task >> convert_json_to_csv_task >>
     upload_csv_to_s3_task)  # save_to_snowflake_task
