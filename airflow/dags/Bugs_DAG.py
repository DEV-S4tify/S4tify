import csv
import json
from datetime import datetime, timedelta

import pandas as pd
from bugs import BugsChartPeriod, BugsChartType, ChartData

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

"""
your-s3-bucket-name을 실제 S3 버킷명으로 바꾸고,
✅ Snowflake 연결 정보 및 테이블명을 맞게 설정
"""

# 날짜 설정
TODAY = datetime.now().strftime("%Y%m%d")

# 파일 경로
JSON_PATH = f"/opt/airflow/data/bugs_chart_{TODAY}.json"
CSV_PATH = f"/opt/airflow/data/bugs_chart_{TODAY}.csv"

# S3 설정
S3_BUCKET = "de5-s4tify"
S3_JSON_KEY = f"raw_data/bugs_chart/bugs_chart_{TODAY}.json"
S3_CSV_KEY = f"raw_data/bugs_chart/bugs_chart_{TODAY}.csv"

"""
# Snowflake 설정
SNOWFLAKE_CONN_ID = "S4tify_SnowFlake"
SNOWFLAKE_TABLE = "raw_data"
"""


# 1. Bugs 차트 데이터 가져오기 및 JSON 저장
def fetch_bugs_chart():
    chart = ChartData(
        chartType=BugsChartType.All,
        chartPeriod=BugsChartPeriod.Realtime,
        fetch=True)
    chart_data = {
        "date": chart.date.strftime("%Y-%m-%d %H:%M:%S"),
        "entries": [
            {
                "rank": entry.rank,
                "title": entry.title,
                "artist": entry.artist,
                "lastPos": entry.lastPos,
                "peakPos": entry.peakPos,
                "image": entry.image,
            }
            for entry in chart.entries
        ],
    }
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)
    print(f"✅ JSON 저장 완료: {JSON_PATH}")


# 2. JSON → CSV 변환
def convert_json_to_csv():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    fields = ["rank", "title", "artist", "lastPos", "peakPos", "image"]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for entry in data["entries"]:
            writer.writerow(entry)
    print(f"✅ CSV 변환 완료: {CSV_PATH}")


# 3. AWS S3 업로드
def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id="S4tify_S3")
    # s3_hook.load_file(filename=JSON_PATH, key=S3_JSON_KEY, bucket_name=S3_BUCKET, replace=True)
    s3_hook.load_file(
        filename=CSV_PATH, key=S3_CSV_KEY, bucket_name=S3_BUCKET, replace=True
    )
    print(f"✅ S3 업로드 완료: {S3_CSV_KEY}")


"""# 4. Snowflake 업로드
def upload_to_snowflake():
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    df = pd.read_csv(CSV_PATH)
    snowflake_hook.run(f"DELETE FROM {SNOWFLAKE_TABLE} WHERE DATE = '{TODAY}';")
    snowflake_hook.insert_rows(table=SNOWFLAKE_TABLE, rows=df.values.tolist(), target_fields=df.columns.tolist())
    print(f"✅ Snowflake 업로드 완료: {SNOWFLAKE_TABLE}")
"""
# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "bugs_chart_dag",
    default_args=default_args,
    schedule_interval="10 0 * * *",  # 매일 00:10 실행
    catchup=False,
) as dag:

    fetch_bugs_chart_task = PythonOperator(
        task_id="fetch_bugs_chart",
        python_callable=fetch_bugs_chart,
    )

    convert_json_to_csv_task = PythonOperator(
        task_id="convert_json_to_csv",
        python_callable=convert_json_to_csv,
    )

    upload_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )
    """
    upload_snowflake_task = PythonOperator(
        task_id="upload_to_snowflake",
        python_callable=upload_to_snowflake,
    )
    """

    # DAG 실행 순서
    (
        fetch_bugs_chart_task >> convert_json_to_csv_task >> upload_s3_task
    )  # upload_snowflake_task
