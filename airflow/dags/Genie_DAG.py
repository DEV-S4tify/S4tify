from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import json
import csv
import pandas as pd
from genie import ChartData, GenieChartPeriod  # genie.py 모듈 import
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 환경 변수 설정
TODAY = datetime.now().strftime("%Y%m%d")
JSON_PATH = f"/opt/airflow/data/genie_chart_{TODAY}.json"
CSV_PATH = f"/opt/airflow/data/genie_chart_{TODAY}.csv"
S3_BUCKET = "de5-s4tify"
S3_CSV_KEY = f"raw_data/genie_chart/genie_chart_{TODAY}.csv"
#SNOWFLAKE_CONN_ID = "S4tify_SnowFlake"  # Airflow에서 설정한 Snowflake 연결 ID

# Genie 차트 데이터를 가져와 JSON으로 저장
def fetch_genie_chart():
    chart = ChartData(chartPeriod=GenieChartPeriod.Realtime, fetch=True)

    chart_data = {
        "date": chart.date.strftime("%Y-%m-%d %H:%M:%S"),
        "entries": [
            {
                "rank": entry.rank,
                "title": entry.title,
                "artist": entry.artist,
                "peakPos": entry.peakPos,
                "lastPos": entry.lastPos,
                "image": entry.image
            }
            for entry in chart.entries
        ]
    }

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)

    print(f"✅ JSON 저장 완료: {JSON_PATH}")
    return JSON_PATH

# JSON을 CSV로 변환 후 저장
def convert_json_to_csv():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        chart_data = json.load(f)

    fields = ["rank", "title", "artist", "peakPos", "lastPos", "image"]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for entry in chart_data["entries"]:
            writer.writerow(entry)

    print(f"✅ CSV 변환 완료: {CSV_PATH}")
    return CSV_PATH

# S3 업로드
def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id="S4tify_S3")  # Airflow Connection ID 사용
    file_name = CSV_PATH.split("/")[-1]
    s3_hook.load_file(filename=CSV_PATH, bucket_name=S3_BUCKET, key=S3_CSV_KEY, replace=True)
    print(f"✅ S3 업로드 완료: s3://{S3_BUCKET}/{S3_CSV_KEY}")

"""
# Snowflake 업로드 (Airflow SnowflakeHook 사용)
def upload_to_snowflake():
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Snowflake에 CSV 파일 업로드
    df = pd.read_csv(CSV_PATH)
    
    # Snowflake 테이블에 데이터를 삽입
    # 'genie_chart' 테이블에 맞는 컬럼 이름과 데이터 형식을 확인하고 매핑
    snowflake_hook.run(f"DELETE FROM genie_chart WHERE date = '{TODAY}';")
    
    # Snowflake 테이블에 데이터 삽입
    snowflake_hook.insert_rows(
        table="genie_chart", 
        rows=df.values.tolist(), 
        target_fields=df.columns.tolist()
    )
    
    print(f"✅ Snowflake 업로드 완료: genie_chart")
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
    "genie_chart_dag",
    default_args=default_args,
    schedule_interval="30 0 * * *",  # 매일 00:30 실행
    catchup=False,
) as dag:

    fetch_genie_chart_task = PythonOperator(
        task_id="fetch_genie_chart",
        python_callable=fetch_genie_chart,
    )

    convert_json_to_csv_task = PythonOperator(
        task_id="convert_json_to_csv",
        python_callable=convert_json_to_csv,
    )

    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )
    """
    upload_to_snowflake_task = PythonOperator(
        task_id="upload_to_snowflake",
        python_callable=upload_to_snowflake,
    )"""

    fetch_genie_chart_task >> convert_json_to_csv_task >> upload_to_s3_task #upload_to_snowflake_task
