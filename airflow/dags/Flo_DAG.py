import csv
import json
from datetime import datetime, timedelta

from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from flo import ChartData  # flo.py 모듈 import

from airflow import DAG

# 파일 경로 및 S3 버킷 정보
TODAY = datetime.now().strftime("%Y%m%d")
JSON_PATH = f"/opt/airflow/data/flo_chart_{TODAY}.json"
CSV_PATH = f"/opt/airflow/data/flo_chart_{TODAY}.csv"
S3_BUCKET = "de5-s4tify"
S3_JSON_KEY = f"raw_data/flo_chart/flo_chart_{TODAY}.json"
S3_CSV_KEY = f"raw_data/flo_chart/flo_chart_{TODAY}.csv"


# AWS S3 업로드 함수
def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id="S4tify_S3")  # Airflow Connection ID 사용
    file_name = CSV_PATH.split("/")[-1]
    s3_hook.load_file(
        filename=CSV_PATH, bucket_name=S3_BUCKET, key=file_name, replace=True
    )
    print(f"✅ S3 업로드 완료: s3://{S3_BUCKET}/{file_name}")


"""# Snowflake 저장 함수
def save_to_snowflake():
    # Airflow 연결에서 Snowflake 연결 정보 가져오기
    connection = BaseHook.get_connection('s4tify_SnowFlake')  # Airflow에 설정된 Snowflake 연결 정보 사용

    # Snowflake 연결 생성
    conn = snowflake.connector.connect(
        user=connection.login,  # Airflow 연결의 username 사용
        password=connection.password,  # Airflow 연결의 password 사용
        account=connection.host,  # Airflow 연결의 account 정보 사용
        warehouse=connection.extra_dejson.get('warehouse'),  # Extra 정보에서 warehouse 읽기
        database=connection.extra_dejson.get('database'),  # Extra 정보에서 database 읽기
        schema=connection.extra_dejson.get('schema'),  # Extra 정보에서 schema 읽기
    )

    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM flo_chart WHERE date = '{TODAY}'")
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        next(f)  # 헤더 스킵
        for line in f:
            values = line.strip().split(",")
            cursor.execute(
                "INSERT INTO flo_chart (rank, title, artist, lastPos, isNew, image, date) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (*values, TODAY)
            )
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Snowflake 저장 완료")
"""


# 1. FLO 차트 데이터 가져오기 및 JSON 저장
def fetch_flo_chart():
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


# 2. JSON → CSV 변환
def convert_json_to_csv():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    fields = ["rank", "title", "artist", "lastPos", "isNew", "image"]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for entry in data["entries"]:
            writer.writerow(entry)
    print(f"✅ CSV 변환 완료: {CSV_PATH}")


# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "flo_chart_dag",
    default_args=default_args,
    schedule_interval="20 0 * * *",  # 매일 00:20 실행
    catchup=False,
) as dag:

    fetch_flo_chart_task = PythonOperator(
        task_id="fetch_flo_chart",
        python_callable=fetch_flo_chart,
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
    (fetch_flo_chart_task >> convert_json_to_csv_task >>
     upload_csv_to_s3_task)  # save_to_snowflake_task
