import csv
import json
from datetime import datetime, timedelta

import boto3
import requests
import snowflake.connector
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator

from airflow import DAG

# 멜론 차트 API 정보
_APP_VERSION = "6.5.8.1"
_CP_ID = "AS40"
_USER_AGENT = f"{_CP_ID}; Android 13; {_APP_VERSION}; sdk_gphone64_arm64"
_CHART_API_URL = f"https://m2.melon.com/m6/chart/ent/songChartList.json?cpId={_CP_ID}&cpKey=14LNC3&appVer={_APP_VERSION}"

# 파일 저장 경로
TODAY = datetime.now().strftime("%Y%m%d")
JSON_PATH = f"/opt/airflow/data/melon_chart_{TODAY}.json"
CSV_PATH = f"/opt/airflow/data/melon_chart_{TODAY}.csv"
S3_JSON_KEY = f"raw_data/melon_chart/melon_chart_{TODAY}.json"
S3_CSV_KEY = f"raw_data/melon_chart/melon_chart_{TODAY}.csv"


# 1. 멜론 차트 데이터 가져오기
def fetch_melon_chart():
    headers = {"User-Agent": _USER_AGENT}
    response = requests.get(_CHART_API_URL, headers=headers)
    if response.status_code != 200:
        raise Exception(f"멜론 API 호출 실패: {response.status_code}")
    data = response.json()

    chart_data = {
        "date": f"{data['response']['RANKDAY']} {data['response']['RANKHOUR']}:00",
        "entries": [
            {
                "rank": int(song["CURRANK"]),
                "title": song["SONGNAME"],
                "artist": song["ARTISTLIST"][0]["ARTISTNAME"],
                "lastPos": int(song["PASTRANK"]),
                "peakPos": int(song.get("PEAKRANK", song["CURRANK"])),
                "isNew": song["RANKTYPE"] == "NEW",
                "image": song["ALBUMIMG"],
            }
            for song in data["response"]["SONGLIST"]
        ],
    }

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)
    print(f"✅ JSON 저장 완료: {JSON_PATH}")
    return JSON_PATH


# 2. JSON → CSV 변환
def convert_json_to_csv():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    fields = [
        "rank",
        "title",
        "artist",
        "lastPos",
        "peakPos",
        "isNew",
        "image"]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for entry in data["entries"]:
            writer.writerow(entry)
    print(f"✅ CSV 변환 완료: {CSV_PATH}")
    return CSV_PATH


# 3. AWS S3 업로드
def upload_to_s3():
    # AWS 연결 정보 가져오기
    aws_connection = BaseHook.get_connection(
        "S4tify_S3"
    )  # Airflow에서 설정한 AWS 연결 사용
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_connection.login,
        aws_secret_access_key=aws_connection.password,
        region_name=aws_connection.extra_dejson.get("region_name"),
    )
    # s3.upload_file(JSON_PATH, aws_connection.schema, S3_JSON_KEY)
    s3.upload_file(CSV_PATH, "de5-s4tify", S3_CSV_KEY)
    print(f"✅ S3 업로드 완료: {S3_JSON_KEY}, {S3_CSV_KEY}")


"""
# 4. Snowflake 업로드
def upload_to_snowflake():
    # Snowflake 연결 정보 가져오기
    snowflake_connection = BaseHook.get_connection("S4tify_SnowFlake")  # Airflow에서 설정한 Snowflake 연결 사용
    conn = snowflake.connector.connect(
        user=snowflake_connection.login,
        password=snowflake_connection.password,
        account=snowflake_connection.host,
        warehouse=snowflake_connection.extra_dejson.get("warehouse"),
        database=snowflake_connection.extra_dejson.get("database"),
        schema=snowflake_connection.extra_dejson.get("schema")
    )
    cur = conn.cursor()
    #cur.execute(f"""
#    COPY INTO {snowflake_connection.extra_dejson.get('database')}.{snowflake_connection.extra_dejson.get('schema')}.melon_chart
#    FROM @"{snowflake_connection.extra_dejson.get('stage')}"
#    FILES = ('{S3_CSV_KEY}')
#    FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"')
# """)
#    conn.commit()
#    print(f"✅ Snowflake 업로드 완료: {S3_CSV_KEY}")
# """

# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "melon_chart_pipeline",
    default_args=default_args,
    description="멜론 차트를 JSON으로 저장하고 CSV로 변환 후 S3 및 Snowflake에 업로드",
    schedule_interval="0 1 * * *",  # 매일 새벽 1시 실행
    catchup=False,
)

fetch_task = PythonOperator(
    task_id="fetch_melon_chart",
    python_callable=fetch_melon_chart,
    dag=dag,
)

convert_task = PythonOperator(
    task_id="convert_json_to_csv",
    python_callable=convert_json_to_csv,
    dag=dag,
)

s3_upload_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)
"""
snowflake_upload_task = PythonOperator(
    task_id="upload_to_snowflake",
    python_callable=upload_to_snowflake,
    dag=dag,
)"""

fetch_task >> convert_task >> s3_upload_task  # snowflake_upload_task
