import csv
import io
import json
from datetime import datetime, timedelta

import requests
from plugins.get_artist_data import get_artist_genre, search_artist_id
from plugins.vibe import ChartData  # vibe.py 모듈 import
from scripts.get_access_token import get_token

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 날짜 설정
TODAY = datetime.now().strftime("%Y-%m-%d")

# S3 설정
S3_BUCKET = "de5-s4tify"
S3_CSV_KEY = f"raw_data/vibe_chart_data/vibe_chart_{TODAY}.csv"
LOCAL_FILE_PATH = f"/opt/airflow/data/vibe_chart_with_genre_{TODAY}.csv"


# 1. VIBE 차트 데이터 가져오기 및 JSON 변환
def fetch_vibe_chart():
    chart = ChartData(fetch=True)
    chart_data = {"date": chart.date.strftime(
        "%Y-%m-%d %H:%M:%S"), "entries": []}
    for entry in chart.entries:
        print(f"📊 차트 데이터 처리: {entry.rank}. {entry.title} - {entry.artist}")
        artist_id = search_artist_id(entry.artist)
        genre = get_artist_genre(artist_id)
        chart_data["entries"].append(
            {
                "rank": entry.rank,
                "title": entry.title,
                "artist": entry.artist,
                "lastPos": entry.lastPos,
                "isNew": entry.isNew,
                "image": entry.image,
                "genres": genre.split(", ") if genre else [],
            }
        )
    return chart_data


# 2. JSON → CSV 변환
def convert_json_to_csv(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="fetch_vibe_chart")

    output = io.StringIO()
    writer = csv.writer(
        output, quoting=csv.QUOTE_ALL
    )  # ✅ 모든 필드를 자동으로 따옴표 처리

    # 헤더 추가
    writer.writerow(["rank", "title", "artist", "lastPos",
                     "isNew", "image", "genre", "date"])

    # 데이터 추가
    for entry in data["entries"]:
        # 리스트 그대로 저장 (genres는 리스트로 저장)
        genres = entry["genres"]  # 수정된 부분: 리스트 그대로 저장

        # CSV에 추가
        writer.writerow(
            [
                entry["rank"],
                entry["title"],
                entry["artist"],
                entry["lastPos"],
                entry["isNew"],
                entry["image"],
                genres,  # 수정된 부분: 리스트 그대로 저장
                TODAY,
            ]
        )

    return output.getvalue()


# 3. 로컬에 CSV 저장 (테스트용)
def save_csv_locally(csv_string):
    with open(LOCAL_FILE_PATH, "w", encoding="utf-8") as f:
        f.write(csv_string)


# 4. AWS S3 업로드
def upload_to_s3(**kwargs):
    ti = kwargs["ti"]
    csv_string = ti.xcom_pull(task_ids="convert_json_to_csv")
    save_csv_locally(csv_string)  # 테스트용 로컬 저장
    s3_hook = S3Hook(aws_conn_id="S4tify_S3")
    s3_hook.load_string(
        csv_string,
        key=S3_CSV_KEY,
        bucket_name=S3_BUCKET,
        replace=True)
    print(f"✅ S3 업로드 완료: {S3_CSV_KEY}")


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

    get_spotify_token_task = PythonOperator(
        task_id="get_spotify_token",
        python_callable=get_token,  # ✅ 먼저 실행해서 Variable 갱신
        provide_context=True,
    )

    fetch_vibe_chart_task = PythonOperator(
        task_id="fetch_vibe_chart",
        python_callable=fetch_vibe_chart,
        provide_context=True,
    )

    convert_json_to_csv_task = PythonOperator(
        task_id="convert_json_to_csv",
        python_callable=convert_json_to_csv,
        provide_context=True,
    )

    upload_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )

    (
        get_spotify_token_task
        >> fetch_vibe_chart_task
        >> convert_json_to_csv_task
        >> upload_s3_task
    )
