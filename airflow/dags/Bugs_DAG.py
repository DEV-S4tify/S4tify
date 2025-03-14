import csv
import io
import json
from datetime import datetime, timedelta

import requests
from plugins.bugs import BugsChartPeriod, BugsChartType, ChartData
from plugins.get_artist_data import get_artist_genre, search_artist_id
from scripts.get_access_token import get_token

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 날짜 설정
TODAY = datetime.now().strftime("%Y-%m-%d")


S3_BUCKET = "de5-s4tify"
S3_CSV_KEY = f"raw_data/bugs_chart_data/bugs_chart_{TODAY}.csv"
LOCAL_FILE_PATH = f"/opt/airflow/data/bugs_chart_with_genre_{TODAY}.csv"


# 1. Bugs 차트 데이터 가져오기 및 JSON 변환
def fetch_bugs_chart():
    chart = ChartData(
        chartType=BugsChartType.All,
        chartPeriod=BugsChartPeriod.Realtime,
        fetch=True)
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
                "peakPos": entry.peakPos, 
                "image": entry.image,
                "genres": genre.split(", ") if genre else [],  # ✅ 리스트 변환,
            }
        )
    return chart_data


# 2. JSON → CSV 변환 (genre를 리스트로 저장)
def convert_json_to_csv(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="fetch_bugs_chart")

    output = io.StringIO()
    writer = csv.writer(
        output, quoting=csv.QUOTE_ALL
    )  # ✅ 모든 필드를 자동으로 따옴표 처리

    # 헤더 추가
    writer.writerow(["rank", "title", "artist", "lastPos",
                    "peakPos", "image", "genre", "date"])

    # 데이터 추가
    for entry in data["entries"]:
        # 리스트 그대로 저장
        genres = entry["genres"]

        # CSV에 추가
        writer.writerow(
            [
                entry["rank"],
                entry["title"],
                entry["artist"],
                entry["lastPos"],
                entry["peakPos"],
                entry["image"],
                genres,  # 수정된 부분: 리스트 그대로 저장
                TODAY,
            ]
        )

    return output.getvalue()


# 3. 로컬에 CSV 저장 (테스트용, 삭제 용이하도록 별도 함수)
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
    "bugs_chart_dag",
    default_args=default_args,
    schedule_interval="10 0 * * *",  # 매일 00:10 실행
    catchup=True,
) as dag:

    get_spotify_token_task = PythonOperator(
        task_id="get_spotify_token",
        python_callable=get_token,  # ✅ 먼저 실행해서 Variable 갱신
        provide_context=True,
    )

    fetch_bugs_chart_task = PythonOperator(
        task_id="fetch_bugs_chart",
        python_callable=fetch_bugs_chart,
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
        >> fetch_bugs_chart_task
        >> convert_json_to_csv_task
        >> upload_s3_task
    )
