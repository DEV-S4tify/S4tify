import csv
import json
from datetime import datetime, timedelta

import requests
from plugins.bugs import BugsChartPeriod, BugsChartType, ChartData
from scripts.get_access_token import get_token

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 날짜 설정
TODAY = datetime.now().strftime("%Y%m%d")

# Spotify API 설정
SPOTIFY_API_URL = "https://api.spotify.com/v1"
SPOTIFY_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN", default_var=None)
S3_BUCKET = "de5-s4tify"
S3_CSV_KEY = f"raw_data/bugs_chart_with_genre_{TODAY}.csv"
LOCAL_FILE_PATH = f"/opt/airflow/data/bugs_chart_with_genre_{TODAY}.csv"


# Spotify API에서 아티스트 ID 검색
def search_artist_id(artist_name):
    SPOTIFY_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN", default_var=None)

    url = f"{SPOTIFY_API_URL}/search"
    headers = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}
    params = {"q": artist_name, "type": "artist", "limit": 1}
    response = requests.get(url, headers=headers, params=params)

    print("headers : ", headers)

    if response.status_code == 200:
        artists = response.json().get("artists", {}).get("items", [])
        artist_id = artists[0]["id"] if artists else None
        print(f"🔍 검색된 아티스트: {artist_name} -> ID: {artist_id}")
        return artist_id
    else:
        print(
            f"❌ 아티스트 검색 실패: {artist_name}, 상태 코드: {response.status_code}, 응답: {response.json()}"
        )
    return None


# Spotify API에서 아티스트 장르 가져오기
def get_artist_genre(artist_id):
    if not artist_id:
        return "Unknown"

    SPOTIFY_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN", default_var=None)

    url = f"{SPOTIFY_API_URL}/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        genres = response.json().get("genres", [])
        genre_str = ", ".join(genres) if genres else "Unknown"
        print(f"🎵 장르 검색: ID {artist_id} -> {genre_str}")
        return genre_str
    else:
        print(
            f"❌ 장르 검색 실패: ID {artist_id}, 상태 코드: {response.status_code}, 응답: {response.json()}"
        )
    return "Unknown"


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
                "genre": genre,
            }
        )
    return chart_data


# 2. JSON → CSV 변환
def convert_json_to_csv(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="fetch_bugs_chart")
    csv_data = [["rank", "title", "artist",
                 "lastPos", "peakPos", "image", "genre"]]
    for entry in data["entries"]:
        csv_data.append(
            [
                entry["rank"],
                entry["title"],
                entry["artist"],
                entry["lastPos"],
                entry["peakPos"],
                entry["image"],
                entry["genre"],
            ]
        )
    csv_string = "\n".join(",".join(map(str, row)) for row in csv_data)
    return csv_string


# 3. 로컬에 CSV 저장 (테스트용, 삭제 용이하도록 별도 함수)
def save_csv_locally(csv_string):
    with open(LOCAL_FILE_PATH, "w", encoding="utf-8") as f:
        f.write(csv_string)


# 4. AWS S3 업로드
def upload_to_s3(**kwargs):
    ti = kwargs["ti"]
    csv_string = ti.xcom_pull(task_ids="convert_json_to_csv")
    # save_csv_locally(csv_string)  # 테스트용 로컬 저장
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
    catchup=False,
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
