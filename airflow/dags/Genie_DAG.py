import csv
import json
import requests
from datetime import datetime, timedelta

from plugins.genie import ChartData, GenieChartPeriod  # genie.py 모듈 import
from scripts.get_access_token import get_token
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

# 날짜 설정
TODAY = datetime.now().strftime("%Y%m%d")

# S3 설정
S3_BUCKET = "de5-s4tify"
S3_CSV_KEY = f"raw_data/genie_chart_{TODAY}.csv"
LOCAL_FILE_PATH = f"/opt/airflow/data/genie_chart_with_genre_{TODAY}.csv"

# Spotify API 설정
SPOTIFY_API_URL = "https://api.spotify.com/v1"
SPOTIFY_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN", default_var=None)

# Spotify API에서 아티스트 ID 검색
def search_artist_id(artist_name):
    SPOTIFY_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN", default_var=None)
    url = f"{SPOTIFY_API_URL}/search"
    headers = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}
    params = {"q": artist_name, "type": "artist", "limit": 1}
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        artists = response.json().get("artists", {}).get("items", [])
        artist_id = artists[0]["id"] if artists else None
        return artist_id
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
        return ", ".join(genres) if genres else "Unknown"
    return "Unknown"

# 1. Genie 차트 데이터 가져오기 및 JSON 변환
def fetch_genie_chart():
    chart = ChartData(chartPeriod=GenieChartPeriod.Realtime, fetch=True)
    chart_data = {
        "date": chart.date.strftime("%Y-%m-%d %H:%M:%S"),
        "entries": []
    }
    for entry in chart.entries:
        print(f"📊 차트 데이터 처리: {entry.rank}. {entry.title} - {entry.artist}")
        artist_id = search_artist_id(entry.artist)
        genre = get_artist_genre(artist_id)
        chart_data["entries"].append({
            "rank": entry.rank,
            "title": entry.title,
            "artist": entry.artist,
            "peakPos": entry.peakPos,
            "lastPos": entry.lastPos,
            "image": entry.image,
            "genre": genre
        })
    return chart_data

# 2. JSON → CSV 변환
def convert_json_to_csv(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="fetch_genie_chart")
    csv_data = [["rank", "title", "artist", "peakPos", "lastPos", "image", "genre"]]
    for entry in data["entries"]:
        csv_data.append([
            entry["rank"], entry["title"], entry["artist"], entry["peakPos"], entry["lastPos"], entry["image"], entry["genre"]
        ])
    csv_string = "\n".join(",".join(map(str, row)) for row in csv_data)
    return csv_string

# 3. 로컬에 CSV 저장 (테스트용)
def save_csv_locally(csv_string):
    with open(LOCAL_FILE_PATH, "w", encoding="utf-8") as f:
        f.write(csv_string)

# 4. AWS S3 업로드
def upload_to_s3(**kwargs):
    ti = kwargs["ti"]
    csv_string = ti.xcom_pull(task_ids="convert_json_to_csv")
    #save_csv_locally(csv_string)  # 테스트용 로컬 저장
    s3_hook = S3Hook(aws_conn_id="S4tify_S3")
    s3_hook.load_string(csv_string, key=S3_CSV_KEY, bucket_name=S3_BUCKET, replace=True)
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
    "genie_chart_dag",
    default_args=default_args,
    schedule_interval="30 0 * * *",  # 매일 00:30 실행
    catchup=False,
) as dag:
    
    get_spotify_token_task = PythonOperator(
        task_id="get_spotify_token",
        python_callable=get_token,  # ✅ 먼저 실행해서 Variable 갱신
        provide_context=True,
    )
    
    fetch_genie_chart_task = PythonOperator(
        task_id="fetch_genie_chart",
        python_callable=fetch_genie_chart,
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
    
    get_spotify_token_task >> fetch_genie_chart_task >> convert_json_to_csv_task >> upload_s3_task
