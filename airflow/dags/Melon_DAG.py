from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import csv
import requests

# 멜론 차트 API 정보
_APP_VERSION = "6.5.8.1"
_CP_ID = "AS40"
_USER_AGENT = f"{_CP_ID}; Android 13; {_APP_VERSION}; sdk_gphone64_arm64"
_CHART_API_URL = f"https://m2.melon.com/m6/chart/ent/songChartList.json?cpId={_CP_ID}&cpKey=14LNC3&appVer={_APP_VERSION}"

# 파일 저장 경로
JSON_PATH = "/opt/airflow/data/melon_chart.json"
CSV_PATH = "/opt/airflow/data/melon_chart.csv"

# 1. 멜론 차트 데이터 가져오기
def fetch_melon_chart():
    headers = {"User-Agent": _USER_AGENT}
    response = requests.get(_CHART_API_URL, headers=headers)

    if response.status_code != 200:
        raise Exception(f"멜론 API 호출 실패: {response.status_code}")

    data = response.json()
    
    # 필요한 데이터만 추출
    chart_data = {
        "date": f"{data['response']['RANKDAY']} {data['response']['RANKHOUR']}:00",
        "entries": [
            {
                "rank": int(song["CURRANK"]),
                "title": song["SONGNAME"],
                "artist": song["ARTISTLIST"][0]["ARTISTNAME"],
                "lastPos": int(song["PASTRANK"]),
                "peakPos": int(song.get("PEAKRANK", song["CURRANK"])),  # peakPos 없으면 현재 순위로 설정
                "isNew": song["RANKTYPE"] == "NEW",
                "image": song["ALBUMIMG"]
            }
            for song in data["response"]["SONGLIST"]
        ]
    }

    # JSON 파일로 저장
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)

    print(f"✅ JSON 저장 완료: {JSON_PATH}")

# 2. JSON → CSV 변환
def convert_json_to_csv():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    fields = ["rank", "title", "artist", "lastPos", "peakPos", "isNew", "image"]

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
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "melon_chart_pipeline",
    default_args=default_args,
    description="멜론 차트를 JSON으로 저장하고 CSV로 변환하는 DAG",
    schedule_interval="0 1 * * *",  # 매일 새벽 1시 실행
    catchup=False,
)

# 작업 정의
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

# 실행 순서 정의
fetch_task >> convert_task
