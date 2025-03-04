from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import csv
from flo import ChartData  # flo.py 모듈 import

# 파일 경로
JSON_PATH = "/opt/airflow/dags/files/flo_chart.json"
CSV_PATH = "/opt/airflow/dags/files/flo_chart.csv"

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
                "image": entry.image
            }
            for entry in chart.entries
        ]
    }

    # JSON 저장
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
    "retry_delay": timedelta(minutes=5),
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

    # 작업 순서
    fetch_flo_chart_task >> convert_json_to_csv_task
