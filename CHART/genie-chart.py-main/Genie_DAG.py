import csv
import json
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from genie import ChartData, GenieChartPeriod  # genie.py 모듈 import

from airflow import DAG


# Genie 차트 데이터를 가져와 JSON으로 저장하는 함수
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
                "image": entry.image,
            }
            for entry in chart.entries
        ],
    }

    # JSON 파일 저장
    JSON_PATH = "/opt/airflow/dags/files/genie_chart.json"
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)

    print(f"✅ JSON 저장 완료: {JSON_PATH}")
    return JSON_PATH


# JSON을 CSV로 변환하는 함수
def convert_json_to_csv():
    JSON_PATH = "/opt/airflow/dags/files/genie_chart.json"
    CSV_PATH = "/opt/airflow/dags/files/genie_chart.csv"

    # JSON 파일 읽기
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        chart_data = json.load(f)

    # CSV 변환 및 저장
    fields = ["rank", "title", "artist", "peakPos", "lastPos", "image"]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for entry in chart_data["entries"]:
            writer.writerow(entry)

    print(f"✅ CSV 변환 완료: {CSV_PATH}")
    return CSV_PATH


# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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

    fetch_genie_chart_task >> convert_json_to_csv_task  # 순서 정의
