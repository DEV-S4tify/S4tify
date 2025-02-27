import json
import csv
from datetime import datetime
from bugs import ChartData, BugsChartType, BugsChartPeriod

# 파일 저장 경로
JSON_PATH = "bugs_chart.json"
CSV_PATH = "bugs_chart.csv"

# 1. Bugs 차트 데이터 가져오기
def fetch_bugs_chart():
    chart = ChartData(
        chartType=BugsChartType.All, 
        chartPeriod=BugsChartPeriod.Realtime, 
        fetch=True
    )

    chart_data = {
        "date": chart.date.strftime("%Y-%m-%d %H:%M:%S"),
        "entries": [
            {
                "rank": entry.rank,
                "title": entry.title,
                "artist": entry.artist,
                "lastPos": entry.lastPos,
                "peakPos": entry.peakPos,
                "image": entry.image
            }
            for entry in chart.entries
        ]
    }

    # JSON 파일 저장
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)

    print(f"✅ JSON 저장 완료: {JSON_PATH}")

# 2. JSON → CSV 변환
def convert_json_to_csv():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    fields = ["rank", "title", "artist", "lastPos", "peakPos", "image"]

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for entry in data["entries"]:
            writer.writerow(entry)

    print(f"✅ CSV 변환 완료: {CSV_PATH}")

if __name__ == "__main__":
    fetch_bugs_chart()
    convert_json_to_csv()
