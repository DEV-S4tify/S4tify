import json
import csv
from genie import ChartData, GenieChartPeriod  # genie.py 모듈 import

# 파일 경로
JSON_PATH = "genie_chart.json"
CSV_PATH = "genie_chart.csv"

# Genie 차트 데이터 가져오기 및 JSON 저장
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
                "image": entry.image
            }
            for entry in chart.entries
        ]
    }

    # JSON 저장
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)

    print(f"✅ JSON 저장 완료: {JSON_PATH}")

# JSON → CSV 변환
def convert_json_to_csv():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    fields = ["rank", "title", "artist", "peakPos", "lastPos", "image"]

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for entry in data["entries"]:
            writer.writerow(entry)

    print(f"✅ CSV 변환 완료: {CSV_PATH}")

if __name__ == "__main__":
    fetch_genie_chart()
    convert_json_to_csv()