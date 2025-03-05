import csv
import json

from vibe import ChartData  # vibe.py 모듈 import

JSON_PATH = "vibe_chart.json"
CSV_PATH = "vibe_chart.csv"


# Vibe 차트 데이터를 가져와 JSON으로 저장하는 함수
def fetch_vibe_chart():
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
                "image": entry.image,
            }
            for entry in chart.entries
        ],
    }

    # JSON 파일 저장
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(chart_data, f, ensure_ascii=False, indent=4)

    print(f"✅ JSON 저장 완료: {JSON_PATH}")
    return JSON_PATH


# JSON → CSV 변환 함수 수정
def convert_json_to_csv():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    # ✅ "isNew" 필드를 추가해야 오류가 발생하지 않음
    fields = [
        "rank",
        "title",
        "artist",
        "peakPos",
        "lastPos",
        "isNew",
        "image"]

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for entry in data["entries"]:
            writer.writerow(entry)

    print(f"✅ CSV 변환 완료: {CSV_PATH}")


if __name__ == "__main__":
    fetch_vibe_chart()
    convert_json_to_csv()
