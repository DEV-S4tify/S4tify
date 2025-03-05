import csv
import json

from melon import *

chart = ChartData(imageSize=500)
print(chart[0].json())
{"artist": "IVE (아이브)",
    "image": "https://cdnimg.melon.co.kr/cm2/album/images/112/11/297/11211297_20230327114349_500.jpg?7d9408105554f2f425c3d1d43ddd3d9f/melon/resize/500/optimize/90",
    "isNew": False,
    "lastPos": 1,
    "rank": 1,
    "title": "Kitsch",
 }
print(chart.name)
print(chart.date)
print(chart)


# JSON 문자열로 변환
json_data = chart.json()

# JSON 파일로 저장
with open("melon_chart.json", "w", encoding="utf-8") as f:
    f.write(json_data)

print("JSON 파일 생성 완료: melon_chart.json")


# JSON 파일 로드
with open("melon_chart.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# CSV 파일 저장 경로
csv_filename = "melon_chart.csv"

# CSV 필드명
fields = ["rank", "title", "artist", "lastPos", "isNew", "image"]

# CSV 파일 쓰기
with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields)

    # 헤더 작성
    writer.writeheader()

    # 데이터 작성
    for entry in data["entries"]:
        writer.writerow(entry)

print(f"CSV 파일 생성 완료: {csv_filename}")
