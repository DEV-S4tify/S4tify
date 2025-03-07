import ast
import time
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import requests
from airflow.models import Variable
from scripts.get_access_token import get_token

TODAY = datetime.now().strftime("%Y-%m-%d")
END_POINT = "https://api.spotify.com/v1"

SPOTIFY_ACCESS_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN")
LAST_FM_API_KEY = Variable.get("LAST_FM_API_KEY")


def transformation(**kwargs):

    task_instance = kwargs["ti"]

    # 중복데이터 제거
    artist_info_df = pd.DataFrame(
        task_instance.xcom_pull(
            task_ids="extract_artist_info",
            key="artist_info")).drop_duplicates(
        subset=["artist_id"])
    artist_top10_df = pd.DataFrame(
        task_instance.xcom_pull(
            task_ids="extract_artist_top10",
            key="artist_top10")).drop_duplicates(
        subset=["song_id"])

    global_top50_artist_df = pd.merge(
        artist_info_df, artist_top10_df, on="artist_id", how="outer"
    )

    column = ["artist_id", "artist", "genre", "album", "song_id", "title"]
    real_global_top50_artist_df = global_top50_artist_df[column]

    for index, row in real_global_top50_artist_df.iterrows():
        artist = row["artist"]
        track = row["title"]
        genre_list = []

        url = f"https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key={LAST_FM_API_KEY}&artist={artist}&track={track}&format=json"

        response = requests.get(url).json()

        try:
            for genre in response["track"]["toptags"]["tag"]:
                genre_list.append(genre["name"])
        except BaseException:
            pass

        real_global_top50_artist_df.at[index, "genre"] = genre_list

    real_global_top50_artist_df.to_csv(
        f"data/spotify_top50_artistData_{TODAY}.csv", encoding="utf-8-sig"
    )


# 아티스트 top 10 트랙 가져오기
def get_arti_top10(**kwargs):

    task_instance = kwargs["ti"]
    arti_top10_list = []

    # csv 파일 읽어오기
    song_info = read_crawling_csv()

    for _, row in song_info.iterrows():

        artist_id = ast.literal_eval(row["artist_id"])

        # 피처링 등의 이유로 아티스트가 2명 이상인 경우가 존재
        for i in range(len(artist_id)):
            id = artist_id[i]
            end_point = END_POINT + f"/artists/{id}/top-tracks/"

            top_10_info = extract(end_point)

            try:
                for track in top_10_info["tracks"]:
                    arti_top10_list.append(
                        {
                            "album": track["album"]["name"],
                            "artist_id": id,
                            "song_id": track["id"],
                            "title": track["name"],
                        }
                    )
            except BaseException:
                time.sleep(5)

                top_10_info = extract(end_point)

                for track in top_10_info["tracks"]:
                    arti_top10_list.append(
                        {
                            "album": track["album"]["name"],
                            "artist_id": id,
                            "song_id": track["id"],
                            "title": track["name"],
                        }
                    )

    task_instance.xcom_push(key="artist_top10", value=arti_top10_list)


# 아티스트 정보 가져오기
def get_artist_info(**kwargs):

    task_instance = kwargs["ti"]

    artist_info_list = []

    # csv 파일 읽어오기
    song_info = read_crawling_csv()

    for _, row in song_info.iterrows():
        artist_id = ast.literal_eval(row["artist_id"])

        for i in range(len(artist_id)):
            id = artist_id[i]

            end_point = END_POINT + f"/artists/{id}"
            artist_info = extract(end_point)
            artist_info_list.append(
                {
                    "artist": artist_info["name"],
                    "artist_id": id,
                    "genre": artist_info["genres"],
                }
            )

    task_instance.xcom_push(key="artist_info", value=artist_info_list)


# 크롤링 데이터 읽어오는 함수
def read_crawling_csv() -> pd.DataFrame:

    daily_chart_crawling = pd.read_csv(
        f"data/spotify_crawling_data_{TODAY}.csv")

    return daily_chart_crawling


# API 요청 함수
def extract(url: str) -> Optional[Dict[str, Any]]:

    access_token = SPOTIFY_ACCESS_TOKEN

    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        result = response.json()

    elif (
        response.status_code == 400 or response.status_code == 401
    ):  # 토큰 만료시 재요청
        time.sleep(3)
        get_token()  # Variable에 저장된 token 변경
        response = requests.get(url, headers=headers)
        result = response.json()

    else:
        print(response.text)

    return result
