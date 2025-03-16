import ast
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import requests
from scripts.get_access_token import get_token
from scripts.load_spotify_data import *

from airflow.models import Variable

TODAY = datetime.now().strftime("%Y-%m-%d")
END_POINT = "https://api.spotify.com/v1"

SPOTIFY_ACCESS_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN", default_var=None)
if not SPOTIFY_ACCESS_TOKEN:
    print("⚠️ Access Token 없음. 새로운 토큰을 가져옵니다.")
    get_token()
    SPOTIFY_ACCESS_TOKEN = Variable.get(
        "SPOTIFY_ACCESS_TOKEN", default_var=None)

LAST_FM_API_KEY = os.getenv("LAST_FM_API_KEY")


# 아티스트 top 10 트랙 가져오기
def get_arti_top10(logical_date, **kwargs):

    task_instance = kwargs["ti"]
    arti_top10_list = []
    dir_name = "artist_top10"
    object_name = f"spotify_artist_top10_{logical_date}.csv"

    # csv 파일 읽어오기
    song_info = read_crawling_csv(logical_date)

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
            except Exception as e:
                print(f"error:{top_10_info}")

    # task_instance.xcom_push(key='artist_top10', value=arti_top10_list)
    artist_top10_df = pd.DataFrame(arti_top10_list)
    artist_top10_df.to_csv(
        f"data/{object_name}",
        encoding="utf-8-sig",
        index=False)
    try:
        load_s3_bucket(dir_name, object_name)
        os.remove(f"data/{object_name}")
    except Exception as e:
        print(f"error: {e}")


# 아티스트 정보 가져오기
def get_artist_info(logical_date, **kwargs):

    task_instance = kwargs["ti"]
    artist_info_list = []
    dir_name = "artist_info"
    object_name = f"spotify_artist_info_{logical_date}.csv"

    # csv 파일 읽어오기
    song_info = read_crawling_csv(logical_date)

    for _, row in song_info.iterrows():
        artist_id = ast.literal_eval(row["artist_id"])

        for i in range(len(artist_id)):
            id = artist_id[i]

            try:
                end_point = END_POINT + f"/artists/{id}"
                artist_info = extract(end_point)
                # print(artist_info['name'])
                artist_info_list.append(
                    {
                        "artist": artist_info["name"],
                        "artist_id": id,
                        "artist_genre": artist_info["genres"],
                    }
                )
            except Exception as e:
                time.sleep(20)
                print(f"error:{artist_info}")

    # task_instance.xcom_push(key="artist_info", value=artist_info_list)
    artist_info_df = pd.DataFrame(artist_info_list)
    artist_info_df.to_csv(
        f"data/{object_name}",
        encoding="utf-8-sig",
        index=False)
    try:
        load_s3_bucket(dir_name, object_name)
        os.remove(f"data/{object_name}")
    except Exception as e:
        print(f"error: {e}")


# 크롤링 데이터 읽어오는 함수
def read_crawling_csv(execution_date) -> pd.DataFrame:

    daily_chart_crawling = pd.read_csv(
        f"data/spotify_crawling_data_{execution_date}.csv"
    )

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
