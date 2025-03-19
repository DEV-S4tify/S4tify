import os

import pandas as pd
import requests
from plugins.spark_snowflake_conn import *

LAST_FM_API_KEY = os.getenv("LAST_FM_API_KEY")


def add_song_genre(file_name, table_name):

    join_data = pd.read_csv(f"data/{file_name}")
    song_genres = []

    for _, row in join_data.iterrows():

        artist = row["artist"]
        track = row["title"]
        url = f"https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key={LAST_FM_API_KEY}&artist={artist}&track={track}&format=json"

        try:
            response = requests.get(url).json()
            # API 응답에서 장르 정보 추출
            genre = response.get("track", {}).get("toptags", {}).get("tag", [])

            if genre:
                genre_list = [g["name"] for g in genre]  # 장르 리스트로 변환
                song_genres.append(", ".join(genre_list))  # 문자열로 저장
            else:
                song_genres.append("Unknown")

        except Exception as e:
            print(f"Error fetching genre for {artist} - {track}: {e}")
            song_genres.append("Error")

    # 새로운 컬럼 추가
    join_data["song_genre"] = song_genres

    # string으로 변경 되었던 아티스트 장르 다시 array 변경
    join_data["artist_genre"] = join_data["artist_genre"].apply(
        lambda x: x.split(",") if isinstance(x, str) else []
    )

    join_data.columns = [col.upper() for col in join_data.columns]

    write_pandas_snowflake(join_data, table_name)


def main(logical_date):
    add_song_genre(
        f"join_artist_info_track10_{logical_date}.csv",
        "ARTIST_INFO_TOP10")
    add_song_genre(
        f"join_artsit_info_chart_{logical_date}.csv", "ARTIST_INFO_GLOBALTOP50"
    )
