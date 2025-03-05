from airflow.models import Variable
from scripts.get_access_token import get_token

import requests
import pandas as pd 
from datetime import datetime 
import json
import os
import ast

'''
1.발급 받은 액세스 토큰으로 top50 차트 요청 -> top 50차트는 매일 갱신 
+) 가끔 액세스 토큰이 만료 -> 다시 받아올 수 있게함 (env 파일 업뎃)

2. top 50 차트에 있는 아티스트 id 추출 + 아티스트 정보 가져오기

3. top 50에 있는 track_id로 track 세부 정보 추출

4. track_id를 기반으로 audio 세부 정보 가져오기 

'''

TODAY = datetime.now().strftime("%Y-%m-%d")
END_POINT = 'https://api.spotify.com/v1'
SPOTIFY_ACCESS_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN")


def get_arti_top_10():
    
    arti_top_10_list = []
    top_10_tracks_df = pd.DataFrame(columns=["album", "artist_id", "song_id", "title"])
    
    #csv 파일 읽어오기 
    song_info = pd.read_csv(f"data/spotify_crawling_data_{TODAY}.csv")

    for _, row in song_info.iterrows():
        
        artist_id = ast.literal_eval(row['artist_id'])
    
        #피처링 등의 이유로 아티스트가 2명 이상인 경우가 존재 
        for i in range(len(artist_id)):
            id = artist_id[i]
            end_point = END_POINT+f'/artists/{id}/top-tracks/'

            top_10_info = extract(end_point)
        
            for track in top_10_info['tracks']:
                arti_top_10_list.append({
                    "album": track['album']['name'],
                    "artist_id": id,
                    "song_id" : track['id'],
                    "title" : track['name']
                })
            
    print(arti_top_10_list)
    top_10_tracks_df = pd.DataFrame(arti_top_10_list)
    #top_10_tracks_df.to_csv(f"arti_top10_track_{TODAY}.csv", encoding="utf-8-sig")
        
            
    return top_10_tracks_df
        
    

def get_artist_info():
    
    artist_info_list = []
    
    #csv 파일 읽어오기 
    song_info = pd.read_csv(f"data/spotify_crawling_data_{TODAY}.csv")


    artist_info_df = pd.DataFrame(columns=["artist", "artist_id", "genre"])
    
    for _, row in song_info.iterrows():
        artist_id = ast.literal_eval(row['artist_id'])
        
        for i in range(len(artist_id)):
            id = artist_id[i]
            
            end_point = END_POINT+f'/artists/{id}'
            artist_info = extract(end_point)
            #print(artist_info['name'])
            artist_info_list.append({
                "artist" : artist_info['name'],
                "artist_id": id,
                "genre": artist_info['genres']
            })
        
    artist_info_df = pd.DataFrame(artist_info_list)
    #artist_info_df.to_csv(f"arti_info_{TODAY}.csv", encoding="utf-8-sig")
    
    return artist_info_df 


#노래 제목, 아티스트 id, 아티스트, 노래 id, 노래 인기도 담은 df 반환 
def get_global_top50():
    
    song_info_list = []
    song_info_df = pd.DataFrame(columns=["title", "artist", "artist_id", "song_id", "popularity"])
    end_point = END_POINT+'/playlists/4RunlK9lvAC8ZtRbbbPWzD'
    track_data = extract(end_point)
    
    
    # 각 트랙 정보 추가
    for track in track_data['tracks']['items']:
        for artist in track['track']['artists']:  # 여러 아티스트 처리
            song_info_list.append({
                "title": track['track']['name'],
                "artist": artist['name'],
                "artist_id": artist['id'],
                "song_id": track['track']['id'],
                "popularity": track['track']['popularity']
            })
        
        song_info_df = pd.DataFrame(song_info_list)
        song_info_df.to_csv(f"global_top50_track_{TODAY}.csv", encoding="utf-8-sig")
    
    return song_info_df


#정보 요청 
def extract(url):
    
    access_token = SPOTIFY_ACCESS_TOKEN

    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    
    payload = {"grant_type": "client_credentials"}
    
    response = requests.get(url, headers=headers)
    
    if  response.status_code == 200:
        result = response.json()
    
    elif response.status_code == 400 or response.status_code == 401 : #토큰 만료시 
        get_token() #.env 파일에 새로 업로드 ㄱ
        response = requests.get(url, headers=headers)
        
        result = response.json()
        
    else:
        print(response.text)

    return result

if __name__ == "__main__":
    get_arti_top_10()