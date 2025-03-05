from scripts.request_spotify_api import *

from airflow.models import Variable

from datetime import datetime
import boto3
import pandas as pd 

TODAY = datetime.now().strftime("%Y-%m-%d")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

BUCKET_NAME = 'de5-s4tify'
OBJECT_NAME = 'raw_data'



def conn_to_s3():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
    )
    
    return s3_client
    
def load_s3_bucket():
    
    s3_client = conn_to_s3()
    
    file_path = f'data/spotify_join_data_{TODAY}.csv'
    bucket_name = BUCKET_NAME
    object_name = f"{OBJECT_NAME}/spotify_join_data_{TODAY}.csv"
    
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
    
    except Exception as e:
        print(f"error:{e}")
    

def processing_csv_file(df):
    if isinstance(df['artist'][0], str):
        df['artist'] = df['artist'].apply(eval)
        df['artist_id'] = df['artist_id'].apply(eval)
    
    # 리스트로 된 'artist' 컬럼을 펼치기
    exploded_df = df.explode(['artist', 'artist_id'])
    
    return exploded_df

def read_and_merge():
    df = pd.read_csv(f"data/spotify_crawling_data_{TODAY}.csv")
    global_top50_df = processing_csv_file(df)
    
    artist_info_df = get_artist_info()
    #중복데이터 제거 
    artist_info_df = artist_info_df.drop_duplicates(subset=['artist_id'])
    artist_top10_df = get_arti_top_10()
    
    #merged_df1 = pd.merge(global_top50_df, artist_info_df, on = 'artist_id', how='outer')
    global_top50_artist_df = pd.merge(artist_info_df, artist_top10_df, on='artist_id')
    
    column = ['artist_id', 'artist', 'genre', 'album', 'song_id', 'title']
    real_global_top50_artist_df = global_top50_artist_df[column]
    
    real_global_top50_artist_df.to_csv(f"rspotify_join_data_{TODAY}.csv",encoding="utf-8-sig")
    
    #버킷 업로드
    load_s3_bucket()
    
    
