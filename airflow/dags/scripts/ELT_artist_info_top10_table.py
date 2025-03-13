from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split, regexp_replace
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, ArrayType
from airflow.models import Variable

import requests
import snowflake.connector
from datetime import datetime

SNOWFLAKE_USER =  Variable.get("SNOWFLAKE_USER")
SNOWFLKAE_USER_PWD =  Variable.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = Variable.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_URL =  Variable.get("SNOWFLAKE_URL")
SNOWFLAKE_DB = 'S4TIFY'
SNOWFLAKE_SCHEMA = 'RAW_DATA'

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
LAST_FM_API_KEY = Variable.get("LAST_FM_API_KEY")

BUCKET_NAME = 'de5-s4tify'
OBJECT_NAME = 'raw_data'

TODAY = datetime.now().strftime("%Y-%m-%d")

snowflake_options = {
    "sfURL": SNOWFLAKE_URL,
    "sfDatabase": SNOWFLAKE_DB,
    "sfSchema": SNOWFLAKE_SCHEMA,
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ANALYTICS_USERS",
    "sfUser": SNOWFLAKE_USER,
    "sfPassword": SNOWFLKAE_USER_PWD
}

def create_spark_session():
    spark = SparkSession.builder \
        .appName("s3 read test") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.jars", "/path/to/spark-snowflake_2.12-2.12.0-spark_3.4.jar,/path/to/snowflake-jdbc-3.13.33.jar") \
        .getOrCreate()
    
    return spark

def load(): 
    
    conn = snowflake.connector.connect(
        user = SNOWFLAKE_USER,
        password = SNOWFLKAE_USER_PWD,
        account = SNOWFLAKE_ACCOUNT,
        warehouse = "COMPUTE_WH",
        database = SNOWFLAKE_DB ,
        schema = SNOWFLAKE_SCHEMA
    )
    
    cur = conn.cursor()

    try:
        cur.execute("BEGIN");
        
        sql = """
        CREATE TABLE IF NOT EXISTS artist_info_top10(
            artist_id VARCHAR(100),
            artist VARCHAR(100),
            artist_genre ARRAY,
            album VARCHAR(100),
            song_id VARCHAR(100),
            title VARCHAR(100),
            song_genre ARRAY
        )
        """
        cur.execute(sql)
        cur.execute("COMMIT");
        conn.commit()
        
    except Exception as e:
        print(f"error:{e}")
        cur.execute("ROLLBACK");
    
    transform_df = transformation()
    transform_df.show()
    
    transform_df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "artist_info_top10") \
        .mode("append") \
        .save()

def transformation():
    
    #스키마 정의 
    artist_info_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_genre", StringType(), True)
    ])
    
    artist_top10_schema = StructType([
        StructField("album", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
    ])
    
    #데이터 읽어오고 중복 제거 
    artist_top10_df = extract("spotify_artist_top10", artist_top10_schema).dropDuplicates(['song_id'])
    artist_info_df = extract("spotify_artist_info", artist_info_schema).dropDuplicates(['artist_id'])
    
    artist_info_top10_df = artist_info_df.join(artist_top10_df, on='artist_id', how='outer')

    #노래 장르 데이터 추가 
    artist_info_top10_df = artist_info_top10_df.withColumn("song_genre", add_song_genre_udf(col("artist"), col("title")))
    
    return artist_info_top10_df


def add_song_genre(artist, track):
    
    url = f"https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key={LAST_FM_API_KEY}&artist={artist}&track={track}&format=json"
    
    try:
        response = requests.get(url).json()
        return [genre['name'] for genre in response.get('track', {}).get('toptags', {}).get('tag', [])]
    except requests.exceptions.RequestException as e:
        print(f"API 요청 오류: {e}")
        return ["API Error"]
    except KeyError:
        return ["Unknown"]
    
add_song_genre_udf = udf(add_song_genre, ArrayType(StringType()))

def extract(file_name, schema):
    
    spark = create_spark_session()
    df = spark.read.csv(f"s3a://{BUCKET_NAME}/{OBJECT_NAME}/{file_name}_{TODAY}.csv", header=True, schema=schema)
    
    if file_name == 'spotify_artist_info':
        df = df.withColumn("artist_genre", regexp_replace(df["artist_genre"], "[\\[\\]']", ""))  # 불필요한 문자 제거
        df = df.withColumn("artist_genre", split(df["artist_genre"], ", "))  # 쉼표 기준으로 배열 변환
    
    return df 


if __name__ == "__main__":
    load()

