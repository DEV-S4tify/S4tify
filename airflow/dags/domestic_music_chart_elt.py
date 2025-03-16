import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago


def wait_one_minute():
    time.sleep(60)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "domestic_music_chart_dashboard_elt",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # 시작 Dummy 태스크
    start = DummyOperator(task_id="start")

    # Step 1: 최신 데이터만 추출하여 adhoc 스키마에 저장 (컬럼 "DATE" 사용)
    clean_music_chart = SnowflakeOperator(
        task_id="clean_music_chart",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE s4tify.adhoc.music_chart_cleaned AS
            SELECT
                rank, title, artist, genre, lastpos, image, peakpos, isnew, source, "DATE" AS time_date
            FROM s4tify.raw_data.music_charts
            WHERE "DATE" = (SELECT MAX("DATE") FROM s4tify.raw_data.music_charts);
        """,
    )

    # Step 2: 1분 대기 (데이터 저장 후 안정적 처리 위해)
    wait_task = PythonOperator(
        task_id="wait_one_minute",
        python_callable=wait_one_minute,
    )
    # Step 3: 대시보드용 ELT SQL 태스크들

    # 3-1. 장르별 인기곡 트렌드 분석 (수정됨)
    genre_trend_analysis = SnowflakeOperator(
        task_id="genre_trend_analysis",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE s4tify.analytics.genre_trend_analysis AS
            SELECT
                genre_flattened.value::string AS genre,
                COUNT(DISTINCT title) AS total_songs,
                AVG(rank) AS avg_rank
            FROM s4tify.adhoc.music_chart_cleaned,
            LATERAL FLATTEN(input => parse_json(REPLACE(genre, '''', '\"'))) AS genre_flattened
            GROUP BY genre_flattened.value
            ORDER BY avg_rank;
        """,
    )

    # 3-2. 아티스트별 최고 순위 및 평균 순위 분석
    artist_performance = SnowflakeOperator(
        task_id="artist_performance",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE s4tify.analytics.artist_performance AS
            SELECT
                artist,
                COUNT(DISTINCT title) AS total_songs,  -- 중복 제거
                MIN(rank) AS best_rank,
                AVG(rank) AS avg_rank
            FROM s4tify.adhoc.music_chart_cleaned
            WHERE time_date = (SELECT MAX(time_date) FROM s4tify.adhoc.music_chart_cleaned)  -- 최신 데이터만 사용
            GROUP BY artist
            ORDER BY best_rank;
        """,
    )

    # 3-3. 신곡(NEW) 현황 분석
    new_songs_analysis = SnowflakeOperator(
        task_id="new_songs_analysis",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE s4tify.analytics.new_songs_analysis AS
            SELECT
                COUNT(*) AS total_songs,
                SUM(CASE WHEN isnew = TRUE THEN 1 ELSE 0 END) AS new_songs,
                ROUND(100 * SUM(CASE WHEN isnew = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS new_song_percentage
            FROM s4tify.adhoc.music_chart_cleaned;
        """,
    )

    # 3-4. TOP 10 곡의 안정성 분석 (평균/최대 유지 기간)
    top10_stability = SnowflakeOperator(
        task_id="top10_stability",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE s4tify.analytics.top10_stability AS
            WITH top10 AS (
                SELECT title, COUNT(*) AS weeks_on_chart
                FROM s4tify.raw_data.music_charts
                WHERE rank <= 10
                GROUP BY title
            )
            SELECT
                AVG(weeks_on_chart) AS avg_weeks_top10,
                MAX(weeks_on_chart) AS max_weeks_top10
            FROM top10;
        """,
    )

    # 3-5. 차트 1위 곡의 주간 유지 기간 분석
    no1_song_duration = SnowflakeOperator(
        task_id="no1_song_duration",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE s4tify.analytics.no1_song_duration AS
            WITH no1_songs AS (
                SELECT
                    LOWER(title) AS title,
                    YEAR("DATE") AS year,
                    WEEKOFYEAR("DATE") AS week
                FROM (
                    SELECT DISTINCT LOWER(title) as title, "DATE"
                    FROM s4tify.raw_data.music_charts
                    WHERE rank = 1
                ) unique_no1
                GROUP BY LOWER(title), YEAR("DATE"), WEEKOFYEAR("DATE")
            )
            SELECT
                title,
                COUNT(*) AS total_weeks_at_no1
            FROM no1_songs
            GROUP BY title
            HAVING COUNT(*) >= 2
            ORDER BY total_weeks_at_no1 DESC;
        """,
    )

    # 3-6. 랭킹 상승/하락 곡 분석
    rank_change_analysis = SnowflakeOperator(
        task_id="rank_change_analysis",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE s4tify.analytics.rank_change_analysis AS
            WITH prev_chart AS (
                SELECT title, artist, rank
                FROM (
                    SELECT title, artist, rank, "DATE",
                        ROW_NUMBER() OVER (PARTITION BY title, artist ORDER BY "DATE" DESC) AS rn
                    FROM s4tify.raw_data.music_charts
                    WHERE "DATE" = (
                        SELECT MAX("DATE") FROM s4tify.raw_data.music_charts
                        WHERE "DATE" < (SELECT MAX("DATE") FROM s4tify.raw_data.music_charts)
                    )
                )
                WHERE rn = 1  -- 중복 제거: 가장 최근 데이터만 선택
            ),
            latest_chart AS (
                SELECT title, artist, rank
                FROM (
                    SELECT title, artist, rank, "DATE",
                        ROW_NUMBER() OVER (PARTITION BY title, artist ORDER BY "DATE" DESC) AS rn
                    FROM s4tify.raw_data.music_charts
                    WHERE "DATE" = (SELECT MAX("DATE") FROM s4tify.raw_data.music_charts)
                )
                WHERE rn = 1  -- 중복 제거: 가장 최근 데이터만 선택
            )
            SELECT
                l.title,
                l.artist,
                p.rank AS prev_rank,
                l.rank AS current_rank,
                (p.rank - l.rank) AS rank_change
            FROM latest_chart l
            LEFT JOIN prev_chart p ON l.title = p.title AND l.artist = p.artist
            WHERE p.rank IS NOT NULL AND l.rank IS NOT NULL  -- NULL 값 제거
            ORDER BY rank_change DESC;
        """,
    )

    # 종료 Dummy 태스크
    end = DummyOperator(task_id="end")

    # DAG 실행 순서
    start >> clean_music_chart >> wait_task
    wait_task >> [
            genre_trend_analysis,
            artist_performance,
            new_songs_analysis,
            top10_stability,
            no1_song_duration,
            rank_change_analysis,
    ] >> end
