-- Step 1: 데이터 정리 및 중복 제거
CREATE OR REPLACE TABLE s4tify.adhoc.music_chart_cleaned AS
SELECT DISTINCT
    RANK AS chart_rank,
    TITLE AS song_title,
    ARTIST AS artist_name,
    GENRE AS genre,
    LASTPOS AS last_position,
    IMAGE AS album_image,
    PEAKPOS AS peak_position,
    ISNEW AS is_new,
    SOURCE AS platform
FROM s4tify.raw_data.music_charts;

-- Step 2: 아티스트별 성과 비교
CREATE OR REPLACE TABLE s4tify.adhoc.artist_performance AS
SELECT
    artist_name,
    COUNT(DISTINCT song_title) AS total_songs,
    AVG(chart_rank) AS avg_rank,
    MIN(chart_rank) AS best_rank
FROM s4tify.adhoc.music_chart_cleaned
GROUP BY artist_name;

-- Step 3: 플랫폼별 인기곡 분석
CREATE OR REPLACE TABLE s4tify.adhoc.platform_popularity AS
SELECT
    platform,
    COUNT(DISTINCT song_title) AS total_songs,
    AVG(chart_rank) AS avg_rank
FROM s4tify.adhoc.music_chart_cleaned
GROUP BY platform;

-- Step 4: 장르별 트렌드 분석
CREATE OR REPLACE TABLE s4tify.adhoc.genre_trends AS
SELECT
    genre,
    COUNT(DISTINCT song_title) AS total_songs,
    AVG(chart_rank) AS avg_rank
FROM s4tify.adhoc.music_chart_cleaned
GROUP BY genre;

-- Step 5: 신곡과 기존 곡의 성과 비교
CREATE OR REPLACE TABLE s4tify.adhoc.new_vs_old_songs AS
SELECT
    CASE WHEN is_new = 'true' THEN 'New' ELSE 'Old' END AS song_type,
    COUNT(*) AS total_songs,
    AVG(chart_rank) AS avg_rank
FROM s4tify.adhoc.music_chart_cleaned
GROUP BY song_type;

-- Step 6: 최고 순위 기록 비교
CREATE OR REPLACE TABLE s4tify.adhoc.peak_rank_analysis AS
SELECT
    artist_name,
    song_title,
    MIN(chart_rank) AS best_rank,
    COUNT(*) AS weeks_on_chart
FROM s4tify.adhoc.music_chart_cleaned
GROUP BY artist_name, song_title;

-- Step 7: 플랫폼별 최고 순위 기록
CREATE OR REPLACE TABLE s4tify.adhoc.platform_peak_rank AS
SELECT
    platform,
    artist_name,
    song_title,
    MIN(chart_rank) AS best_rank
FROM s4tify.adhoc.music_chart_cleaned
GROUP BY platform, artist_name, song_title;

-- Step 8: 아티스트별 플랫폼 편중 분석
CREATE OR REPLACE TABLE s4tify.adhoc.artist_platform_focus AS
SELECT
    artist_name,
    platform,
    COUNT(DISTINCT song_title) AS total_songs
FROM s4tify.adhoc.music_chart_cleaned
GROUP BY artist_name, platform
ORDER BY artist_name, total_songs DESC;
