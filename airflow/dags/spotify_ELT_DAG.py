from datetime import datetime, timedelta

from scripts.crawling_spotify_data import *
from scripts.load_spotify_data import *
from scripts.request_spotify_api import *

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator


# Spakr JARs 설정
SPARK_HOME = os.environ.get("SPARK_JAR_DIR")
SPARK_JARS = ",".join(
    [
        os.path.join(SPARK_HOME, "snowflake-jdbc-3.13.33.jar"),
        os.path.join(SPARK_HOME, "spark-snowflake_2.12-2.12.0-spark_3.4.jar"),
        os.path.join(SPARK_HOME, "hadoop-aws-3.3.4.jar"),
        os.path.join(SPARK_HOME, "aws-java-sdk-bundle-1.12.262.jar"),
    ]
)



# DAG 기본 설정
default_args = {
    "owner": "yerin",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    dag_id="SpotifyDataProcessing",
    default_args=default_args,
    catchup=False,
    tags=["final_project"],
    schedule="0 12 * * *",
) as dag:

    artist_info_Top10_table = SparkSubmitOperator(
        task_id="artist_info_top10_table",
        application="dags/scripts/ELT_artist_info_top10.py",
        conn_id="spark_conn",
        jars=SPARK_JARS,
        dag=dag,
    )

    artist_info_globalTop50_table = SparkSubmitOperator(
        task_id="artist_info_globalTop50_table",
        application="dags/scripts/ELT_artist_info_globalTop50.py",
        conn_id="spark_conn",
        jars=SPARK_JARS,
        dag=dag,
    )

    [artist_info_Top10_table, artist_info_globalTop50_table]
