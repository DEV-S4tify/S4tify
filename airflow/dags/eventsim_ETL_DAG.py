import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator

# S3 및 Snowflake 설정
S3_BUCKET = "s3a://de5-s4tify"
# Spark JARs 설정
SPARK_HOME = os.environ.get("SPARK_JAR_DIR")
SPARK_JARS = ",".join(
    [
        os.path.join(SPARK_HOME, "snowflake-jdbc-3.9.2.jar"),
        os.path.join(SPARK_HOME, "hadoop-aws-3.3.4.jar"),
        os.path.join(SPARK_HOME, "aws-java-sdk-bundle-1.12.262.jar"),
    ]
)

default_args = {
    "owner": "sanghyeok_boo",
    "start_date": datetime(2025, 3, 1),
    "end_date": datetime(2025, 3, 2),
    "template_searchpath": ["/opt/airflow/dags/spark_jobs/"],
}

dag = DAG(
    dag_id="eventsim_ETL",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["ETL", "Eventsim"],
)

# Dummy 시작 태스크
start_task = DummyOperator(task_id="start", dag=dag)

# SparkSubmitOperator: Spark에서 S3 데이터를 처리하고 Snowflake에 MERGE
spark_job = SparkSubmitOperator(
    task_id="spark_process_s3_upsert",
    application="/opt/airflow/dags/scripts/eventsim_ETL_script.py",
    conn_id="spark_conn",
    application_args=[
        S3_BUCKET,
        "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
    ],
    executor_memory="2g",
    driver_memory="1g",
    jars=SPARK_JARS,
    conf={
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": "{{ conn.aws_s3.login }}",
        "spark.hadoop.fs.s3a.secret.key": "{{ conn.aws_s3.password }}",
        "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    },
    dag=dag,
)

# Dummy 종료 태스크
end_task = DummyOperator(task_id="end", dag=dag)

# DAG 실행 순서 정의
start_task >> spark_job >> end_task
