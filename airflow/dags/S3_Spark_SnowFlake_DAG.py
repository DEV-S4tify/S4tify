import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator

# Airflow Variables에서 AWS 자격 증명 불러오기 (없으면 예외 발생 방지)
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY", default_var=None)
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY", default_var=None)

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise ValueError("AWS_ACCESS_KEY 또는 AWS_SECRET_KEY가 설정되지 않았습니다.")

# S3 및 Snowflake 설정
S3_BUCKET = "s3a://de5-s4tify"

# Spark JARs 설정
SPARK_HOME = os.environ.get("SPARK_JAR_DIR", "/opt/spark/jars")
SPARK_JARS = ",".join(
    [
        os.path.join(SPARK_HOME, "snowflake-jdbc-3.9.2.jar"),
        os.path.join(SPARK_HOME, "hadoop-aws-3.3.4.jar"),
        os.path.join(SPARK_HOME, "aws-java-sdk-bundle-1.12.262.jar"),
    ]
)

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "s3_to_snowflake_pipeline",
    default_args=default_args,
    description="Read from S3, process data with Spark, and store in Snowflake",
    schedule_interval="0 2 * * *",
    catchup=False,
)

# Spark 작업 스크립트 경로 설정
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
spark_script_path = os.path.abspath(
    os.path.join(AIRFLOW_HOME, "dags", "scripts", "S3_Spark_SnowFlake_ELT.py")
)

# SparkSubmitOperator 설정
spark_submit_task = SparkSubmitOperator(
    task_id="spark_submit_task",
    application=spark_script_path,
    conn_id="spark_conn",
    executor_memory="4g",
    executor_cores=4,
    driver_memory="4g",
    name="s3_to_snowflake_pipeline",
    execution_timeout=timedelta(minutes=45),
    verbose=True,
    jars=SPARK_JARS,
    conf={
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_KEY,
        "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    },
    dag=dag,
)

spark_submit_task
