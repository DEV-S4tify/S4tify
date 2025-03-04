from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

# S3 및 Snowflake 설정
S3_BUCKET = "s3://eventsim-log/eventsim_log/"

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id='spark_s3_to_snowflake_upsert',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Dummy 시작 태스크
start_task = DummyOperator(
    task_id="start",
    dag=dag
)

# SparkSubmitOperator: Spark에서 S3 데이터를 처리하고 Snowflake에 MERGE
spark_job = SparkSubmitOperator(
    task_id='spark_process_s3_upsert',
    application="/opt/airflow/dags/spark_jobs/eventsim_ETL.py",
    conn_id='spark_default',
    application_args=[S3_BUCKET],
    executor_memory="4g",
    driver_memory="2g",
    dag=dag
)

# Dummy 종료 태스크
end_task = DummyOperator(
    task_id="end",
    dag=dag
)

# DAG 실행 순서 정의
start_task >> spark_job >> end_task
