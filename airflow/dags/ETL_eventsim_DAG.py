from datetime import datetime, timedelta

from dags.plugins.variables import SPARK_JARS

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator

# S3 설정
S3_BUCKET = "s3a://de5-s4tify"

default_args = {
    "owner": "sanghyeok_boo",
    "start_date": datetime(2025, 3, 1),
    "end_date": datetime(2025, 3, 7),
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
    application="dags/scripts/ETL_eventsim_script.py",
    conn_id="spark_conn",
    application_args=[
        S3_BUCKET,
        "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
    ],
    executor_memory="2g",
    driver_memory="1g",
    jars=SPARK_JARS,
    dag=dag,
)

# Dummy 종료 태스크
end_task = DummyOperator(task_id="end", dag=dag)

# DAG 실행 순서 정의
start_task >> spark_job >> end_task
