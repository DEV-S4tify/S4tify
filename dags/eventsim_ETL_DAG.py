from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

S3_BUCKET = "s3://data-lake-csvs/eventsim_log/"

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id='spark_s3_to_snowflake_latest',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

start_task = DummyOperator(
    task_id="start",
    dag=dag
)

spark_job = SparkSubmitOperator(
    task_id='spark_process_s3_latest',
    application="/opt/airflow/dags/spark_jobs/s3_to_snowflake.py",
    conn_id='spark_default',
    application_args=[S3_BUCKET],
    executor_memory="4g",
    driver_memory="2g",
    dag=dag
)

end_task = DummyOperator(
    task_id="end",
    dag=dag
)

start_task >> spark_job >> end_task
