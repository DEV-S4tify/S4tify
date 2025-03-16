import os

import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def load_s3_bucket(dir_name,file_name):

    s3_hook = S3Hook(aws_conn_id="AWS_S3")
    s3_bucket = "de5-s4tify"
    s3_key = f"raw_data/{dir_name}/{file_name}"

    local_file_path = f"data/{file_name}"

    try:
        s3_hook.load_file(
            filename=local_file_path,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True)

    except Exception as e:
        print(f"error:{e}")
