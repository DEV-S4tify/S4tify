from datetime import datetime

import boto3
import pandas as pd
from scripts.request_spotify_api import *

from airflow.models import Variable

TODAY = datetime.now().strftime("%Y-%m-%d")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

BUCKET_NAME = "de5-s4tify"
OBJECT_NAME = "raw_data"


def conn_to_s3():
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    return s3_client


def load_s3_bucket(file_name):

    s3_client = conn_to_s3()
    file_path = f"data/{file_name}"
    bucket_name = BUCKET_NAME
    object_name = f"{OBJECT_NAME}/{file_name}"

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)

    except Exception as e:
        print(f"error:{e}")
