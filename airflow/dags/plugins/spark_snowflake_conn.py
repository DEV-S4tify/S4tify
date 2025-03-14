from pyspark.sql import SparkSession
from airflow.models import Variable

import snowflake.connector
from datetime import datetime

SNOWFLAKE_USER =  Variable.get("SNOWFLAKE_USER")
SNOWFLKAE_USER_PWD =  Variable.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = Variable.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_URL =  Variable.get("SNOWFLAKE_URL")
SNOWFLAKE_DB = 'S4TIFY'
SNOWFLAKE_SCHEMA = 'RAW_DATA'

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


TODAY = datetime.now().strftime("%Y-%m-%d")

snowflake_options = {
    "sfURL": SNOWFLAKE_URL,
    "sfDatabase": SNOWFLAKE_DB,
    "sfSchema": SNOWFLAKE_SCHEMA,
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ANALYTICS_USERS",
    "sfUser": SNOWFLAKE_USER,
    "sfPassword": SNOWFLKAE_USER_PWD 
}

def create_spark_session(app_name: str):
    #만약 정의된 connection이 cluster라면 master를 spark master 주소로 변경
    spark = SparkSession.builder \
        .appName(f"{app_name}") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.jars", "/path/to/spark-snowflake_2.12-2.12.0-spark_3.4.jar,/path/to/snowflake-jdbc-3.13.33.jar") \
        .getOrCreate()
    
    return spark


def create_snowflake_conn():
    conn = snowflake.connector.connect(
        user = SNOWFLAKE_USER,
        password = SNOWFLKAE_USER_PWD,
        account = SNOWFLAKE_ACCOUNT,
        warehouse = "COMPUTE_WH",
        database = SNOWFLAKE_DB ,
        schema = SNOWFLAKE_SCHEMA
    )

    return conn


def create_snowflake_table(sql):
    
    conn = create_snowflake_conn()
    cur = conn.cursor()
    
    try:
        cur.execute("BEGIN");
        cur.execute(sql)
        cur.execute("COMMIT");
        conn.commit()
        
    except Exception as e:
        print(f"error:{e}")
        cur.execute("ROLLBACK");
        
        
def write_snowflake_spark_dataframe(table_name, df):
    
    
    df.show()
    
    df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", f"{table_name}") \
        .mode("append") \
        .save()