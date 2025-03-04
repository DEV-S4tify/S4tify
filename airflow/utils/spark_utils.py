import os
from pyspark.sql import SparkSession
import snowflake.connector

# Spark JARs 설정
SPARK_HOME = os.environ.get("SPARK_HOME")
SPARK_JARS = ",".join([
    os.path.join(SPARK_HOME, "jars", "snowflake-jdbc-3.9.2.jar"),
    os.path.join(SPARK_HOME, "jars", "hadoop-aws-3.3.4.jar"),
    os.path.join(SPARK_HOME, "jars", "aws-java-sdk-bundle-1.12.262.jar")
])

# Spark Session builder
def spark_session_builder(app_name: str) -> SparkSession:
    """_summary_
        spark session builder for AWS S3 and Snowflake
    Args:
        app_name (str): spark session anme

    Returns:
        SparkSession
    """    
    return SparkSession.builder \
        .appName(f"{app_name}") \
        .config("spark.jars", SPARK_JARS)\
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.environ.get('AWS_REGION')}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def execute_snowflake_query(query, snowflake_options):
    '''
    Snowflake에서 SQL 쿼리를 실행하는 함수
    '''
    try:
        conn = snowflake.connector.connect(
            user=snowflake_options['user'],
            password=snowflake_options['password'],
            account=snowflake_options['account'],
            database=snowflake_options['db'],
            schema=snowflake_options['schema'],
            warehouse=snowflake_options['warehouse']
        )
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        print("Query executed successfully.")
    except Exception as e:
        print(f"Execute_snowflake_query Error: {e}")