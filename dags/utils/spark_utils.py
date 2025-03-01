from pyspark.sql import SparkSession

def spark_session_builder(app_name:str, configs:list[str] = ["spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0"]):
    return SparkSession.builder \
        .appName(app_name) \
        .config(configs[0], configs[1]) \
        .getOrCreate()