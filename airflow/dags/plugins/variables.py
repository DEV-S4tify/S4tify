import os

SNOWFLAKE_PROPERTIES = {
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "db": os.environ.get("SNOWFLAKE_DB", "S4TIFY"),
    "warehouse": os.environ.get("SNOWFLAKE_WH", "COMPUTE_WH"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "ANALYTICS_USERS"),
    "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
    "url": f'jdbc:snowflake://{os.environ.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com',
}
snowflake_options = {
    "sfURL": f"{os.getenv("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com",
    "sfDatabase": os.getenv("SNOWFLAKE_DB"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WH"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
}

snowflake_gold_options = {
    "sfURL": f"{os.getenv("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com",
    "sfDatabase": os.getenv("SNOWFLAKE_DB"),
    "sfSchema": "ANALYTICS",
    "sfWarehouse": os.getenv("SNOWFLAKE_WH"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
}
# Spark JARs 설정
SPARK_JAR_DIR = os.environ.get("SPARK_JAR_DIR")
SPARK_JARS = ",".join(
    [
        os.path.join(SPARK_JAR_DIR, "snowflake-jdbc-3.13.33.jar"),
        os.path.join(SPARK_JAR_DIR, "spark-snowflake_2.12-2.12.0-spark_3.4.jar"),
        os.path.join(SPARK_JAR_DIR, "hadoop-aws-3.3.4.jar"),
        os.path.join(SPARK_JAR_DIR, "aws-java-sdk-bundle-1.12.262.jar"),
    ]
)