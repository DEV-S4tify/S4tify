#Dockerfile
FROM apache/airflow:2.9.1

ENV SPARK_JAR_DIR=/opt/spark/jars

USER root
RUN echo "deb http://deb.debian.org/debian bullseye main" >> /etc/apt/sources.list && \
    apt-get update && apt-get install -y \
    wget openjdk-11-jdk && \
    mkdir -p $SPARK_JAR_DIR && \
    wget -O $SPARK_JAR_DIR/snowflake-jdbc-3.9.2.jar https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.9.2/snowflake-jdbc-3.9.2.jar && \
    wget -O $SPARK_JAR_DIR/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -O $SPARK_JAR_DIR/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr


USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark