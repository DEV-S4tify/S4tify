#Dockerfile
FROM apache/airflow:2.9.1

USER root

ENV SPARK_JAR_DIR=/opt/spark/jars

RUN echo "deb http://deb.debian.org/debian bullseye main" >> /etc/apt/sources.list && \
    apt-get update && apt-get install -y \
    wget openjdk-11-jdk && \
    mkdir -p $SPARK_JAR_DIR && \
    wget -O $SPARK_JAR_DIR/snowflake-jdbc-3.9.2.jar \
        https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.9.2/snowflake-jdbc-3.9.2.jar && \
    wget -O $SPARK_JAR_DIR/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -O $SPARK_JAR_DIR/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    # Snowflake JDBC Driver
    wget -O $SPARK_JAR_DIR/snowflake-jdbc-3.13.33.jar \
        https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar && \
    # Snowflake Spark Connector
    wget -O $SPARK_JAR_DIR/spark-snowflake_2.12-2.12.0-spark_3.4.jar \
        https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.4/spark-snowflake_2.12-2.12.0-spark_3.4.jar && \
    # Clean up to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    wget \
    libnss3 \
    libgconf-2-4 \
    libx11-xcb1 \
    libappindicator3-1 \
    fonts-liberation \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Google Chrome download
RUN apt-get update && apt-get install -y wget && \
    wget -q -O google-chrome.deb http://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_132.0.6834.159-1_amd64.deb \
    && apt install -y ./google-chrome.deb \
    && rm google-chrome.deb

# ChromeDriver download
RUN apt-get update && \
    apt-get install -y wget unzip && \
    wget -q -O chromedriver.zip https://storage.googleapis.com/chrome-for-testing-public/132.0.6834.159/linux64/chromedriver-linux64.zip && \
    unzip chromedriver.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm chromedriver.zip

USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark