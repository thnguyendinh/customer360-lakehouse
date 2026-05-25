FROM apache/spark:3.5.1

USER root

# Cài Python + pip
RUN apt-get update && apt-get install -y python3 python3-pip curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Cài Python packages
RUN pip3 install --no-cache-dir boto3==1.34.0 pyarrow==14.0.0 pandas==2.0.3

# Tải S3 jars vào đúng thư mục Spark
RUN curl -sL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws.jar && \
    curl -sL \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle.jar

# FIX: Tạo .ivy2 cho cả root lẫn user 1001 (spark chạy as 1001)
RUN mkdir -p /root/.ivy2/local /home/spark/.ivy2/local && \
    chmod -R 777 /root/.ivy2 && \
    chmod -R 777 /home/spark/.ivy2

# FIX: Set HOME cố định về /root để Ivy luôn tìm đúng path
ENV HOME=/root
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

WORKDIR /app

COPY src/spark_jobs/ /app/spark_jobs/
COPY src/config/     /app/config/

# Giữ USER root để HOME=/root có hiệu lực
USER root
