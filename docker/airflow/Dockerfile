FROM apache/airflow:2.8.0-python3.11

USER root

RUN apt-get update && apt-get install -y default-jdk curl && rm -rf /var/lib/apt/lists/* && \
    export JAVA_HOME=/usr/lib/jvm/default-java && \
    echo "JAVA_HOME=/usr/lib/jvm/default-java" >> /etc/environment

USER airflow

RUN pip install --no-cache-dir boto3==1.34.0 apache-airflow-providers-apache-spark==4.7.1

COPY src/dags/ /opt/airflow/dags/
