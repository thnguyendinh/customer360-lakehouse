from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import os

BUCKET=os.environ.get("S3_BUCKET","customer360-lakehouse")
AWS_KEY=os.environ.get("AWS_ACCESS_KEY_ID","")
AWS_SECRET=os.environ.get("AWS_SECRET_ACCESS_KEY","")
AWS_REGION=os.environ.get("AWS_DEFAULT_REGION","ap-southeast-1")

default_args={"owner":"data-team","retries":2,"retry_delay":timedelta(minutes=3)}
COMMON_ENV={"S3_BUCKET":BUCKET,"AWS_ACCESS_KEY_ID":AWS_KEY,"AWS_SECRET_ACCESS_KEY":AWS_SECRET,"AWS_DEFAULT_REGION":AWS_REGION}

with DAG(dag_id="customer360_pipeline",default_args=default_args,description="Customer 360 ETL",
         schedule_interval="*/5 * * * *",start_date=datetime(2024,1,1),catchup=False,max_active_runs=1,tags=["customer360"]) as dag:

    generate_data = DockerOperator(task_id="generate_fake_data",image="customer360-generator:latest",
        container_name="task_generator_{{ ts_nodash }}",command="python scripts/generate_data.py --interval 0",
        environment=COMMON_ENV,auto_remove=True,docker_url="unix://var/run/docker.sock",
        network_mode="customer360_c360_net")

    bronze_to_silver = DockerOperator(task_id="bronze_to_silver",image="customer360-spark:latest",
        container_name="task_spark_silver_{{ ts_nodash }}",
        command="spark-submit --master local[2] --driver-memory 2g /app/spark_jobs/bronze_to_silver.py",
        environment=COMMON_ENV,auto_remove=True,docker_url="unix://var/run/docker.sock",
        network_mode="customer360_c360_net",execution_timeout=timedelta(minutes=15))

    silver_to_gold = DockerOperator(task_id="silver_to_gold",image="customer360-spark:latest",
        container_name="task_spark_gold_{{ ts_nodash }}",
        command="spark-submit --master local[2] --driver-memory 2g /app/spark_jobs/silver_to_gold.py",
        environment=COMMON_ENV,auto_remove=True,docker_url="unix://var/run/docker.sock",
        network_mode="customer360_c360_net",execution_timeout=timedelta(minutes=10))

    generate_data >> bronze_to_silver >> silver_to_gold
