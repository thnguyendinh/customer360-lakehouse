#!/bin/bash
cd ~/customer360
set -a; source .env; set +a
while true; do
  echo "$(date): Running bronze_to_silver_incremental"
  docker run --rm \
    -v $(pwd)/src/spark_jobs:/app/spark_jobs \
    -v $(pwd)/src/config:/app/config \
    -w /app \
    -e HOME=/root \
    -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    -e AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
    -e S3_BUCKET=${S3_BUCKET} \
    customer360-spark \
    /opt/spark/bin/spark-submit --master local[2] /app/spark_jobs/bronze_to_silver_incremental.py
  echo "$(date): Running silver_to_gold_incremental"
  docker run --rm \
    -v $(pwd)/src/spark_jobs:/app/spark_jobs \
    -v $(pwd)/src/config:/app/config \
    -w /app \
    -e HOME=/root \
    -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    -e AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
    -e S3_BUCKET=${S3_BUCKET} \
    customer360-spark \
    /opt/spark/bin/spark-submit --master local[2] /app/spark_jobs/silver_to_gold_incremental.py
  sleep 300
done
