#!/bin/bash
cd ~/customer360
while true; do
  echo "$(date): Running bronze_to_silver"
  docker run --rm \
    -v $(pwd)/src/spark_jobs:/spark_jobs \
    -v $(pwd)/src/config:/config \
    -e HOME=/tmp \
    -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    -e AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
    -e S3_BUCKET=${S3_BUCKET} \
    apache/spark:3.5.1 \
    /opt/spark/bin/spark-submit --master local[2] /spark_jobs/bronze_to_silver.py
  echo "$(date): Running silver_to_gold"
  docker run --rm \
    -v $(pwd)/src/spark_jobs:/spark_jobs \
    -v $(pwd)/src/config:/config \
    -e HOME=/tmp \
    -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    -e AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
    -e S3_BUCKET=${S3_BUCKET} \
    apache/spark:3.5.1 \
    /opt/spark/bin/spark-submit --master local[2] /spark_jobs/silver_to_gold.py
  sleep 1800   # 30 phút
done
