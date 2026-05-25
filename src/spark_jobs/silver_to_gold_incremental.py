import sys, os, boto3
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, countDistinct, datediff, current_date, when, round as spark_round
from botocore.exceptions import ClientError

spark = SparkSession.builder.appName("Silver2GoldIncremental") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

BUCKET = os.environ.get("S3_BUCKET", "customer360-lakehouse")
CHECKPOINT_KEY = "checkpoints/silver_to_gold/last_processed.txt"

def get_last_processed():
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=CHECKPOINT_KEY)
        return datetime.fromisoformat(obj['Body'].read().decode())
    except ClientError:
        return datetime.now() - timedelta(days=2)

def save_last_processed(dt):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=BUCKET, Key=CHECKPOINT_KEY, Body=dt.isoformat().encode())

last_processed = get_last_processed()
print(f"🟡 Silver → Gold (incremental) since {last_processed}")

# Đọc toàn bộ silver (vì gold cần tổng hợp lại, nhưng chỉ đọc data mới sẽ tinh hơn)
silver_path = f"s3a://{BUCKET}/silver/kplus_history/"
df_silver_full = spark.read.parquet(silver_path)

# Chỉ lấy phần mới hơn last_processed
df_silver = df_silver_full.filter(col("event_time") > last_processed)

if df_silver.count() == 0:
    print("No new silver data, skipping gold update")
    sys.exit(0)

# Tổng hợp lại toàn bộ gold (có thể ghi đè để đơn giản, nhưng dữ liệu không lớn)
df_rfm = df_silver_full.groupBy("contract", "region").agg(
    max("event_time").alias("last_seen"),
    min("event_time").alias("first_seen"),
    count("*").alias("frequency"),
    countDistinct("mac").alias("device_count"),
    sum("total_duration_sec").alias("total_duration_sec"),
    avg("total_duration_sec").alias("avg_duration_sec"),
    countDistinct("date_partition").alias("active_days")
).withColumn("recency_days", datediff(current_date(), col("last_seen").cast("date"))) \
 .withColumn("avg_duration_min", spark_round(col("avg_duration_sec") / 60, 1)) \
 .withColumn("rfm_segment",
    when((col("frequency") >= 20) & (col("recency_days") <= 3), "Champions")
    .when((col("frequency") >= 10) & (col("recency_days") <= 7), "Loyal")
    .when((col("recency_days") <= 3) & (col("frequency") < 10), "New User")
    .when((col("frequency") >= 5) & (col("recency_days") <= 14), "Potential")
    .when((col("recency_days").between(14, 30)), "At Risk")
    .when(col("recency_days") > 30, "Lost")
    .otherwise("Casual"))

gold_path = f"s3a://{BUCKET}/gold/customer_rfm/"
df_rfm.write.mode("overwrite").parquet(gold_path)

max_event_time = df_silver.select(max("event_time")).collect()[0][0]
save_last_processed(max_event_time)
print(f"✅ Gold updated, checkpoint {max_event_time}")
spark.stop()
