import sys, os, boto3
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp, length, trim, max as spark_max
from botocore.exceptions import ClientError

spark = SparkSession.builder.appName("Bronze2SilverIncremental") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

BUCKET = os.environ.get("S3_BUCKET", "customer360-lakehouse")
CHECKPOINT_KEY = "checkpoints/bronze_to_silver/last_processed.txt"

def get_last_processed():
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=CHECKPOINT_KEY)
        return datetime.fromisoformat(obj['Body'].read().decode())
    except ClientError:
        # Lần đầu: xử lý 2 ngày gần nhất
        return datetime.now() - timedelta(days=2)

def save_last_processed(dt):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=BUCKET, Key=CHECKPOINT_KEY, Body=dt.isoformat().encode())

last_processed = get_last_processed()
cutoff = last_processed - timedelta(minutes=10)  # overlap

print(f"🔵 Bronze → Silver (incremental) since {last_processed}")

# Xác định các ngày cần đọc (từ cutoff đến hiện tại)
start_date = cutoff.date()
end_date = datetime.now().date()
paths = []
current = start_date
while current <= end_date:
    paths.append(f"s3a://{BUCKET}/bronze/{current.strftime('%Y%m%d')}/*.json")
    current += timedelta(days=1)

df_raw = spark.read.option("mode", "PERMISSIVE").json(paths)
raw_count = df_raw.count()
print(f"Raw records: {raw_count:,}")

# Lọc theo event_time > cutoff
df_filtered = df_raw.filter(col("_source.EventTime").cast("timestamp") > cutoff)
filtered_count = df_filtered.count()
print(f"After time filter: {filtered_count:,}")

df_clean = df_filtered.select(
    col("_id").alias("event_id"),
    col("_source.Contract").alias("contract"),
    col("_source.Mac").alias("mac"),
    col("_source.TotalDuration").cast("integer").alias("total_duration_sec"),
    col("_source.AppName").alias("app_name"),
    col("_source.EventTime").alias("event_time_raw"),
    col("_source.Region").alias("region")
).filter(
    col("contract").isNotNull() & (length(trim(col("contract"))) > 0) & (col("total_duration_sec") > 0)
).withColumn("event_time", to_timestamp("event_time_raw")) \
 .withColumn("date_partition", to_date("event_time")) \
 .withColumn("ingested_at", current_timestamp()) \
 .drop("event_time_raw") \
 .dropDuplicates(["event_id"])

clean_count = df_clean.count()
print(f"Clean records: {clean_count:,}")

if clean_count > 0:
    output_path = f"s3a://{BUCKET}/silver/kplus_history/"
    df_clean.write.mode("append").partitionBy("date_partition").parquet(output_path)
    max_event_time = df_clean.select(spark_max("event_time")).collect()[0][0]
    save_last_processed(max_event_time)
    print(f"✅ Appended {clean_count} records to silver")
else:
    print("No new data")

spark.stop()
