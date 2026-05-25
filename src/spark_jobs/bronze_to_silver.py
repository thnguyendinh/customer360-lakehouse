import sys, os, boto3
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp, length, trim

spark = SparkSession.builder.appName("Bronze2Silver") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

BUCKET = os.environ.get("S3_BUCKET", "customer360-lakehouse")
if not BUCKET:
    print("❌ S3_BUCKET environment variable not set!")
    sys.exit(1)

DAYS = 7
print(f"🔵 Bronze → Silver (last {DAYS} days), bucket={BUCKET}")

# Tạo danh sách path 7 ngày
paths = []
for i in range(DAYS):
    d = datetime.now().date() - timedelta(days=i)
    paths.append(f"s3a://{BUCKET}/bronze/{d.strftime('%Y%m%d')}/*.json")

df_raw = spark.read.option("mode", "PERMISSIVE").json(paths)
raw_count = df_raw.count()
print(f"Raw records: {raw_count:,}")

df_clean = df_raw.select(
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

# Ghi đè lên partition
output_path = f"s3a://{BUCKET}/silver/kplus_history/"
df_clean.write.mode("overwrite").partitionBy("date_partition").parquet(output_path)
print(f"✅ Silver updated for last {DAYS} days")
spark.stop()
