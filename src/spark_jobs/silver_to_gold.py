import sys, os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, countDistinct, datediff, current_date, when, round as spark_round

spark = SparkSession.builder.appName("Silver2Gold") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

BUCKET = os.environ.get("S3_BUCKET", "customer360-lakehouse")
if not BUCKET:
    print("❌ S3_BUCKET environment variable not set!")
    sys.exit(1)

DAYS = 7
print(f"🟡 Silver → Gold (last {DAYS} days), bucket={BUCKET}")

silver_path = f"s3a://{BUCKET}/silver/kplus_history/"
df_silver = spark.read.parquet(silver_path)

# Lọc lấy 7 ngày gần nhất (dựa trên partition)
end_date = datetime.now().date()
start_date = end_date - timedelta(days=DAYS-1)
df_silver = df_silver.filter(col("date_partition") >= start_date)

print(f"Silver records (last {DAYS} days): {df_silver.count():,}")

df_rfm = df_silver.groupBy("contract", "region").agg(
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
print(f"✅ Gold updated for last {DAYS} days")
spark.stop()
