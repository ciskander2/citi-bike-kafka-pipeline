import os
from pathlib import Path
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=True)

S3_BUCKET = os.getenv("S3_BUCKET")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

raw_path = f"s3a://{S3_BUCKET}/raw/station_status/"
processed_path = f"s3a://{S3_BUCKET}/processed/station_status_parquet/"

spark = (
    SparkSession.builder
    .appName("CitiBikeStationStatusSparkTransform")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

df = spark.read.json(raw_path)

clean_df = (
    df.select(
        col("station_id").cast("string").alias("station_id"),
        col("num_bikes_available").cast("int").alias("num_bikes_available"),
        col("num_docks_available").cast("int").alias("num_docks_available"),
        col("is_installed").cast("boolean").alias("is_installed"),
        col("is_renting").cast("boolean").alias("is_renting"),
        col("is_returning").cast("boolean").alias("is_returning"),
        col("station_status").cast("string").alias("station_status"),
    )
    .withColumn("processed_at", current_timestamp())
)

clean_df.write.mode("append").parquet(processed_path)

print(f"Processed Parquet written to: {processed_path}")

spark.stop()