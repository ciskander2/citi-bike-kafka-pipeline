import os
from pathlib import Path
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=True)

# AWS / S3 config
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Debug prints
print("S3_BUCKET:", S3_BUCKET)
print("AWS_REGION:", AWS_DEFAULT_REGION)
print("AWS KEY LOADED:", AWS_ACCESS_KEY_ID is not None)
print("AWS SECRET LOADED:", AWS_SECRET_ACCESS_KEY is not None)

# S3 paths
raw_path = f"s3a://{S3_BUCKET}/new_raw/station_status_batched_1/"
processed_path = f"s3a://{S3_BUCKET}/processed/station_status_parquet/"

# Create Spark session
spark = (
    SparkSession.builder
    .appName("CitiBikeStationStatusSparkTransform")

    # Hadoop AWS packages
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "software.amazon.awssdk:bundle:2.25.70,"
        "software.amazon.awssdk:url-connection-client:2.25.70"
    )

    # AWS credentials
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

    # S3 endpoint config
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    .config("spark.hadoop.fs.s3a.endpoint.region", AWS_DEFAULT_REGION)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")

    # S3A filesystem
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    # Connection settings
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    .config("spark.hadoop.fs.s3a.connection.maximum", "100")

    # Windows / Spark fixes
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .config("spark.hadoop.fs.s3a.committer.name", "directory")

    .config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    )

    .config(
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
        "2"
    )

    .getOrCreate()
)

try:
    print("READING FROM:", raw_path)
    print("WRITING TO:", processed_path)

    # Read JSONL microbatches
    df = spark.read.json(raw_path)

    # Select + clean columns
    clean_df = (
        df.select(
            col("station_id").cast("string").alias("station_id"),
            col("num_bikes_available").cast("int").alias("num_bikes_available"),
            col("num_docks_available").cast("int").alias("num_docks_available"),
            col("is_installed").cast("boolean").alias("is_installed"),
            col("is_renting").cast("boolean").alias("is_renting"),
            col("is_returning").cast("boolean").alias("is_returning")
        )
        .withColumn("processed_at", current_timestamp())
    )

    # Write Parquet to S3
    (
        clean_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(processed_path)
    )

    print(f"SUCCESS: Parquet written to {processed_path}")

finally:
    spark.stop()