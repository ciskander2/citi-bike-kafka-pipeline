import os
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load .env from project root
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=True)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
S3_BUCKET = os.getenv("S3_BUCKET")

BATCH_SIZE = 2000

s3 = boto3.client("s3")

conn = psycopg2.connect(SUPABASE_DB_URL)
conn.autocommit = False


def write_batch_to_s3(events: list[dict]) -> str:
    now = datetime.now(timezone.utc)

    key = (
        f"new_raw/station_status_batched_1/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"batch_{uuid.uuid4()}.jsonl"
    )

    body = "\n".join(json.dumps(event) for event in events)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
    )

    return key


def write_batch_to_supabase(events: list[dict], s3_key: str) -> None:
    rows = [
        (
            event.get("station_id"),
            event.get("num_bikes_available"),
            event.get("num_docks_available"),
            bool(event.get("is_installed")),
            bool(event.get("is_renting")),
            bool(event.get("is_returning")),
            event.get("station_status"),
            s3_key,
            json.dumps(event),
        )
        for event in events
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO station_status_batched (
                station_id,
                num_bikes_available,
                num_docks_available,
                is_installed,
                is_renting,
                is_returning,
                station_status,
                s3_key,
                raw_payload,
                ingestion_time
            )
            VALUES %s
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())",
            page_size=1000,
        )


def process_batch(events: list[dict], consumer: KafkaConsumer) -> None:
    try:
        s3_key = write_batch_to_s3(events)
        write_batch_to_supabase(events, s3_key)

        conn.commit()
        consumer.commit()

        print(f"Processed batch of {len(events)} events → S3: {s3_key}")

    except Exception as e:
        conn.rollback()
        print(f"Failed to process batch: {e}")


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="station-status-batched-consumer-fast",
    )

    print("Fast batched consumer started. Waiting for Kafka messages...")

    buffer = []

    for message in consumer:
        buffer.append(message.value)

        if len(buffer) >= BATCH_SIZE:
            process_batch(buffer, consumer)
            buffer.clear()


if __name__ == "__main__":
    main()