# 🚲 Citi Bike Kafka Streaming Pipeline

![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Warehouse-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-Data%20Lake-569A31?style=for-the-badge&logo=amazons3&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

> A real-time data engineering pipeline that streams Citi Bike station availability events through Apache Kafka, lands raw microbatches in AWS S3, loads operational data into PostgreSQL/Supabase, transforms analytics models with SQL/dbt/PySpark, and visualizes bike availability trends in a Streamlit dashboard.

<p align="center">
  <img src="photos/citi_bike_kafka_architecture_diagram.png" alt="Real-Time Citi Bike Streaming Analytics Pipeline Architecture" width="100%">
</p>

---

## ✨ Project Highlights

- ⚡ **Engineered a real-time streaming pipeline** processing **700K+ Citi Bike station-status events** using Apache Kafka, Python producers/consumers, Docker, PostgreSQL, AWS S3, PySpark, dbt, SQL, and Streamlit.
- 🧱 **Built a data lake to warehouse architecture** by persisting raw events to **AWS S3** and loading query-ready datasets into **PostgreSQL/Supabase**.
- 📦 **Redesigned ingestion from per-event JSON writes to JSONL microbatches**, reducing S3 object count by **99%+** from roughly **143K individual objects** to compact **1K-5K event batches**.
- ✅ **Implemented fault-aware batched ingestion** with manual Kafka offset commits, database transactions, S3 persistence, and rollback behavior when a batch fails.
- 🔥 **Developed PySpark, dbt, and advanced SQL transformations** to convert raw streaming data into compressed Parquet datasets, reducing storage footprint by approximately **99%** compared with raw JSON-style storage.
- 📊 **Created a Streamlit analytics dashboard** for continuously ingested Kafka event data, surfacing operational trends from live Citi Bike station availability.

---

## 🖼️ Preview

### Warehouse / Supabase Snapshot

![Supabase screenshot](photos/supabase_screenshot.jpeg)

---

## 🧠 What This Project Does

Citi Bike publishes live station availability through the public GBFS feed. This project turns that live feed into a streaming analytics system:

1. 🚲 A Python producer polls the Citi Bike `station_status` GBFS endpoint.
2. 📨 Each station status record is published to an Apache Kafka topic.
3. 📥 A Python Kafka consumer reads events in batches.
4. 🪣 Raw event microbatches are written to AWS S3 as newline-delimited JSON.
5. 🐘 Event records are inserted into PostgreSQL/Supabase for warehouse-style querying.
6. ⚙️ PySpark can transform raw S3 data into optimized Parquet datasets.
7. 🧮 dbt SQL models create analytics-ready views/tables.
8. 📊 Streamlit displays live operational metrics and trend analysis.

---

## 🏗️ Architecture

```mermaid
flowchart LR
    A["Citi Bike GBFS API<br/>station_status.json"] --> B["Python Kafka Producer"]
    B --> C["Apache Kafka<br/>station_status topic"]
    C --> D["Python Batched Consumer"]
    D --> E["AWS S3 Data Lake<br/>JSONL microbatches"]
    D --> F["PostgreSQL / Supabase<br/>Streaming event warehouse"]
    E --> G["PySpark Transformations<br/>Clean Parquet outputs"]
    F --> H["dbt SQL Models<br/>Analytics layer"]
    H --> I["Streamlit Dashboard<br/>Operational insights"]
```

### Core Design Choices

| Layer | Technology | Purpose |
|---|---|---|
| Source | Citi Bike GBFS API | Live station availability data |
| Streaming | Apache Kafka | Durable event stream for station status messages |
| Producer | Python + `kafka-python` | Polls GBFS and publishes events to Kafka |
| Consumer | Python + `kafka-python` | Batches events, writes to S3, inserts into Postgres |
| Data Lake | AWS S3 | Stores raw JSONL event microbatches |
| Warehouse | PostgreSQL / Supabase | Stores queryable station event data |
| Batch Transform | PySpark | Converts raw JSON into cleaned Parquet datasets |
| Modeling | dbt + SQL | Builds analytics models for station operations |
| Dashboard | Streamlit + Pandas | Displays real-time metrics and trend tables |
| Local Infra | Docker Compose | Runs Kafka and PostgreSQL locally |

---

## 📂 Repository Structure

```text
.
|-- docker-compose.yml
|-- requirements.txt
|-- src/
|   |-- citibike_producer.py
|   |-- simple_consumer.py
|   `-- spark_transformation.py
|-- citibike_dbt/
|   |-- dbt_project.yml
|   `-- models/
|       `-- staging/
|           |-- top_20_stations.sql
|           |-- no_docking_but_availability.sql
|           |-- capacity_zero_availability.sql
|           |-- hourly_bike_status.sql
|           |-- utlilization_ratio.sql
|           `-- schema.yaml
|-- dashboard/
|   `-- app.py
|-- photos/
|   |-- citi_bike_kafka_architecture_diagram.png
|   `-- supabase_screenshot.jpeg
`-- README.md
```

---

## 🧰 Tech Stack

- 🐍 **Python** for streaming producers, consumers, S3 writes, Postgres inserts, and dashboard logic
- 📨 **Apache Kafka** for real-time event streaming
- 🐳 **Docker Compose** for local Kafka and PostgreSQL infrastructure
- 🐘 **PostgreSQL / Supabase** for warehousing streaming station events
- 🪣 **AWS S3** for raw cloud data lake storage
- 🔥 **PySpark** for scalable S3 transformations into Parquet
- 🧮 **dbt** for SQL-based analytics models
- 📊 **Streamlit** for the real-time analytics dashboard
- 📈 **Pandas / Plotly** for dashboard-ready analysis and visualization support

---

## ⚙️ Prerequisites

Before running the project, install:

- Python 3.10+
- Docker Desktop
- AWS credentials with permission to write to the target S3 bucket
- A PostgreSQL database or Supabase Postgres connection string
- Java, if running the PySpark transformation locally

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/ciskander2/citi-bike-kafka-pipeline.git
cd citi-bike-kafka-pipeline
```

### 2. Create a Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Start Kafka and PostgreSQL

```bash
docker compose up -d
```

This starts:

- Kafka on `localhost:9092`
- PostgreSQL on `localhost:5432`

### 5. Create the Kafka Topic

```bash
docker exec -it citibike-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic station_status \
  --partitions 1 \
  --replication-factor 1
```

---

## 🔐 Environment Variables

Create a `.env` file in the project root:

```env
KAFKA_TOPIC=station_status
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

S3_BUCKET=your-s3-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1

SUPABASE_DB_URL=postgresql://user:password@host:5432/database
SUPABASE_URL=postgresql://user:password@host:5432/database
```

### Notes

- `SUPABASE_DB_URL` is used by the Kafka consumer.
- `SUPABASE_URL` is used by the Streamlit dashboard.
- `S3_BUCKET` is used by both the consumer and the PySpark transformation.
- The local Docker Postgres defaults are defined in `docker-compose.yml`.

---

## 📨 Run the Streaming Pipeline

### Start the Producer

The producer polls Citi Bike's GBFS station status endpoint every 30 seconds and publishes each station record to Kafka.

```bash
python src/citibike_producer.py
```

Producer behavior:

- Fetches live station status data from Citi Bike
- Adds metadata such as `producer_event_time` and `source`
- Sends each station event to the `station_status` Kafka topic
- Flushes messages after every polling cycle

### Start the Consumer

In a second terminal, run:

```bash
python src/simple_consumer.py
```

Consumer behavior:

- Reads from Kafka with `enable_auto_commit=False`
- Buffers events into batches of `2000`
- Writes each batch to S3 as JSONL
- Inserts the batch into PostgreSQL/Supabase
- Commits the database transaction and Kafka offsets only after successful processing
- Rolls back the database transaction if processing fails

### S3 Raw Batch Layout

The consumer writes raw batches using this partitioned key pattern:

```text
new_raw/station_status_batched_1/
  year=YYYY/
  month=MM/
  day=DD/
  hour=HH/
  batch_<uuid>.jsonl
```

This microbatch strategy is the key optimization that reduced S3 object count by **99%+** compared with writing one object per event.

---

## 🐘 PostgreSQL / Supabase Storage

The consumer inserts streaming records into `station_status_batched`.

Expected inserted fields include:

| Column | Description |
|---|---|
| `station_id` | Citi Bike station identifier |
| `num_bikes_available` | Number of bikes available at the station |
| `num_docks_available` | Number of docks available |
| `is_installed` | Whether the station is installed |
| `is_renting` | Whether bikes can be rented |
| `is_returning` | Whether bikes can be returned |
| `station_status` | Station status string |
| `s3_key` | S3 object key for the raw JSONL batch |
| `raw_payload` | Original event payload as JSONB |
| `ingestion_time` | Insert timestamp |

Example table definition:

```sql
CREATE TABLE IF NOT EXISTS station_status_batched (
    id BIGSERIAL PRIMARY KEY,
    station_id TEXT,
    num_bikes_available INTEGER,
    num_docks_available INTEGER,
    is_installed BOOLEAN,
    is_renting BOOLEAN,
    is_returning BOOLEAN,
    station_status TEXT,
    s3_key TEXT,
    raw_payload JSONB,
    ingestion_time TIMESTAMPTZ DEFAULT NOW()
);
```

The dashboard and dbt models query analytics tables/views such as `station_status`, `station_information`, `top_20_stations`, `hourly_bike_status`, and related modeled outputs. In a production setup, `station_status_batched` can be promoted into a cleaned `station_status` table or view.

---

## 🔥 PySpark Transformation

The PySpark job reads raw station status JSON from S3, casts fields into analytics-friendly types, adds a processing timestamp, and writes cleaned Parquet output back to S3.

Run:

```bash
python src/spark_transformation.py
```

Input path:

```text
s3a://<S3_BUCKET>/raw/station_status/
```

Output path:

```text
s3a://<S3_BUCKET>/processed/station_status_parquet/
```

Selected output fields:

- `station_id`
- `num_bikes_available`
- `num_docks_available`
- `is_installed`
- `is_renting`
- `is_returning`
- `station_status`
- `processed_at`

---

## 🧮 dbt Analytics Models

The dbt project lives in `citibike_dbt/` and contains SQL models for station-level analytics.

Run from the dbt project directory:

```bash
cd citibike_dbt
dbt run
dbt test
```

### Models

| Model | What It Shows |
|---|---|
| `top_20_stations` | Top stations ranked by maximum e-bike availability |
| `no_docking_but_availability` | High-capacity stations where no docks are available |
| `capacity_zero_availability` | Large-capacity stations with zero bike availability |
| `hourly_bike_status` | Hourly empty-station and full-station rates |
| `utlilization_ratio` | Station-level bike utilization ratio |

### Example: Hourly Shortage Metrics

`hourly_bike_status` groups station events by hour and calculates:

- Total station events
- Empty station events
- Full station events
- Empty station rate
- Full station rate

These metrics help identify operational stress windows when stations are frequently empty or full.

---

## 📊 Streamlit Dashboard

The dashboard lives in `dashboard/app.py`.

Run:

```bash
streamlit run dashboard/app.py
```

Dashboard sections:

- 🚲 Total streaming events
- 🔋 Top 20 e-bike stations
- 🅿️ Stations with no docking availability
- ⚠️ Large-capacity stations with zero bikes available
- 📉 Hourly bike shortage trends
- 📍 Station utilization ratio

The dashboard caches query results for 30 seconds, making it useful for near-real-time monitoring while the Kafka pipeline is running.

---

## ✅ Reliability Features

This project includes several practical streaming reliability patterns:

- Manual Kafka offset management with `enable_auto_commit=False`
- Batch-level processing instead of event-by-event commits
- PostgreSQL transactions around each batch insert
- Consumer rollback on failed processing
- S3 raw batch persistence for replay and auditability
- JSONB payload storage for preserving original event details
- Partitioned S3 paths by year, month, day, and hour

---

## 📈 Analytics Use Cases

This pipeline can answer operational questions such as:

- Which Citi Bike stations have the highest e-bike availability?
- Which large stations frequently run out of bikes?
- Which stations have bikes available but no docks for returns?
- During which hours are stations most likely to be empty or full?
- Which stations have the highest utilization ratio?
- How does live station availability change throughout the day?

---

## 🧪 Testing and Validation

There is no dedicated Python test suite in this repository yet, but the pipeline can be validated with these checks:

```bash
# Confirm containers are running
docker compose ps

# Confirm Kafka topic exists
docker exec -it citibike-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

# Run dbt model tests
cd citibike_dbt
dbt test
```

Suggested future tests:

- Unit tests for producer event enrichment
- Unit tests for consumer batch formatting
- Integration test for Kafka to Postgres ingestion
- dbt schema tests for modeled analytics outputs
- Dashboard query smoke tests

---

## 🛣️ Future Improvements

- Add schema migrations for warehouse tables
- Add a station information ingestion job
- Add a Docker profile for producer, consumer, Spark, dbt, and Streamlit services
- Add Great Expectations or Soda checks for data quality
- Add orchestration with Airflow, Dagster, or Prefect
- Add CI checks for Python formatting, dbt compilation, and SQL linting
- Add Kafka Connect or Redpanda alternatives for easier local development
- Add dashboard charts with Plotly for richer interactive exploration

---

## 👤 Author

Built by [ciskander2](https://github.com/ciskander2) as an end-to-end data engineering project demonstrating real-time streaming, cloud data lake storage, warehouse modeling, and analytics dashboarding.

---

## 📌 Portfolio Summary

**Python, Apache Kafka, Docker, PostgreSQL, SQL, PySpark, dbt, AWS S3, ETL, Streamlit**

Engineered a real-time streaming pipeline processing **700K+ Citi Bike station-status events** using Apache Kafka, containerized Python producers/consumers, and fault-tolerant batched ingestion workflows with manual offset management. Built a cloud-based **data lake to warehouse architecture** by persisting raw streaming events to AWS S3 and loading transformed datasets into PostgreSQL/Supabase. Developed PySpark, dbt, and advanced SQL transformation workflows to convert raw streaming data into compressed Parquet datasets, reducing cloud storage footprint by approximately **99%**, and built a Streamlit dashboard visualizing bike availability, dock congestion, and mobility demand trends from continuously ingested Kafka event data.
