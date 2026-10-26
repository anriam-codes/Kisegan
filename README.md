![Profile Picture](assets/kiset.png)

A real-time weather pipeline that ingests weather observations, processes them with Apache Kafka and PySpark Structured Streaming, and stores cleaned records as partitioned Parquet files in MinIO.

## Overview

Kisegan follows weather events from API ingestion to object storage. The pipeline keeps the original payloads in Kafka, transforms nested JSON into a consistent record structure, applies basic data-quality checks, adds derived fields, and publishes processed events for downstream use. A separate Spark streaming job persists records from the cleaned topic to MinIO as Parquet.

## System Design

```text
        Weather API
             |
             v
    Python Kafka Producer
             |
             v
      Kafka: weather_raw
             |
             v
 PySpark Structured Streaming
   Parse -> Flatten -> Validate
       -> Derived Features
             |
             v
    Kafka: weather_cleaned
             |
             v
    Spark Storage Streaming Job
             |
             v
      MinIO Object Storage
       weather-data bucket
             |
             v
 Partitioned Parquet datasets
 year=YYYY/month=MM/day=DD
```

## Pipeline Stages

- **Ingestion:** A Python producer fetches weather observations and publishes the original JSON payloads to `weather_raw`.
- **Streaming ETL:** PySpark parses nested JSON, flattens fields, standardizes names, handles selected nulls, and normalizes timestamps.
- **Data quality:** Required fields and basic plausible-value ranges are checked in the transformation logic.
- **Feature engineering:** Adds derived fields such as `heat_index`, `avg_temp`, and an event identifier.
- **Storage:** A separate Spark streaming job reads `weather_cleaned` and writes Parquet objects to MinIO, partitioned by event date.

## Data Flow

### Raw events

The `weather_raw` topic preserves the nested API payload, including location and observation data such as temperature, humidity, wind, precipitation, and visibility.

### Cleaned events

The `weather_cleaned` topic contains flattened records with fields such as `city`, `latitude`, `temperature_c`, `humidity_pct`, `wind_speed_ms`, `event_time`, and `ingestion_time`. The transformation currently replaces selected missing precipitation values with `0.0`.

### Parquet storage

Stored objects follow a date-partitioned layout similar to:

```text
weather-data/
└── weather_events/
    └── year=YYYY/
        └── month=MM/
            └── day=DD/
                └── part-....snappy.parquet
```

Spark can create multiple Parquet files across streaming micro-batches. File count does not equal record count; the records written depend on Kafka offsets, checkpoint state, and events arriving while the storage job runs.

## Key Engineering Decisions

- **Kafka as an event buffer:** Decouples API ingestion from downstream processing.
- **Schema-driven parsing:** Converts nested JSON into a predictable structure before flattening it.
- **Separate raw and cleaned topics:** Preserves the source payload while making normalized events available downstream.
- **Parquet for historical storage:** Provides a columnar format suitable for analytical workloads.
- **MinIO for local object storage:** Uses an S3-compatible interface without requiring a cloud bucket.
- **Date-based partitioning:** Organizes data by year, month, and day for easier date-filtered reads.

## Tech Stack

- **Python** — API ingestion and pipeline logic.
- **Apache Kafka** — Buffers raw and cleaned weather events.
- **Apache Spark / PySpark Structured Streaming** — Parses, transforms, validates, enriches, and streams records.
- **Docker Compose** — Runs the local Kafka, ZooKeeper, and Spark services.
- **MinIO** — S3-compatible object storage for the local data lake.
- **Apache Parquet** — Columnar format for processed weather history.
- **Hadoop AWS / S3A** — Enables Spark to access MinIO through its S3-compatible API.

## Folder Structure

```text
KISEGAN/
├── docker/
│   └── spark/
│       └── Dockerfile
├── src/
│   ├── ingestion/
│   │   └── kafka_prod.py
│   └── spark_app/
│       ├── spark_session.py
│       ├── schemas.py
│       ├── transform.py
│       ├── stream_jobs.py
│       └── storage_jobs.py
├── config/
├── docker-compose.yaml
└── README.md
```

## Running Kisegan Locally

Run commands from the project root in Windows Command Prompt (`D:\1MyDev\KISEGAN`).

### 1. Start containers

```cmd
docker compose up -d
docker ps
```

### 2. Create Kafka topics

```cmd
docker exec -it kafka kafka-topics --create --topic weather_raw --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic weather_cleaned --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
```

If a topic already exists, leave it in place rather than deleting it just to repeat setup.

### 3. Restart Spark and run the producer

```cmd
docker restart spark
python src\ingestion\kafka_prod.py
```

### 4. Check topic offsets

```cmd
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic weather_raw
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic weather_cleaned
```

Optional: inspect messages in a console consumer and stop it with `Ctrl+C` when finished.

```cmd
docker exec -it kafka kafka-console-consumer --topic weather_raw --from-beginning --bootstrap-server localhost:9092
docker exec -it kafka kafka-console-consumer --topic weather_cleaned --from-beginning --bootstrap-server localhost:9092
```

### 5. Create the MinIO bucket

Open the MinIO Console at `http://localhost:9001`, sign in using your configured credentials, and create a bucket named `weather-data` if it does not already exist. Make sure the endpoint, credentials, and output path in `storage_jobs.py` match your local MinIO setup. Do not commit credentials to Git.

### 6. Run the storage job

Once `weather_cleaned` is receiving events, run:

```cmd
docker exec -it spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 src/spark_app/storage_jobs.py
```

Keep the storage process running while new cleaned events arrive. The Kafka package enables Kafka reads; Hadoop AWS and the AWS SDK enable S3A access to MinIO.

### 7. Verify Parquet output

In MinIO, open `weather-data/weather_events/year=YYYY/month=MM/day=DD/` and confirm that `.parquet` objects appear. `_spark_metadata` files are streaming sink metadata, not weather records.
