# Kisegan — Real-Time Weather Streaming Data Platform

A real-time weather data pipeline that ingests weather events from an API, processes them with Kafka and PySpark Structured Streaming, and persists processed records as partitioned Parquet files in MinIO for future analytical querying.

## Problem

Live API responses are nested, inconsistent raw events rather than datasets that can be queried directly. A useful data pipeline needs to ingest those events reliably, standardize their structure, apply basic data-quality checks, derive useful fields, and retain processed history beyond Kafka's streaming topics.

Kisegan explores this workflow locally using a containerized streaming stack and S3-compatible object storage.

## Solution

Kisegan moves weather observations through a small set of connected stages:

- **Ingestion** — A Python producer fetches weather observations and publishes JSON events to Kafka's `weather_raw` topic.
- **Streaming ETL** — PySpark Structured Streaming parses nested JSON, flattens fields, standardizes names and units, handles selected null values, and normalizes timestamps.
- **Data-quality checks** — Required fields and basic plausible-value ranges are checked in the transformation logic.
- **Feature engineering** — Adds derived fields such as `heat_index`, `avg_temp`, and an event identifier.
- **Streaming storage** — A separate Spark streaming job consumes `weather_cleaned` and writes Parquet data to a MinIO bucket, partitioned by event date.

The project currently demonstrates streaming ingestion, transformation, and object-storage persistence. A warehouse, dbt models, dashboards, and production-grade idempotency/monitoring are not claimed as completed components.

## System Architecture

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

## Key Engineering Decisions

- **Kafka as the event buffer** — Separates API ingestion from downstream processing and retains events in topics.
- **Schema-driven parsing** — Parses nested JSON into a known structure before flattening fields into a consistent record.
- **A dedicated cleaned topic** — Keeps normalized records separate from the original raw payloads.
- **Parquet for historical storage** — Uses a columnar format suitable for downstream analytical workloads.
- **MinIO instead of a cloud bucket** — Provides S3-compatible object storage locally, while Spark accesses it through the S3A connector.
- **Date-based partitioning** — Organizes stored records by year, month, and day to make future date-filtered reads more efficient.

## Data Flow

### Raw events

The `weather_raw` topic contains the original nested API payload. A representative record includes a `location` object and a `data` object with observation time, temperature, humidity, wind, precipitation, and visibility fields.

### Cleaned events

The `weather_cleaned` topic contains flattened, standardized records with fields such as `city`, `latitude`, `temperature_c`, `humidity_pct`, `wind_speed_ms`, `event_time`, and `ingestion_time`. Selected missing precipitation values are represented as `0.0` by the current transformation logic.

### Stored Parquet data

The storage job reads events from `weather_cleaned` and writes Parquet objects into the MinIO `weather-data` bucket under a path similar to:

```text
weather_events/
└── year=YYYY/
    └── month=MM/
        └── day=DD/
            └── part-....snappy.parquet
```

Spark may create multiple Parquet files as separate streaming micro-batches are committed. The number of files is not the same as the number of records. The exact records written depend on the storage query's Kafka starting offset, checkpoint state, and the events arriving while it runs.

## Tech Stack

- **Python** — API ingestion and pipeline logic.
- **Apache Kafka** — Buffers raw and cleaned weather events.
- **Apache Spark / PySpark Structured Streaming** — Parses, transforms, validates, enriches, and streams records.
- **Docker Compose** — Runs the local Kafka, ZooKeeper, and Spark services.
- **MinIO** — S3-compatible object storage for the local data lake.
- **Apache Parquet** — Columnar storage format for processed weather history.
- **S3A connector (Hadoop AWS)** — Allows Spark to write to MinIO using its S3-compatible API.

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

This is a representative structure based on the files discussed; adjust names if your repository differs.

## Running Kisegan Locally

Run the following commands from the project root in Windows Command Prompt. The order matters: start the services, create the topics, restart Spark so the streaming job starts with the topics available, and then publish events.

### 1. Start the containers

```cmd
docker compose up -d
docker ps
```

### 2. Create the Kafka topics

```cmd
docker exec -it kafka kafka-topics --create --topic weather_raw --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic weather_cleaned --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
```

If Kafka reports that a topic already exists, do not recreate or delete it just to repeat the setup.

### 3. Restart Spark and publish weather events

```cmd
docker restart spark
python src\ingestion\kafka_prod.py
```

### 4. Check Kafka topic offsets

```cmd
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic weather_raw
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic weather_cleaned
```

The offsets help confirm that events have reached the topics. They are not a direct count of unique weather observations if topics have been reused or offsets are non-zero.

Optional: inspect messages from the host-accessible Kafka listener inside the container:

```cmd
docker exec -it kafka kafka-console-consumer --topic weather_raw --from-beginning --bootstrap-server localhost:9092
docker exec -it kafka kafka-console-consumer --topic weather_cleaned --from-beginning --bootstrap-server localhost:9092
```

These consumers continue running until stopped with `Ctrl+C`.

### 5. Create the MinIO bucket

Start MinIO with the project's configured services, open the MinIO Console (in this setup, `http://localhost:9001`), sign in with the credentials configured for MinIO, and create a bucket named:

```text
weather-data
```

The storage job expects its MinIO endpoint, access key, secret key, and bucket/path to match the values configured in `storage_jobs.py` or the project's environment/configuration. Do not commit real credentials to Git.

### 6. Run the Parquet storage job

Run this from the project root after the raw-to-cleaned streaming flow is working. The package versions below match the Spark 3.4.1 / Hadoop 3.3.4 setup discussed in the project:

```cmd
docker exec -it spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 src/spark_app/storage_jobs.py
```

Keep the storage job running while new cleaned events are produced. The Kafka connector is required because the job reads from Kafka; the Hadoop AWS and AWS SDK packages enable S3A access to MinIO. If your `storage_jobs.py` configures these packages in another way, avoid loading conflicting versions.

### 7. Verify the stored files

In MinIO Console, open `weather-data` and navigate to `weather_events/year=YYYY/month=MM/day=DD`. Look for files ending in `.parquet`. Spark also writes `_spark_metadata` entries for streaming file-sink progress; those metadata files are expected and are not weather records.

## Current Status and Next Steps

| Component | Status |
|---|---|
| API ingestion to `weather_raw` | Implemented and manually verified |
| Kafka-to-Spark transformation | Implemented and manually verified |
| Basic data-quality checks | Implemented in transformation logic |
| Derived weather features | Implemented in transformation logic |
| Cleaned events in `weather_cleaned` | Implemented and manually verified |
| MinIO bucket and Parquet sink | Implemented and verified by visible Parquet objects |
| Idempotent/replay-safe storage guarantees | Not yet established |
| Automated orchestration and monitoring | Not yet implemented |
| SQL warehouse, dbt models, dashboards | Future extensions |

## Notes on Streaming Semantics

- A streaming query processes data in micro-batches; each batch may create one or more Parquet part files.
- `startingOffsets` determines where a new Kafka query begins when it has no existing checkpoint. A retained checkpoint generally controls progress on restarts, so changing `startingOffsets` alone does not reset a query that is resuming from its checkpoint.
- A storage job started with `startingOffsets=latest` normally consumes events arriving after it starts if no checkpoint already determines its position. It does not automatically backfill the latest 30 records already in the topic.
- Multiple Parquet files do not prove that every record in the cleaned topic was written. Verify the configured starting offset/checkpoint and compare record counts when completeness matters.

## License

Add a license if and when you choose one for the repository.