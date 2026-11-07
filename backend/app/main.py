import io
import os
import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
import sqlite3
import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError


load_dotenv(".env.minio")
load_dotenv(".env.spark.minio")

app = FastAPI(title="FluxCart API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = (
    os.getenv("MINIO_ROOT_USER")
    or os.getenv("MINIO_ACCESS_KEY")
    or os.getenv("AWS_ACCESS_KEY_ID")
    or "minioadmin"
)
SECRET_KEY = (
    os.getenv("MINIO_ROOT_PASSWORD")
    or os.getenv("MINIO_SECRET_KEY")
    or os.getenv("AWS_SECRET_ACCESS_KEY")
    or "minioadmin"
)
BUCKET = os.getenv("MINIO_BUCKET", "fluxcart-raw")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ecommerce-events",
)

def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_objects(prefix=""):
    client = get_s3()
    paginator = client.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


@lru_cache(maxsize=1)
def _unused_cache_marker():
    return None


@app.get("/")
def root():
    return {"service": "FluxCart API", "docs": "/docs"}


@app.get("/api/health")
def health():
    result = {
        "api": "healthy",
        "minio": "unknown",
        "bucket": BUCKET,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        get_s3().head_bucket(Bucket=BUCKET)
        result["minio"] = "healthy"
    except Exception as exc:
        result["minio"] = "unavailable"
        result["detail"] = str(exc)[:300]
    return result


@app.get("/api/overview")
def overview():
    try:
        objects = list_objects()
        event_objects = [
            obj for obj in objects
            if obj["Key"].startswith("events/")
            and obj["Key"].endswith(".parquet")
        ]
        quarantine_objects = [
            obj for obj in objects
            if obj["Key"].startswith("quarantine/")
            and obj["Key"].endswith(".parquet")
        ]
        checkpoints = [
            obj for obj in objects
            if obj["Key"].startswith("checkpoints/")
        ]
        return {
            "status": "connected",
            "bucket": BUCKET,
            "parquet_files": len(event_objects),
            "quarantine_files": len(quarantine_objects),
            "checkpoint_objects": len(checkpoints),
            "raw_bytes": sum(o["Size"] for o in event_objects),
            "quarantine_bytes": sum(o["Size"] for o in quarantine_objects),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        return {
            "status": "unavailable",
            "bucket": BUCKET,
            "error": str(exc)[:300],
        }



@app.get("/api/events")
def events(limit: int = 500):
    """Read recent Parquet events from MinIO and normalize API fields."""
    limit = max(1, min(limit, 2000))

    try:
        objects = [
            obj for obj in list_objects("events/")
            if obj["Key"].endswith(".parquet")
        ]

        # Read the newest available files first.
        objects.sort(
            key=lambda obj: obj["LastModified"],
            reverse=True,
        )

        rows = []
        client = get_s3()

        for obj in objects:
            response = client.get_object(
                Bucket=BUCKET,
                Key=obj["Key"],
            )
            table = pq.read_table(
                io.BytesIO(response["Body"].read())
            )

            remaining = limit - len(rows)
            if remaining <= 0:
                break

            rows.extend(
                table.slice(0, remaining).to_pylist()
            )

        # Normalize Spark's event_timestamp field for the frontend.
        for event in rows:
            if not event.get("timestamp"):
                event["timestamp"] = (
                    event.get("event_timestamp")
                    or event.get("kafka_timestamp")
                )

            # Keep the API's event timestamp consistently serializable.
            value = event.get("timestamp")
            if isinstance(value, datetime):
                event["timestamp"] = value.isoformat()

        # Sort the returned sample by timestamp, newest first.
        def timestamp_key(event):
            value = event.get("timestamp")

            if isinstance(value, datetime):
                parsed = value
            elif isinstance(value, str) and value.strip():
                try:
                    parsed = datetime.fromisoformat(
                        value.replace("Z", "+00:00")
                    )
                except ValueError:
                    return float("-inf")
            else:
                return float("-inf")

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            return parsed.timestamp()

        rows.sort(key=timestamp_key, reverse=True)

        return {
            "count": len(rows),
            "events": rows,
        }

    except Exception:
        import logging

        logging.exception("Failed to read events from MinIO")
        return {
            "count": 0,
            "events": [],
            "error": "Unable to retrieve events from storage.",
        }


@app.get("/api/quarantine")
def quarantine(limit: int = 20):
    """Return quarantine statistics and the most recent rejected records."""
    limit = max(1, min(limit, 100))

    try:
        objects = [
            obj for obj in list_objects("quarantine/")
            if obj["Key"].endswith(".parquet")
        ]

        client = get_s3()
        reason_counts = {}
        total_records = 0
        recent_records = []

        for obj in objects:
            response = client.get_object(
                Bucket=BUCKET,
                Key=obj["Key"],
            )

            table = pq.read_table(
                io.BytesIO(response["Body"].read()),
                columns=[
                    "rejection_reason",
                    "event_id",
                    "event_type",
                    "kafka_timestamp",
                    "kafka_partition",
                    "kafka_offset",
                ],
            )

            for row in table.to_pylist():
                total_records += 1

                # One rejected record may have multiple reasons.
                reasons = {
                    reason.strip()
                    for reason in str(
                        row.get("rejection_reason") or ""
                    ).split(";")
                    if reason.strip()
                }

                for reason in reasons:
                    reason_counts[reason] = (
                        reason_counts.get(reason, 0) + 1
                    )

                timestamp = row.get("kafka_timestamp")

                recent_records.append({
                    "event_id": row.get("event_id"),
                    "event_type": row.get("event_type"),
                    "timestamp": (
                        timestamp.isoformat()
                        if isinstance(timestamp, datetime)
                        else timestamp
                    ),
                    "rejection_reason": row.get("rejection_reason"),
                    "kafka_partition": row.get("kafka_partition"),
                    "kafka_offset": row.get("kafka_offset"),
                })

        recent_records.sort(
            key=lambda record: record.get("timestamp") or "",
            reverse=True,
        )

        return {
            "status": "connected",
            "bucket": BUCKET,
            "total_files": len(objects),
            "total_records": total_records,
            "reason_counts": [
                {"reason": reason, "count": count}
                for reason, count in sorted(
                    reason_counts.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            ],
            "recent_records": recent_records[:limit],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception:
        import logging

        logging.exception("Failed to read quarantine data from MinIO")

        return {
            "status": "unavailable",
            "total_files": 0,
            "total_records": 0,
            "reason_counts": [],
            "recent_records": [],
            "error": "Unable to retrieve quarantine analytics.",
        }


@app.get("/api/quality")
def quality():
    """Calculate data quality metrics for the sampled raw dataset."""
    try:
        result = events(limit=2000)

        if result.get("error"):
            raise RuntimeError("Unable to read raw event sample")

        records = result.get("events", [])
        total = len(records)

        checks = [
            {
                "name": "Event ID",
                "field": "event_id",
                "passed": sum(
                    bool(row.get("event_id")) for row in records
                ),
            },
            {
                "name": "Event type",
                "field": "event_type",
                "passed": sum(
                    bool(row.get("event_type")) for row in records
                ),
            },
            {
                "name": "Event timestamp",
                "field": "timestamp",
                "passed": sum(
                    bool(
                        row.get("timestamp")
                        or row.get("event_timestamp")
                    )
                    for row in records
                ),
            },
        ]

        for check in checks:
            check["missing"] = total - check["passed"]
            check["pass_rate"] = (
                round(check["passed"] / total * 100, 2)
                if total else 0
            )

        # Equal weighting across the three validation dimensions.
        score = (
            round(
                sum(check["pass_rate"] for check in checks)
                / len(checks),
                2,
            )
            if total else None
        )

        return {
            "status": "connected",
            "score": score,
            "score_label": "Raw dataset completeness",
            "sample_size": total,
            "valid_records": sum(
                all([
                    bool(row.get("event_id")),
                    bool(row.get("event_type")),
                    bool(
                        row.get("timestamp")
                        or row.get("event_timestamp")
                    ),
                ])
                for row in records
            ),
            "checks": checks,
            "scope": (
                "Sampled raw Parquet records only. "
                "Rejected records are tracked separately."
            ),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception:
        import logging

        logging.exception("Failed to calculate data quality metrics")

        return {
            "status": "unavailable",
            "score": None,
            "sample_size": 0,
            "valid_records": 0,
            "checks": [],
            "error": "Unable to calculate data quality metrics.",
        }


@app.get("/api/event-types")
def event_types():
    result = events(limit=2000)
    if result.get("error"):
        return {"event_types": [], "error": result["error"]}
    counts = {}
    for event in result["events"]:
        name = str(event.get("event_type") or "UNKNOWN")
        counts[name] = counts.get(name, 0) + 1
    return {
        "total_events_sampled": result["count"],
        "event_types": [
            {"name": name, "count": count}
            for name, count in sorted(
                counts.items(), key=lambda item: item[1], reverse=True
            )
        ],
    }


@app.get("/api/analytics")
def analytics():
    """Calculate consistent Overview metrics from one raw-event sample."""
    result = events(limit=2000)

    if result.get("error"):
        return {
            "status": "unavailable",
            "error": "Unable to calculate event analytics.",
        }

    records = result.get("events", [])
    event_counts = {}
    hourly_counts = {}
    purchase_count = 0
    revenue = 0.0
    hourly_revenue = {}

    for event in records:
        event_type = str(event.get("event_type") or "UNKNOWN")
        event_counts[event_type] = event_counts.get(event_type, 0) + 1

        if event_type == "PURCHASE":
            purchase_count += 1
            amount = float(event.get("amount") or 0)
            revenue += amount

        timestamp = event.get("timestamp") or event.get("event_timestamp")

        if timestamp:
            try:
                parsed = datetime.fromisoformat(
                    str(timestamp).replace("Z", "+00:00")
                )
                hour = parsed.replace(
                    minute=0, second=0, microsecond=0
                ).isoformat()

                hourly_counts[hour] = hourly_counts.get(hour, 0) + 1
                if event_type == "PURCHASE":
                    amount = float(event.get("amount") or 0)
                    hourly_revenue[hour] = (
                        hourly_revenue.get(hour, 0) + amount
                    )
            except ValueError:
                continue

    total = len(records)

    return {
        "status": "connected",
        "sample_size": total,
        "purchase_count": purchase_count,
        "purchase_share": round(
            purchase_count / total * 100, 2
        ) if total else 0,
        "revenue": round(revenue, 2),
        "average_purchase_value": round(
            revenue / purchase_count, 2
        ) if purchase_count else 0,

        "revenue_trend": [
            {
                "hour": hour,
                "revenue": round(amount, 2),
            }
            for hour, amount in sorted(hourly_revenue.items())
        ],
        "currency": "INR",
        "event_distribution": [
            {"name": name, "count": count}
            for name, count in sorted(
                event_counts.items(),
                key=lambda item: item[1],
                reverse=True,
            )
        ],
        "event_trend": [
            {"hour": hour, "events": count}
            for hour, count in sorted(hourly_counts.items())
        ],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/pipeline")
def pipeline():
    health_result = health()
    overview_result = overview()
    return {
        "api": health_result["api"],
        "minio": health_result["minio"],
        "bucket": BUCKET,
        "raw_files": overview_result.get("parquet_files", 0),
        "quarantine_files": overview_result.get("quarantine_files", 0),
        "status": overview_result.get("status", "unavailable"),
        "error": overview_result.get("error"),
    }


SPARK_METRICS_FILE = (
    Path(__file__).resolve().parents[2]
    / "spark"
    / "data"
    / "streaming_metrics.json"
)

STREAMING_DB = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "streaming_history.db"
)


def init_streaming_db():
    STREAMING_DB.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(STREAMING_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS streaming_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_name TEXT NOT NULL,
                run_id TEXT,
                batch_id INTEGER NOT NULL,
                recorded_at TEXT NOT NULL,
                input_rows INTEGER DEFAULT 0,
                input_rate REAL DEFAULT 0,
                processed_rate REAL DEFAULT 0,
                batch_duration_ms INTEGER DEFAULT 0,
                kafka_lag INTEGER,
                UNIQUE(query_name, run_id, batch_id)
            )
        """)


def save_streaming_history(queries):
    init_streaming_db()

    with sqlite3.connect(STREAMING_DB) as conn:
        for query in queries:
            if query.get("batch_id") is None:
                continue

            lag = query.get("offsets_behind_latest")

            conn.execute("""
                INSERT OR IGNORE INTO streaming_history (
                    query_name,
                    run_id,
                    batch_id,
                    recorded_at,
                    input_rows,
                    input_rate,
                    processed_rate,
                    batch_duration_ms,
                    kafka_lag
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                query.get("name"),
                query.get("run_id"),
                query["batch_id"],
                query.get("last_batch_timestamp") or datetime.now(
                    timezone.utc
                ).isoformat(),
                query.get("input_rows", 0),
                query.get("input_rows_per_second", 0),
                query.get("processed_rows_per_second", 0),
                query.get("batch_duration_ms", 0),
                int(lag) if lag is not None else None,
            ))


def get_streaming_history(limit=500):
    init_streaming_db()

    with sqlite3.connect(STREAMING_DB) as conn:
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT *
            FROM streaming_history
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()

    return [dict(row) for row in reversed(rows)]

@app.get("/api/streaming")
def streaming_metrics():
    try:
        if not SPARK_METRICS_FILE.exists():
            return {
                "status": "unavailable",
                "message": "Spark metrics file not found",
            }

        with SPARK_METRICS_FILE.open("r") as file:
            metrics = json.load(file)

        queries = []

        for query in metrics.get("queries", []):
            progress = query.get("progress") or {}
            sources = progress.get("sources") or {}
            source = sources[0] if sources else {}

            queries.append({
                "name": query.get("name"),
                "id": query.get("id"),
                "is_active": query.get("is_active", False),
                "status": query.get("status", {}),
                "batch_id": progress.get("batchId"),
                "batch_duration_ms": progress.get("batchDuration"),
                "input_rows": progress.get("numInputRows", 0),
                "run_id": progress.get("runId"),
                "input_rows_per_second": progress.get(
                    "inputRowsPerSecond", 0
                ),
                "processed_rows_per_second": progress.get(
                    "processedRowsPerSecond", 0
                ),
                "offsets_behind_latest": (
                    source.get("metrics", {}).get(
                        "maxOffsetsBehindLatest"
                    )
                ),
                "last_batch_timestamp": progress.get("timestamp"),
            })
            save_streaming_history(queries)

        return {
            "status": "connected",
            "application": metrics.get("application"),
            "updated_at": metrics.get("updated_at"),
            "queries": queries,
        }

    except Exception as exc:
        return {
            "status": "unavailable",
            "error": str(exc)[:300],
        }


@app.get("/api/streaming/history")
def streaming_history(limit: int = 500):
    try:
        limit = max(1, min(limit, 5000))
        records = get_streaming_history(limit)

        return {
            "status": "connected",
            "total_records": len(records),
            "records": records,
        }

    except Exception as exc:
        return {
            "status": "unavailable",
            "error": str(exc)[:300],
        }


@app.get("/api/kafka")
def kafka_metrics():
    admin = None

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id="fluxcart-monitor",
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )

        brokers = admin.describe_cluster()
        topic_metadata = admin.describe_topics([KAFKA_TOPIC])

        topic = next(
            (t for t in topic_metadata if t.get("topic") == KAFKA_TOPIC),
            None,
        )

        if not topic:
            return {
                "status": "unavailable",
                "broker_status": "connected",
                "topic": KAFKA_TOPIC,
                "error": "Topic not found",
            }

        partitions = []
        total_messages = 0

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            request_timeout_ms=10000,
        )

        try:
            for partition_info in topic["partitions"]:
                partition_id = partition_info["partition"]
                tp = TopicPartition(KAFKA_TOPIC, partition_id)

                consumer.assign([tp])

                beginning = consumer.beginning_offsets([tp])[tp]
                ending = consumer.end_offsets([tp])[tp]

                lag = max(0, ending - beginning)
                total_messages += lag

                partitions.append({
                    "partition": partition_id,
                    "leader": partition_info.get("leader"),
                    "replicas": partition_info.get("replicas", []),
                    "in_sync_replicas": partition_info.get(
                        "isr", []
                    ),
                    "earliest_offset": beginning,
                    "latest_offset": ending,
                    "retained_messages": lag,
                })
        finally:
            consumer.close()

        return {
            "status": "connected",
            "broker_status": "healthy",
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "broker_count": len(brokers.get("brokers", [])),
            "brokers": brokers.get("brokers", []),
            "topic": KAFKA_TOPIC,
            "partition_count": len(partitions),
            "total_retained_messages": total_messages,
            "partitions": partitions,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        return {
            "status": "unavailable",
            "broker_status": "unavailable",
            "topic": KAFKA_TOPIC,
            "error": str(exc)[:300],
        }

    finally:
        if admin:
            admin.close()
