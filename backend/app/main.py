import io
import os
from datetime import datetime, timezone
from functools import lru_cache

import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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
    """Read recent available Parquet rows from MinIO."""
    limit = max(1, min(limit, 2000))
    try:
        objects = [
            obj for obj in list_objects("events/")
            if obj["Key"].endswith(".parquet")
        ]
        rows = []
        for obj in sorted(objects, key=lambda x: x["LastModified"], reverse=True):
            response = get_s3().get_object(Bucket=BUCKET, Key=obj["Key"])
            table = pq.read_table(io.BytesIO(response["Body"].read()))
            rows.extend(table.slice(0, limit - len(rows)).to_pylist())
            if len(rows) >= limit:
                break
        return {"count": len(rows), "events": rows}
    except Exception as exc:
        return {"count": 0, "events": [], "error": str(exc)[:300]}


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
