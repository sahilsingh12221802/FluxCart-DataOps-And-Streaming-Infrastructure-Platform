import logging
import os
import sys
import json
import threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat_ws, from_json, lit,
                                   to_timestamp, try_to_timestamp, when)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from datetime import datetime, timezone


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("fluxcart-spark")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-events")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/fluxcart/data/raw")
CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH", "/opt/fluxcart/checkpoints/raw"
)
QUARANTINE_OUTPUT_PATH = os.getenv(
    "QUARANTINE_OUTPUT_PATH", "/opt/fluxcart/data/quarantine"
)
QUARANTINE_CHECKPOINT_PATH = os.getenv(
    "QUARANTINE_CHECKPOINT_PATH",
    "/opt/fluxcart/checkpoints/quarantine",
)

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
    "org.apache.hadoop:hadoop-aws:3.4.1"
)

event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField(
            "user",
            StructType(
                [
                    StructField("user_id", StringType(), True),
                    StructField("session_id", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "product",
            StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("product_name", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("price", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "transaction",
            StructType(
                [
                    StructField("amount", DoubleType(), True),
                    StructField("currency", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "context",
            StructType(
                [
                    StructField("device", StringType(), True),
                    StructField("location", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("timestamp", StringType(), True),
        StructField(
            "search",
            StructType(
                [StructField("query", StringType(), True)]
            ),
            True,
        ),
        StructField("_corrupt_record", StringType(), True),
    ]
)

def read_kafka(spark, starting_offsets):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

def write_streaming_metrics(queries, stop_event):
    metrics_path = "/opt/fluxcart/data/streaming_metrics.json"
    temp_path = metrics_path + ".tmp"

    while not stop_event.is_set():
        try:
            query_metrics = []

            for query in queries:
                progress = query.lastProgress

                query_metrics.append({
                    "name": query.name,
                    "id": str(query.id),
                    "is_active": query.isActive,
                    "status": query.status,
                    "progress": progress,
                })

            metrics = {
                "application": "FluxCartKafkaStream",
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "queries": query_metrics,
            }

            os.makedirs(os.path.dirname(metrics_path), exist_ok=True)

            with open(temp_path, "w") as file:
                json.dump(metrics, file, indent=2, default=str)

            os.replace(temp_path, metrics_path)

        except Exception:
            logger.exception("Failed to write streaming metrics")

        stop_event.wait(5)

def main():
    ivy_dir = "/tmp/fluxcart-ivy"
    os.makedirs(ivy_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("FluxCartKafkaStream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

    logger.info(
        "Starting FluxCart: brokers=%s topic=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
    )

    # Preserve the original raw stream and its existing checkpoint.
    raw_kafka = read_kafka(spark, starting_offsets)

    parsed_stream = (
        raw_kafka
        .select(
            col("value").cast("string").alias("raw_payload"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
        .withColumn("event", from_json(col("raw_payload"), event_schema))
        .withColumn(
            "event_timestamp",
            try_to_timestamp(col("event.timestamp")),
        )
        .select(
            col("event.event_id").alias("event_id"),
            col("event.event_type").alias("event_type"),
            col("event.user.user_id").alias("user_id"),
            col("event.user.session_id").alias("session_id"),
            col("event.product.product_id").alias("product_id"),
            col("event.product.product_name").alias("product_name"),
            col("event.product.category").alias("category"),
            col("event.product.price").alias("product_price"),
            col("event.transaction.amount").alias("amount"),
            col("event.transaction.currency").alias("currency"),
            col("event.context.device").alias("device"),
            col("event.context.location").alias("location"),
            col("event.search.query").alias("search_query"),
            col("event_timestamp"),
            col("kafka_timestamp"),
            col("kafka_partition"),
            col("kafka_offset"),
        )
    )

    valid_events = parsed_stream.filter(
        col("event_id").isNotNull()
        & col("event_type").isNotNull()
        & col("event_timestamp").isNotNull()
    )

    raw_query = (
        valid_events.writeStream
        .queryName("fluxcart_raw_events")
        .trigger(processingTime="10 seconds")
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    logger.info(
        "Raw stream started: output=%s checkpoint=%s",
        OUTPUT_PATH,
        CHECKPOINT_PATH,
    )

    # Separate source and checkpoint: only newly arriving events are
    # considered by this new quarantine query on its first start.
    quarantine_kafka = read_kafka(spark, "latest")


    quarantine_base = (
        quarantine_kafka
        .select(
            col("value").cast("string").alias("raw_payload"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
        .withColumn(
            "event",
            from_json(
                col("raw_payload"),
                event_schema,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ),
        )
        .withColumn(
            "event_timestamp",
            try_to_timestamp(col("event.timestamp")),
        )
    )




    malformed = (
        col("event").isNull()
        | col("event._corrupt_record").isNotNull()
    )

    valid_json_event = (
        col("event").isNotNull()
        & col("event._corrupt_record").isNull()
    )

    rejection_reason = concat_ws(
        "; ",
        when(
            malformed,
            lit("malformed_json_or_null_event"),
        ),
        when(
            valid_json_event & col("event.event_id").isNull(),
            lit("missing_event_id"),
        ),
        when(
            valid_json_event & col("event.event_type").isNull(),
            lit("missing_event_type"),
        ),
        when(
            valid_json_event & col("event_timestamp").isNull(),
            lit("missing_or_invalid_timestamp"),
        ),
    )



    rejected_events = (
        quarantine_base
        .withColumn("rejection_reason", rejection_reason)
        .filter(col("rejection_reason") != "")
        .select(
            "raw_payload",
            "rejection_reason",
            col("event.event_id").alias("event_id"),
            col("event.event_type").alias("event_type"),
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
        )
    )

    quarantine_query = (
        rejected_events.writeStream
        .queryName("fluxcart_quarantined_events")
        .trigger(processingTime="10 seconds")
        .format("parquet")
        .outputMode("append")
        .option("path", QUARANTINE_OUTPUT_PATH)
        .option("checkpointLocation", QUARANTINE_CHECKPOINT_PATH)
        .start()
    )

    logger.info(
        "Quarantine stream started: output=%s checkpoint=%s",
        QUARANTINE_OUTPUT_PATH,
        QUARANTINE_CHECKPOINT_PATH,
    )

    monitor_stop_event = threading.Event()

    monitor_thread = threading.Thread(
        target=write_streaming_metrics,
        args=([raw_query, quarantine_query], monitor_stop_event),
        daemon=True,
        name="fluxcart-streaming-monitor",
    )

    monitor_thread.start()

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
    finally:
        monitor_stop_event.set()
        monitor_thread.join(timeout=6)

        for query in spark.streams.active:
            query.stop()

        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("FluxCart streaming job failed")
        sys.exit(1)
