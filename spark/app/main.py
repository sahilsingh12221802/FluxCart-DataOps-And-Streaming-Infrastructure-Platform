import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

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
    ]
)


def main():
    ivy_dir = "/tmp/fluxcart-ivy"
    os.makedirs(ivy_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("FluxCartKafkaStream")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("S3A_ENDPOINT", "http://minio:9000"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("S3A_ACCESS_KEY", ""),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("S3A_SECRET_KEY", ""),
        )
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "Starting FluxCart stream: brokers=%s topic=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
    )

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option(
            "startingOffsets",
            os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
        )
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        kafka_stream
        .select(
            from_json(
                col("value").cast("string"),
                event_schema,
            ).alias("event"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
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
            to_timestamp(
                col("event.timestamp")
            ).alias("event_timestamp"),
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

    query = (
        valid_events.writeStream
        .queryName("fluxcart_raw_events")
        .trigger(processingTime="10 seconds")
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    logger.info("Streaming query started. Output: %s", OUTPUT_PATH)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
    finally:
        if query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("FluxCart streaming job failed")
        sys.exit(1)
