FROM apache/spark:4.0.0

USER root

WORKDIR /opt/fluxcart

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Resolve Spark Kafka and S3 dependencies during image build.
RUN python3 -c 'from pyspark.sql import SparkSession; s = (SparkSession.builder.appName("FluxCartDependencySetup").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1").config("spark.jars.ivy", "/tmp/fluxcart-ivy").getOrCreate()); s.stop()' \
    && mkdir -p /opt/spark/jars \
    && find /tmp/fluxcart-ivy/jars -maxdepth 1 -type f -name '*.jar' \
    -exec cp -n {} /opt/spark/jars/ \; \
    && rm -rf /tmp/fluxcart-ivy

COPY app/ ./app/
COPY schemas/ ./schemas/
COPY jobs/ ./jobs/

RUN mkdir -p /opt/fluxcart/data /opt/fluxcart/checkpoints \
    && chown -R spark:spark /opt/fluxcart

USER spark

CMD ["python3", "/opt/fluxcart/app/main.py"]
