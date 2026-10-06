# Local PySpark environment: readable Spark UI at localhost:4040.
# Versions pinned to match requirements.txt (pyspark 3.5.3 + delta-spark 3.2.1).
FROM python:3.11-slim-bookworm

# Spark 3.5 runs on Java 17. default-jre-headless resolves to 17 on bookworm
# and gives an arch-independent JAVA_HOME (works on Apple Silicon and x86).
# procps provides `ps`, which Spark's launch scripts call. make runs the Makefile.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps make \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencies first so code changes do not invalidate this layer.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-fetch the Delta jars at build time. configure_spark_with_delta_pip pulls
# them from Maven on first SparkSession; baking them in makes runs offline-safe.
RUN python -c "from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip; \
configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()"

COPY . .

# Spark UI
EXPOSE 4040

CMD ["bash"]