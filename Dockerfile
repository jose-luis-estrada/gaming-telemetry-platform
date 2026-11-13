# Local PySpark runtime with Delta Lake. Spark UI on port 4040.
# Spark 3.5.3 + Delta 3.2.1, matching pyproject.toml.
FROM python:3.11-slim-bookworm

# Spark 3.5 runs on Java 17; default-jre-headless resolves to 17 on bookworm.
# procps provides `ps`, which Spark's launch scripts call.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps make \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TELEMETRY_DATA_ROOT=/app/data

RUN useradd --create-home --uid 1000 app
WORKDIR /app

# Dependencies first, so code changes do not invalidate this layer.
COPY pyproject.toml README.md ./
COPY src/__init__.py src/__init__.py
RUN pip install --no-cache-dir . && pip uninstall -y gaming-telemetry-platform

COPY --chown=app:app . .
USER app

# Fetch the Delta jars at build time, as the runtime user, so runs work offline.
RUN python -c "from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip; \
configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()"

EXPOSE 4040
CMD ["bash"]
