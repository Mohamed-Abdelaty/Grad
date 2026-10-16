# ── Stage 1: builder ──────────────────────────────────────────────────────────
# Install Python dependencies into an isolated prefix so only the compiled
# wheels are copied to the final stage — not pip itself or build tools.
FROM bitnamilegacy/spark:3.5.0 AS spark-client

# The backend uses the same Spark and Java build as the worker cluster.
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies needed to compile native extensions (asyncpg, confluent-kafka, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker layer caching:
# this layer is only rebuilt when requirements.txt changes.
COPY requirements.txt .

RUN pip install --upgrade pip --no-cache-dir \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# Copy only the installed packages from the builder — no build tools, no pip.
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime system libraries required by asyncpg / psycopg / confluent-kafka at runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        librdkafka1 \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder stage
COPY --from=builder /install /usr/local

# Submit jobs with the Spark client that matches the running cluster.
COPY --from=spark-client /opt/bitnami/java /opt/bitnami/java
COPY --from=spark-client /opt/bitnami/spark /opt/bitnami/spark
ENV JAVA_HOME=/opt/bitnami/java
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1

# ── Non-root user ──────────────────────────────────────────────────────────────
# Running as root inside a container is a security anti-pattern.
RUN groupadd --gid 1001 appgroup \
 && useradd --uid 1001 --gid appgroup --create-home appuser \
 && mkdir -p /home/appuser/.ivy2/cache \
 && chown -R appuser:appgroup /home/appuser

# Copy application source
COPY --chown=appuser:appgroup . .

# Switch to non-root user before the entrypoint
USER appuser

EXPOSE 8000

# ── Startup script ─────────────────────────────────────────────────────────────
# entrypoint.sh (embedded via CMD) runs alembic migrations then starts uvicorn.
# We use a shell form so we can chain commands and handle signals properly.
CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1 --log-config /dev/null"]
