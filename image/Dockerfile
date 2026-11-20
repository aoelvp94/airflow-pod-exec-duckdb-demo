FROM python:3.12-slim

RUN pip install --no-cache-dir "dbt-core==1.12.7" "dbt-duckdb==1.11.1" \
 && useradd --uid 1000 --create-home dbt \
 && mkdir -p /data && chown 1000:1000 /data

WORKDIR /app
COPY --chown=1000:1000 dbt/ /app/dbt/
COPY --chown=1000:1000 image/bin/ /app/bin/

USER 1000
ENV DBT_PROFILES_DIR=/app/dbt \
    DBT_PROJECT_DIR=/app/dbt \
    DUCKDB_PATH=/data/warehouse.duckdb

# Pre-parse with the SAME profile the pod uses, so every exec starts from a warm manifest
# (a different DUCKDB_PATH here changes the profile hash and forces a full re-parse).
RUN cd /app/dbt && dbt parse && rm -f /data/warehouse.duckdb*
