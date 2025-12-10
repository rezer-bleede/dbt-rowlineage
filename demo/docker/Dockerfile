FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/demo

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /demo
COPY demo/. .
COPY pyproject.toml .
COPY MANIFEST.in .
COPY dbt_rowlineage ./dbt_rowlineage

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir dbt-postgres dbt-rowlineage \
    && pip install --no-cache-dir -e .

ENTRYPOINT ["/bin/bash", "-c", "mkdir -p /demo/output/lineage && dbt clean && dbt deps && dbt seed --full-refresh && dbt run && dbt-rowlineage && sleep infinity"]
