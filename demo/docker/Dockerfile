FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/demo

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /demo
COPY . /demo

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir dbt-postgres \
    && pip install --no-cache-dir -e /demo

ENTRYPOINT ["/bin/bash", "-c", "mkdir -p /demo/output/lineage && dbt deps && dbt seed --full-refresh && dbt run && python scripts/generate_lineage.py"]
