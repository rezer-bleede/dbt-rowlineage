FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl libpq-dev gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir "sqlmesh[web,postgres]" psycopg2-binary

EXPOSE 8000

CMD ["sqlmesh", "ui", "--host", "0.0.0.0", "--port", "8000"]
