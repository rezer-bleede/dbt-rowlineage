FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY demo/ui/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml MANIFEST.in README.md ./
COPY dbt_rowlineage ./dbt_rowlineage
COPY demo ./demo
RUN pip install --no-cache-dir -e .

EXPOSE 8080
CMD ["uvicorn", "demo.ui.app:app", "--host", "0.0.0.0", "--port", "8080"]
